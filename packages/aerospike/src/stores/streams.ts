import type { ConversationRecord } from '@flue/runtime/adapter';
import {
	ConversationStreamStoreError,
	clampLimit,
	formatOffset,
	parseOffset,
	DEFAULT_READ_LIMIT,
	MAX_READ_LIMIT,
} from '@flue/runtime/adapter';
import type {
	ConversationFoldCheckpoint,
	ConversationProducerClaim,
	ConversationStreamBatch,
	ConversationStreamIdentity,
	ConversationStreamMeta,
	ConversationStreamReadResult,
	ConversationStreamStore,
} from '@flue/runtime/adapter';
import type { AgentSubmission } from '@flue/runtime/adapter';
import { AerospikeRecords, compositeId, errorCode } from '../storage.ts';

type Meta = ConversationStreamMeta & { path: string };
type Batch = ConversationStreamBatch & {
	path: string;
	producerId: string;
	producerEpoch: number;
	producerSequence: number;
	submissionId?: string;
	attemptId?: string;
	recordCount: number;
	chunkCount: number;
	dataHash: string;
};
const MAX_APPEND_BYTES = 12 * 1024 * 1024;
// 256KiB UTF-8 chunks + header + metadata fit comfortably below the 4096
// record MRT ceiling even at Flue's 12MiB serialized append maximum.
const CHUNK_BYTES = 256 * 1024;
const listeners = new Map<string, Set<() => void>>();

export class AerospikeStreams implements ConversationStreamStore {
	constructor(private readonly db: AerospikeRecords) {}
	private fail(operation: string, path: string, reason: string): never {
		throw new ConversationStreamStoreError({ operation, path, reason });
	}
	async createStream(path: string, identity: ConversationStreamIdentity): Promise<void> {
		const current = await this.db.get<Meta>('streams', path);
		if (current) {
			if (
				current.value.identity.agentName !== identity.agentName ||
				current.value.identity.instanceId !== identity.instanceId
			)
				this.fail('create', path, 'Stream identity conflicts.');
			return;
		}
		const created = await this.db.create('streams', path, {
			path,
			identity,
			incarnation: crypto.randomUUID(),
			nextOffset: '-1',
			producerId: null,
			producerEpoch: 0,
			nextProducerSequence: 0,
		} satisfies Meta);
		if (!created) {
			const winner = await this.db.get<Meta>('streams', path);
			if (
				!winner ||
				winner.value.identity.agentName !== identity.agentName ||
				winner.value.identity.instanceId !== identity.instanceId
			)
				this.fail('create', path, 'Stream identity conflicts.');
		}
	}
	async acquireProducer(path: string, producerId: string): Promise<ConversationProducerClaim> {
		for (let i = 0; i < 64; i++) {
			const row = await this.db.get<Meta>('streams', path);
			if (!row) this.fail('acquire_producer', path, 'Stream does not exist.');
			const next = {
				...row.value,
				producerId,
				producerEpoch: row.value.producerEpoch + 1,
				nextProducerSequence: 0,
			};
			if (!Number.isSafeInteger(next.producerEpoch))
				this.fail('acquire_producer', path, 'Producer epoch exceeded the safe integer range.');
			if (await this.db.replace(row, next))
				return {
					producerId,
					producerEpoch: next.producerEpoch,
					incarnation: next.incarnation,
					nextProducerSequence: 0,
					offset: next.nextOffset,
				};
		}
		this.fail('acquire_producer', path, 'Producer claim did not converge.');
	}
	async append(input: {
		path: string;
		producerId: string;
		producerEpoch: number;
		incarnation: string;
		producerSequence: number;
		submission?: { submissionId: string; attemptId: string };
		records: readonly ConversationRecord[];
	}): Promise<{ offset: string }> {
		const data = JSON.stringify(input.records);
		const bytes = new TextEncoder().encode(data);
		if (data.length > MAX_APPEND_BYTES)
			this.fail('append', input.path, 'Serialized batch exceeds the 12MiB per-append limit.');
		if (input.records.length === 0)
			this.fail('append', input.path, 'A canonical batch cannot be empty.');
		const dataHash = await sha256(data);
		for (let retry = 0; retry < 32; retry++) {
			const current = await this.db.get<Meta>('streams', input.path);
			if (!current) this.fail('append', input.path, 'Stream does not exist.');
			const m = current.value;
			if (
				m.incarnation !== input.incarnation ||
				m.producerId !== input.producerId ||
				m.producerEpoch !== input.producerEpoch
			)
				this.fail('append', input.path, 'Producer claim is stale.');
			if (input.producerSequence < m.nextProducerSequence) {
				const existing = (
					await this.db.listByBin<Batch>('stream_batches', 'path', input.path)
				).find(
					(r) =>
						r.value.path === input.path &&
						r.value.producerId === input.producerId &&
						r.value.producerEpoch === input.producerEpoch &&
						r.value.producerSequence === input.producerSequence,
				);
				if (
					existing &&
					existing.value.dataHash === dataHash &&
					existing.value.submissionId === input.submission?.submissionId &&
					existing.value.attemptId === input.submission?.attemptId
				)
					return { offset: existing.value.offset };
				if (existing)
					this.fail('append', input.path, 'Producer sequence was retried with different records.');
				this.fail('append', input.path, 'Producer sequence has already been consumed.');
			}
			if (input.producerSequence !== m.nextProducerSequence)
				this.fail('append', input.path, 'Producer sequence is out of order.');
			const chunks = utf8Chunks(bytes);
			const nextOffset = parseOffset(m.nextOffset) + 1;
			if (!Number.isSafeInteger(nextOffset) || nextOffset < 0)
				this.fail('append', input.path, 'Stream offset exceeded the safe integer range.');
			if (!Number.isSafeInteger(m.nextProducerSequence + 1))
				this.fail('append', input.path, 'Producer sequence exceeded the safe integer range.');
			const offset = formatOffset(nextOffset);
			const batch: Batch = {
				path: input.path,
				offset,
				records: [],
				producerId: input.producerId,
				producerEpoch: input.producerEpoch,
				producerSequence: input.producerSequence,
				recordCount: input.records.length,
				chunkCount: chunks.length,
				dataHash,
				...(input.submission
					? { submissionId: input.submission.submissionId, attemptId: input.submission.attemptId }
					: {}),
			};
			const ownedIds = new Set(
				input.records
					.filter((record) => record.submissionId !== undefined)
					.map((record) => record.submissionId as string),
			);
			const txn = this.db.transaction(
				ownedIds.size + (input.submission ? 2 : 1),
				chunks.length + 2,
			);
			try {
				const txnPolicy = txn.policy;
				const txCurrent = await this.db.getInTransaction<Meta>('streams', input.path, txnPolicy);
				if (!txCurrent || txCurrent.generation !== current.generation) {
					await txn.abort();
					continue;
				}
				if (
					txCurrent.value.incarnation !== input.incarnation ||
					txCurrent.value.producerId !== input.producerId ||
					txCurrent.value.producerEpoch !== input.producerEpoch ||
					txCurrent.value.nextProducerSequence !== input.producerSequence
				) {
					await txn.abort();
					continue;
				}
				await this.authorize(
					input.path,
					input.submission,
					input.records,
					txnPolicy,
					txCurrent.value,
				);
				await this.db.putInTransaction(
					'stream_batches',
					compositeId(input.path, offset),
					batch,
					txnPolicy,
				);
				for (let i = 0; i < chunks.length; i++) {
					await this.db.putInTransaction(
						'stream_chunks',
						compositeId(input.path, offset, i),
						chunks[i],
						txnPolicy,
					);
				}
				const updated: Meta = {
					...txCurrent.value,
					nextOffset: offset,
					nextProducerSequence: txCurrent.value.nextProducerSequence + 1,
				};
				await this.db.putInTransaction('streams', input.path, updated, txnPolicy);
				await txn.commit();
				notify(input.path);
				return { offset };
			} catch (error) {
				if (txn.isInDoubt()) {
					try {
						await txn.commit();
						notify(input.path);
						return { offset };
					} catch {
						// The same transaction remains the authority; an exact
						// producer retry resolves any still-unknown outcome.
					}
				} else {
					try {
						await txn.abort();
					} catch {}
				}
				if (errorCode(error) === 3 || errorCode(error) === 120 || errorCode(error) === 121)
					continue;
				throw error;
			}
		}
		this.fail('append', input.path, 'Concurrent append retries exhausted.');
	}
	private async authorize(
		path: string,
		submission: { submissionId: string; attemptId: string } | undefined,
		records: readonly ConversationRecord[],
		txnPolicy: object,
		stream: Meta,
	) {
		const owned = records.filter(
			(record) => record.submissionId !== undefined || record.attemptId !== undefined,
		);
		if (!submission) {
			if (owned.length)
				this.fail('append', path, 'Submission-owned records require attempt authorization.');
			return;
		}
		const hostRow = await this.db.getInTransaction<
			AgentSubmission & {
				settlementRecordId?: string;
				settlementRecord?: unknown;
			}
		>('submissions', submission.submissionId, txnPolicy);
		const host = hostRow?.value;
		if (
			!host ||
			host.attemptId !== submission.attemptId ||
			host.input.agent !== stream.identity.agentName ||
			host.input.id !== stream.identity.instanceId
		)
			this.fail('append', path, 'Submission attempt no longer owns work for this agent instance.');
		const terminalizingSettlement =
			host.status === 'terminalizing' &&
			records.length === 1 &&
			owned.length === 1 &&
			owned[0]?.type === 'submission_settled' &&
			host.submissionId === owned[0]?.submissionId &&
			host.settlementRecordId === owned[0]?.id &&
			JSON.stringify(host.settlementRecord) === JSON.stringify(owned[0]);
		if (host.status !== 'running' && !terminalizingSettlement)
			this.fail('append', path, 'Submission attempt no longer owns work for this agent instance.');
		for (const record of owned) {
			if (
				record.submissionId === submission.submissionId &&
				record.attemptId === submission.attemptId
			)
				continue;
			const joinedRow = record.submissionId
				? await this.db.getInTransaction<AgentSubmission>(
						'submissions',
						record.submissionId,
						txnPolicy,
					)
				: null;
			const joined = joinedRow?.value;
			if (
				!joined ||
				joined.joinedInto !== submission.submissionId ||
				(joined.status !== 'joining' && joined.status !== 'joined') ||
				record.attemptId !== submission.attemptId
			)
				this.fail(
					'append',
					path,
					'Record ownership does not match the authorized submission attempt.',
				);
		}
	}
	async read(
		path: string,
		options?: { offset?: string; limit?: number },
	): Promise<ConversationStreamReadResult> {
		const meta = await this.db.get<Meta>('streams', path);
		if (!meta) return { batches: [], nextOffset: '-1', upToDate: true };
		const start = options?.offset ?? '-1';
		if (start === 'now') return { batches: [], nextOffset: meta.value.nextOffset, upToDate: true };
		const after = parseOffset(start);
		if (!Number.isSafeInteger(after) || after > parseOffset(meta.value.nextOffset))
			this.fail('read', path, 'Read offset is beyond the canonical stream head.');
		const limit = clampLimit(options?.limit, DEFAULT_READ_LIMIT, MAX_READ_LIMIT);
		const head = parseOffset(meta.value.nextOffset);
		const first = Math.max(0, after + 1);
		const count = Math.min(limit, Math.max(0, head - first + 1));
		const offsets = Array.from({ length: count }, (_, i) => formatOffset(first + i));
		const headerRows = await this.db.getMany<Batch>(
			'stream_batches',
			offsets.map((offset) => compositeId(path, offset)),
		);
		const headers = headerRows.map((row, i) => {
			if (!row) this.fail('read', path, `Batch ${offsets[i]} is incomplete.`);
			return row.value;
		});
		const chunkKeys = headers.flatMap((header) =>
			Array.from({ length: header.chunkCount }, (_, i) =>
				compositeId(path, header.offset, i),
			),
		);
		const chunks = await this.db.getMany<string>('stream_chunks', chunkKeys);
		const batches: ConversationStreamBatch[] = [];
		let chunkIndex = 0;
		for (const header of headers) {
			let data = '';
			for (let i = 0; i < header.chunkCount; i++) {
				const chunk = chunks[chunkIndex++];
				if (!chunk) this.fail('read', path, `Batch ${header.offset} is incomplete.`);
				data += chunk.value;
			}
			batches.push({ offset: header.offset, records: JSON.parse(data) as ConversationRecord[] });
		}
		return {
			batches,
			nextOffset: batches.at(-1)?.offset ?? formatOffset(after),
			upToDate: first + count > head,
		};
	}
	async getMeta(path: string): Promise<ConversationStreamMeta | null> {
		const row = await this.db.get<Meta>('streams', path);
		return row
			? {
					identity: row.value.identity,
					incarnation: row.value.incarnation,
					nextOffset: row.value.nextOffset,
					producerId: row.value.producerId,
					producerEpoch: row.value.producerEpoch,
					nextProducerSequence: row.value.nextProducerSequence,
				}
			: null;
	}
	subscribe(path: string, listener: () => void): () => void {
		let set = listeners.get(path);
		if (!set) {
			set = new Set();
			listeners.set(path, set);
		}
		set.add(listener);
		return () => {
			set!.delete(listener);
			if (!set!.size) listeners.delete(path);
		};
	}
	async putFoldCheckpoint(path: string, checkpoint: ConversationFoldCheckpoint): Promise<void> {
		await this.db.put('meta', `checkpoint:${path}`, checkpoint);
	}
	async getFoldCheckpoint(
		path: string,
		options?: { atOrBefore?: string },
	): Promise<ConversationFoldCheckpoint | null> {
		const row = await this.db.get<ConversationFoldCheckpoint>('meta', `checkpoint:${path}`);
		if (
			!row ||
			(options?.atOrBefore && parseOffset(row.value.offset) > parseOffset(options.atOrBefore))
		)
			return null;
		return row.value;
	}
}
function notify(path: string) {
	for (const listener of listeners.get(path) ?? []) {
		try {
			listener();
		} catch {}
	}
}
async function sha256(value: string) {
	const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
	return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
}
function utf8Chunks(bytes: Uint8Array): string[] {
	const decoder = new TextDecoder('utf-8', { fatal: true });
	const out: string[] = [];
	for (let start = 0; start < bytes.length;) {
		let end = Math.min(start + CHUNK_BYTES, bytes.length);
		while (end < bytes.length && ((bytes[end] ?? 0) & 0xc0) === 0x80) end--;
		out.push(decoder.decode(bytes.subarray(start, end)));
		start = end;
	}
	return out.length ? out : ['[]'];
}
