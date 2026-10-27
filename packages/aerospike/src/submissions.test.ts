import { describe, expect, it } from 'vitest';
import { defineStoreContractTests } from '@flue/runtime/test-utils';
import { defineAttachmentStoreContractTests } from '@flue/runtime/test-utils/attachment-store';
import { AerospikeAttachments } from './stores/attachments.ts';
import { AerospikeSubmissions } from './stores/submissions.ts';
import type { AerospikeRecords } from './storage.ts';

class MemoryRows {
	rows = new Map<string, unknown>();
	failId: string | undefined;
	chunkLookups = 0;
	getManyCalls: Array<{ set: string; ids: readonly string[] }> = [];
	createManyCalls: Array<{ set: string; rows: readonly { id: string; value: unknown }[] }> = [];
	api = { status: { ERR_RECORD_EXISTS: 5 } };
	private key(set: string, id: string) {
		return `${set}/${id}`;
	}
	async get<T>(set: string, id: string) {
		const value = this.rows.get(this.key(set, id)) as T | undefined;
		return value === undefined ? null : { value, key: { key: id }, generation: 1 };
	}
	async create<T>(set: string, id: string, value: T) {
		const key = this.key(set, id);
		if (this.rows.has(key)) return false;
		this.rows.set(key, value);
		return true;
	}
	async put<T>(set: string, id: string, value: T) {
		this.rows.set(this.key(set, id), value);
	}
	async remove(set: string, id: string) {
		this.rows.delete(this.key(set, id));
	}
	async list<T>(set: string) {
		return [...this.rows]
			.filter(([key]) => key.startsWith(`${set}/`))
			.map(([key, value]) => ({ id: key.slice(set.length + 1), value: value as T, generation: 1 }));
	}
	async listByBin<T>(set: string, bin: string, value: string | number) {
		if (bin === 'submission_id') this.chunkLookups++;
		const property: Record<string, string> = {
			session_key: 'sessionKey',
			joined_into: 'joinedInto',
			submission_id: 'submissionId',
		};
		const field = property[bin] ?? bin;
		return (await this.list<T>(set)).filter(
			(row) => row.value && (row.value as Record<string, unknown>)[field] === value,
		);
	}
	async getMany<T>(set: string, ids: readonly string[]) {
		this.getManyCalls.push({ set, ids });
		return Promise.all(ids.map((id) => this.get<T>(set, id)));
	}
	async createMany<T>(set: string, rows: readonly { id: string; value: T }[]) {
		this.createManyCalls.push({ set, rows });
		const statuses: number[] = [];
		for (const row of rows) statuses.push((await this.create(set, row.id, row.value)) ? 0 : 5);
		return statuses;
	}
	async getInTransaction<T>(set: string, id: string) {
		return this.get<T>(set, id);
	}
	async putInTransaction<T>(set: string, id: string, value: T) {
		this.rows.set(this.key(set, id), value);
	}
	async createInTransaction<T>(set: string, id: string, value: T) {
		return this.create(set, id, value);
	}
	transaction() {
		return {
			policy: { txn: {} },
			commit: async () => {},
			abort: async () => {},
			isInDoubt: () => false,
		};
	}
	async update<T>(set: string, id: string, change: (current: T | null) => T | null) {
		if (this.failId === id) {
			this.failId = undefined;
			throw new Error('simulated transient failure');
		}
		const key = this.key(set, id);
		const current = this.rows.get(key) as T | undefined;
		const next = change(current ?? null);
		if (next === null) return null;
		this.rows.set(key, next);
		return next;
	}
}
const input = (id: string, acceptedAt: string) => ({
	kind: 'direct' as const,
	submissionId: id,
	agent: 'assistant',
	id: 'one',
	message: { kind: 'user' as const, body: id },
	acceptedAt,
});
async function ready(store: AerospikeSubmissions, id: string, date: string) {
	await store.admitDirect(input(id, date));
	await store.markSubmissionCanonicalReady(id);
}
async function runningHost(store: AerospikeSubmissions) {
	await ready(store, 'host', '2026-01-01T00:00:00.000Z');
	return store.claimSubmission({
		submissionId: 'host',
		attemptId: 'attempt-host',
		ownerId: 'owner',
		leaseExpiresAt: Date.now() + 60_000,
	});
}

describe('Aerospike submission per-row recovery', () => {
	it('batch-creates chunks and batch-reads only immutable conflicts', async () => {
		const db = new MemoryRows();
		const store = new AerospikeSubmissions(db as unknown as AerospikeRecords);
		const chunks = [
			{ itemId: 'existing', index: 0, count: 1, data: 'same' },
			{ itemId: 'new', index: 0, count: 1, data: 'new' },
		];
		const existingKey = JSON.stringify(['submission', 'scope', 'existing', 0]);
		db.rows.set(`submission_chunks/${existingKey}`, {
			submissionId: 'submission',
			scope: 'scope',
			...chunks[0],
		});

		await (
			store as unknown as {
				replaceChunks(
					id: string,
					chunks: readonly {
						itemId: string;
						index: number;
						count: number;
						data: string;
					}[],
					scope: string,
				): Promise<void>;
			}
		).replaceChunks('submission', chunks, 'scope');

		expect(db.createManyCalls).toHaveLength(1);
		expect(db.createManyCalls[0]?.rows).toHaveLength(2);
		expect(db.getManyCalls).toEqual([
			{ set: 'submission_chunks', ids: [existingKey] },
		]);
	});

	it('skips chunk queries when restoring submissions without attachment chunks', async () => {
		const db = new MemoryRows();
		const store = new AerospikeSubmissions(db as unknown as AerospikeRecords);
		await ready(store, 'plain-1', '2026-01-01T00:00:00.000Z');
		await ready(store, 'plain-2', '2026-01-01T00:00:01.000Z');
		db.chunkLookups = 0;

		expect(await store.listRunnableSubmissions()).toHaveLength(1);
		expect(db.chunkLookups).toBe(0);
	});

	it('preserves object-shaped settlement error messages during finalization', async () => {
		const store = new AerospikeSubmissions(new MemoryRows() as unknown as AerospikeRecords);
		await runningHost(store);
		const ref = { submissionId: 'host', attemptId: 'attempt-host' };
		const record = {
			v: 1,
			id: 'settlement',
			type: 'submission_settled',
			conversationId: 'conversation',
			harness: 'default',
			session: 'default',
			timestamp: '2026-01-01T00:00:00.000Z',
			submissionId: 'host',
			attemptId: 'attempt-host',
			outcome: 'failed',
			error: { message: 'submission failed' },
		} as never;

		await store.reserveSubmissionSettlement(ref, { recordId: 'settlement', record });
		expect(await store.finalizeSubmissionSettlement(ref, 'settlement')).toBe(true);
		expect((await store.getSubmission('host'))?.error).toBe('submission failed');
	});

	it('claims a strict ordered prefix and never passes an unresolved earlier row', async () => {
		const db = new MemoryRows();
		const store = new AerospikeSubmissions(db as unknown as AerospikeRecords);
		await runningHost(store);
		await store.admitDirect(input('earlier', '2026-01-01T00:00:01.000Z'));
		await ready(store, 'later', '2026-01-01T00:00:02.000Z');
		expect(
			await store.claimJoinableSubmissions(
				{ submissionId: 'host', attemptId: 'attempt-host' },
				'assistant',
			),
		).toEqual([]);
		await store.markSubmissionCanonicalReady('earlier');
		expect(
			(
				await store.claimJoinableSubmissions(
					{ submissionId: 'host', attemptId: 'attempt-host' },
					'assistant',
				)
			).map((r) => r.submissionId),
		).toEqual(['earlier', 'later']);
	});

	it('keeps the host nonterminal while a joined-row backstop has partial progress, then retries', async () => {
		const db = new MemoryRows();
		const store = new AerospikeSubmissions(db as unknown as AerospikeRecords);
		await runningHost(store);
		await ready(store, 'joined-1', '2026-01-01T00:00:01.000Z');
		await ready(store, 'joined-2', '2026-01-01T00:00:02.000Z');
		const host = { submissionId: 'host', attemptId: 'attempt-host' };
		await store.claimJoinableSubmissions(host, 'assistant');
		await store.finalizeJoinedSubmission(host, 'joined-1');
		await store.finalizeJoinedSubmission(host, 'joined-2');
		db.failId = 'joined-2';
		await expect(store.completeSubmission(host)).rejects.toThrow('simulated transient failure');
		expect((await store.getSubmission('host'))?.status).toBe('running');
		expect((await store.getSubmission('joined-1'))?.status).toBe('settled');
		expect((await store.getSubmission('joined-2'))?.status).toBe('joined');
		expect(await store.completeSubmission(host)).toBe(true);
		expect((await store.getSubmission('host'))?.status).toBe('settled');
		expect((await store.getSubmission('joined-2'))?.status).toBe('settled');
	});

	it('preserves the first durable fan-out outcome when a retry proposes another outcome', async () => {
		const db = new MemoryRows();
		const store = new AerospikeSubmissions(db as unknown as AerospikeRecords);
		await runningHost(store);
		await ready(store, 'joined-1', '2026-01-01T00:00:01.000Z');
		await ready(store, 'joined-2', '2026-01-01T00:00:02.000Z');
		const host = { submissionId: 'host', attemptId: 'attempt-host' };
		await store.claimJoinableSubmissions(host, 'assistant');
		await store.finalizeJoinedSubmission(host, 'joined-1');
		await store.finalizeJoinedSubmission(host, 'joined-2');

		db.failId = 'joined-2';
		await expect(store.completeSubmission(host)).rejects.toThrow('simulated transient failure');
		expect(await store.failSubmission(host, new Error('late failure'))).toBe(true);
		expect(await store.getSubmission('host')).toMatchObject({ status: 'settled' });
		expect((await store.getSubmission('host'))?.error).toBeUndefined();
		expect(await store.getSubmission('joined-2')).toMatchObject({ status: 'settled' });
		expect((await store.getSubmission('joined-2'))?.error).toBeUndefined();
	});
});

defineStoreContractTests('Aerospike submission store contract (CAS test double)', {
	create: () => new AerospikeSubmissions(new MemoryRows() as unknown as AerospikeRecords),
});

defineAttachmentStoreContractTests('Aerospike attachment store contract (record test double)', {
	create: () => new AerospikeAttachments(new MemoryRows() as unknown as AerospikeRecords),
});
