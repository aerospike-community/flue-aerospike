import type {
	AgentDispatchAdmission,
	AgentSubmission,
	AgentSubmissionInput,
	AgentSubmissionStore,
	DispatchInput,
	SubmissionAttemptRef,
	SubmissionClaimRef,
	SubmissionSettlementObligation,
	SubmissionSettledRecord,
} from '@flue/runtime/adapter';
import {
	admitSubmissionWithBackend,
	createDispatchAgentSubmissionInput,
	DURABILITY_DEFAULT_MAX_ATTEMPTS,
	DURABILITY_DEFAULT_TIMEOUT_MS,
	LEASE_DURATION_MS,
	hydratePersistedSubmissionAttachments,
	prepareSubmissionAttachments,
} from '@flue/runtime/adapter';
import type { SubmissionChunkRow } from '@flue/runtime/adapter';
import { AerospikeRecords, compositeId } from '../storage.ts';

type Row = AgentSubmission & {
	payload: string;
	chunkScope: string;
	hasAttachmentChunks?: boolean;
	settlementRecordId?: string;
	settlementRecord?: SubmissionSettledRecord;
	settlementOutcome?: 'completed' | 'failed' | 'aborted';
	settlementIntentError?: string;
};

export class AerospikeSubmissions implements AgentSubmissionStore {
	constructor(private readonly db: AerospikeRecords) {}
	private async restore(row: Row): Promise<Row> {
		const chunks =
			row.hasAttachmentChunks === false
				? []
				: await this.readChunks(row.submissionId, row.chunkScope);
		return {
			...row,
			input: hydratePersistedSubmissionAttachments(
				JSON.parse(row.payload) as AgentSubmissionInput,
				chunks,
			),
		};
	}
	private async rows() {
		return Promise.all((await this.db.list<Row>('submissions')).map((r) => this.restore(r.value)));
	}
	private async rowsByStatus(...statuses: AgentSubmission['status'][]) {
		const entries = await Promise.all(
			statuses.map((status) => this.db.listByBin<Row>('submissions', 'status', status)),
		);
		const unique = new Map(entries.flat().map((entry) => [entry.id, entry.value]));
		return Promise.all([...unique.values()].map((row) => this.restore(row)));
	}
	private async rowsForSession(sessionKey: string) {
		const rows = await this.db.listByBin<Row>('submissions', 'session_key', sessionKey);
		return Promise.all(rows.map((row) => this.restore(row.value)));
	}
	private async rowsJoinedInto(hostId: string) {
		const rows = await this.db.listByBin<Row>('submissions', 'joined_into', hostId);
		return Promise.all(rows.map((row) => this.restore(row.value)));
	}
	private async row(id: string) {
		const row = (await this.db.get<Row>('submissions', id))?.value;
		return row ? this.restore(row) : null;
	}
	async getSubmission(id: string) {
		const r = await this.row(id);
		return r ? publicRow(r) : null;
	}
	async hasUnsettledSubmissions() {
		return (
			(await this.rowsByStatus('queued', 'running', 'terminalizing', 'joining', 'joined')).length >
			0
		);
	}
	async listRunnableSubmissions() {
		const rows = await this.rowsByStatus('queued', 'running', 'terminalizing', 'joining', 'joined');
		return rows
			.filter(
				(r) =>
					r.status === 'queued' &&
					r.canonicalReadyAt !== null &&
					!rows.some(
						(e) => e.sessionKey === r.sessionKey && unsettled(e.status) && e.sequence < r.sequence,
					),
			)
			.sort(order)
			.map(publicRow);
	}
	async listUnreadySubmissions() {
		return (await this.rowsByStatus('queued'))
			.filter((r) => r.status === 'queued' && r.canonicalReadyAt === null)
			.sort(order)
			.map(publicRow);
	}
	async listRunningSubmissions() {
		return (await this.rowsByStatus('running'))
			.filter((r) => r.status === 'running')
			.sort(order)
			.map(publicRow);
	}
	async listPendingSubmissionSettlements(): Promise<SubmissionSettlementObligation[]> {
		return (await this.rowsByStatus('terminalizing'))
			.filter((r) => r.status === 'terminalizing' && r.settlementRecord && r.settlementRecordId)
			.sort(order)
			.map((r) => ({
				submissionId: r.submissionId,
				sessionKey: r.sessionKey,
				attemptId: r.attemptId!,
				recordId: r.settlementRecordId!,
				record: r.settlementRecord!,
			}));
	}
	async replaceSubmissionAttempt(
		ref: SubmissionAttemptRef,
		nextAttemptId: string,
		lease?: { ownerId: string; leaseExpiresAt: number },
	) {
		const updated = await this.db.update<Row>('submissions', ref.submissionId, (r) =>
			!r || r.status !== 'running' || r.attemptId !== ref.attemptId
				? null
				: {
						...r,
						attemptId: nextAttemptId,
						startedAt: Date.now(),
						attemptCount: r.attemptCount + 1,
						...(lease ? lease : {}),
					},
		);
		return updated ? publicRow(await this.restore(updated)) : null;
	}
	async admitDispatch(input: DispatchInput): Promise<AgentDispatchAdmission> {
		return this.admit(createDispatchAgentSubmissionInput(input));
	}
	async admitDirect(input: AgentSubmissionInput): Promise<AgentSubmission> {
		const result = await this.admit(input);
		if (result.kind !== 'submission')
			throw new Error('[flue] Internal direct admission returned an unexpected result.');
		return result.submission;
	}
	private async admit(input: AgentSubmissionInput): Promise<AgentDispatchAdmission> {
		const prepared = prepareSubmissionAttachments(input);
		let payloadForChunks = JSON.stringify(prepared.value);
		let scopeForChunks = await payloadScope(payloadForChunks, prepared.chunks);
		const admission = await admitSubmissionWithBackend(input, {
			insertIfAbsent: async ({ submissionId, sessionKey, kind, payload, acceptedAt }) => {
				payloadForChunks = payload;
				scopeForChunks = await payloadScope(payload, prepared.chunks);
				if (await this.db.get<Row>('submissions', submissionId)) return;
				await this.replaceChunks(submissionId, prepared.chunks, scopeForChunks);
				for (let retry = 0; retry < 32; retry++) {
					const txn = this.db.transaction(1, 2);
					try {
						const counter = await this.db.getInTransaction<{ value: number }>(
							'meta',
							'submission_sequence',
							txn.policy,
						);
						const sequence = (counter?.value.value ?? 0) + 1;
						if (!Number.isSafeInteger(sequence) || sequence < 1)
							throw new Error(
								'[flue] Aerospike submission sequence exceeded the safe integer range.',
							);
						if (counter) {
							await this.db.putInTransaction(
								'meta',
								'submission_sequence',
								{ value: sequence },
								txn.policy,
							);
						} else {
							await this.db.createInTransaction(
								'meta',
								'submission_sequence',
								{ value: sequence },
								txn.policy,
							);
						}
						const row: Row = {
							submissionId,
							sessionKey,
							kind,
							input: JSON.parse(payload) as AgentSubmissionInput,
							payload,
							status: 'queued',
							sequence,
							chunkScope: scopeForChunks,
							hasAttachmentChunks: prepared.chunks.length > 0,
							acceptedAt,
							canonicalReadyAt: null,
							attemptCount: 0,
							maxAttempts: DURABILITY_DEFAULT_MAX_ATTEMPTS,
							timeoutAt: 0,
							leaseExpiresAt: 0,
						};
						await this.db.createInTransaction('submissions', submissionId, row, txn.policy);
						await txn.commit();
						return;
					} catch (error) {
						if (txn.isInDoubt()) {
							try {
								await txn.commit();
								return;
							} catch {
								// An exact admission replay below resolves the outcome.
							}
						} else {
							try {
								await txn.abort();
							} catch {}
						}
						if (await this.db.get<Row>('submissions', submissionId)) return;
						if ([3, 5, 120, 121].includes(Number((error as { code?: number })?.code))) continue;
						throw error;
					}
				}
				throw new Error('[flue] Aerospike admission sequence contention did not converge.');
			},
			getExisting: async (id) => {
				const row = (await this.db.get<Row>('submissions', id))?.value;
				if (row) {
					payloadForChunks = row.payload;
					scopeForChunks = row.chunkScope;
				}
				return row;
			},
			readChunks: async (id) => this.readChunks(id, scopeForChunks),
			replaceChunks: async (id, chunks) => this.replaceChunks(id, chunks, scopeForChunks),
			parseSubmission: (row, chunks) =>
				publicRow({
					...(row as Row),
					input: hydratePersistedSubmissionAttachments(
						JSON.parse(String((row as Row).payload)),
						chunks,
					),
				}),
		});
		return admission as AgentDispatchAdmission;
	}
	private async readChunks(id: string, scope: string): Promise<SubmissionChunkRow[]> {
		const rows = await this.db.listByBin<
			{ submissionId: string; scope: string } & SubmissionChunkRow
		>('submission_chunks', 'submission_id', id);
		return rows
			.filter((r) => r.value.submissionId === id && r.value.scope === scope)
			.map(({ value }) => ({
				itemId: value.itemId,
				index: value.index,
				count: value.count,
				data: value.data,
			}));
	}
	private async replaceChunks(
		id: string,
		chunks: readonly SubmissionChunkRow[],
		scope: string,
	): Promise<void> {
		const rows = chunks.map((chunk) => ({
			id: compositeId(id, scope, chunk.itemId, chunk.index),
			value: { submissionId: id, scope, ...chunk },
		}));
		const statuses = await this.db.createMany('submission_chunks', rows);
		if (statuses.length !== rows.length)
			throw new Error('@flue/aerospike batch submission chunk write returned incomplete results.');
		const conflicts = rows.filter((_, index) => {
			const status = statuses[index];
			if (status === 0) return false;
			if (status === this.db.api.status.ERR_RECORD_EXISTS) return true;
			throw new Error(`@flue/aerospike batch submission chunk write failed with status ${status}.`);
		});
		if (!conflicts.length) return;
		const existing = await this.db.getMany<{ submissionId: string } & SubmissionChunkRow>(
			'submission_chunks',
			conflicts.map((row) => row.id),
		);
		for (const [i, row] of conflicts.entries()) {
			if (
				!existing[i] ||
				existing[i].value.data !== row.value.data ||
				existing[i].value.count !== row.value.count
			)
				throw new Error('[flue] Conflicting immutable submission attachment chunk.');
		}
	}
	async markSubmissionCanonicalReady(id: string) {
		const row = await this.db.update<Row>('submissions', id, (r) =>
			r?.status !== 'queued' ? null : { ...r, canonicalReadyAt: r.canonicalReadyAt ?? Date.now() },
		);
		return row ? publicRow(await this.restore(row)) : null;
	}
	async claimSubmission(claim: SubmissionClaimRef) {
		for (let retry = 0; retry < 32; retry++) {
			const sequenceSnapshot = await this.db.get<{ value: number }>('meta', 'submission_sequence');
			if (!sequenceSnapshot) return null;
			const candidate = await this.row(claim.submissionId);
			if (!candidate || candidate.status !== 'queued' || candidate.canonicalReadyAt === null)
				return null;
			const earlier = (await this.rowsForSession(candidate.sessionKey)).some(
				(row) => unsettled(row.status) && row.sequence < candidate.sequence,
			);
			if (earlier) return null;

			const txn = this.db.transaction(2, 1);
			try {
				const sequence = await this.db.getInTransaction<{ value: number }>(
					'meta',
					'submission_sequence',
					txn.policy,
				);
				const row = await this.db.getInTransaction<Row>(
					'submissions',
					claim.submissionId,
					txn.policy,
				);
				if (
					!sequence ||
					sequence.generation !== sequenceSnapshot.generation ||
					!row ||
					row.value.status !== 'queued' ||
					row.value.canonicalReadyAt === null
				) {
					await txn.abort();
					continue;
				}
				const now = Date.now();
				const updated: Row = {
					...row.value,
					status: 'running',
					attemptId: claim.attemptId,
					ownerId: claim.ownerId,
					leaseExpiresAt: claim.leaseExpiresAt,
					startedAt: now,
					attemptCount: row.value.attemptCount + 1,
					maxAttempts: DURABILITY_DEFAULT_MAX_ATTEMPTS,
					timeoutAt: row.value.timeoutAt || now + DURABILITY_DEFAULT_TIMEOUT_MS,
				};
				await this.db.putInTransaction('submissions', claim.submissionId, updated, txn.policy);
				await txn.commit();
				return publicRow(await this.restore(updated));
			} catch (error) {
				if (txn.isInDoubt()) {
					try {
						await txn.commit();
					} catch {
						// Recovery uses the durable owner/attempt fence.
					}
				} else {
					try {
						await txn.abort();
					} catch {}
				}
				if ([3, 120, 121].includes(Number((error as { code?: number })?.code))) continue;
				throw error;
			}
		}
		return null;
	}
	async markSubmissionInputApplied(
		ref: SubmissionAttemptRef,
		durability?: { maxAttempts: number; timeoutAt: number },
	) {
		return Boolean(
			await this.db.update<Row>('submissions', ref.submissionId, (r) => {
				if (!owns(r, ref) || r!.inputAppliedAt) return null;
				return {
					...r!,
					inputAppliedAt: Date.now(),
					maxAttempts: durability?.maxAttempts ?? DURABILITY_DEFAULT_MAX_ATTEMPTS,
					timeoutAt: durability?.timeoutAt ?? r!.timeoutAt,
				};
			}),
		);
	}
	async requestSessionAbort(sessionKey: string) {
		const ids: string[] = [];
		for (const r of await this.rowsForSession(sessionKey))
			if (
				r.sessionKey === sessionKey &&
				['queued', 'running', 'joining', 'joined'].includes(r.status)
			) {
				const saved = await this.db.update<Row>('submissions', r.submissionId, (x) =>
					!x ||
					x.sessionKey !== sessionKey ||
					!['queued', 'running', 'joining', 'joined'].includes(x.status)
						? null
						: { ...x, abortRequestedAt: x.abortRequestedAt ?? Date.now() },
				);
				if (saved) ids.push(r.submissionId);
			}
		return ids;
	}
	async requeueSubmission(ref: SubmissionAttemptRef) {
		return Boolean(
			await this.db.update<Row>('submissions', ref.submissionId, (r) => {
				if (!owns(r, ref) || r!.settlementOutcome !== undefined) return null;
				return {
					...r!,
					status: 'queued',
					attemptId: undefined,
					ownerId: undefined,
					leaseExpiresAt: 0,
					inputAppliedAt: undefined,
				};
			}),
		);
	}
	async reserveSubmissionSettlement(
		ref: SubmissionAttemptRef,
		settlement: { recordId: string; record: SubmissionSettledRecord },
	) {
		if (settlement.record.id !== settlement.recordId) return null;
		const current = await this.row(ref.submissionId);
		if (!current) return null;
		if (current.status === 'terminalizing') {
			if (
				current.attemptId !== ref.attemptId ||
				current.settlementRecordId !== settlement.recordId ||
				JSON.stringify(current.settlementRecord) !== JSON.stringify(settlement.record)
			)
				return null;
			return obligation(current);
		}
		if (current.status === 'joined' && current.joinedInto) {
			for (let retry = 0; retry < 16; retry++) {
				const txn = this.db.transaction(2, 2);
				try {
					const host = await this.db.getInTransaction<Row>(
						'submissions',
						current.joinedInto,
						txn.policy,
					);
					const child = await this.db.getInTransaction<Row>(
						'submissions',
						ref.submissionId,
						txn.policy,
					);
					if (
						!host ||
						host.value.status !== 'running' ||
						host.value.attemptId !== ref.attemptId ||
						!child ||
						child.value.status !== 'joined' ||
						child.value.joinedInto !== current.joinedInto ||
						(host.value.settlementOutcome !== undefined &&
							host.value.settlementOutcome !== settlement.record.outcome)
					) {
						await txn.abort();
						return null;
					}
					const hostNext: Row = {
						...host.value,
						settlementOutcome: host.value.settlementOutcome ?? settlement.record.outcome,
						settlementIntentError:
							host.value.settlementIntentError ?? recordError(settlement.record),
					};
					await this.db.putInTransaction('submissions', current.joinedInto, hostNext, txn.policy);
					const childNext: Row = {
						...child.value,
						attemptId: host.value.attemptId,
						startedAt: host.value.startedAt,
						status: 'terminalizing',
						settlementOutcome: settlement.record.outcome,
						settlementIntentError: recordError(settlement.record),
						settlementRecordId: settlement.recordId,
						settlementRecord: settlement.record,
					};
					await this.db.putInTransaction('submissions', ref.submissionId, childNext, txn.policy);
					await txn.commit();
					return obligation(childNext);
				} catch (error) {
					if (txn.isInDoubt()) {
						try {
							await txn.commit();
							const saved = await this.row(ref.submissionId);
							if (
								saved?.status === 'terminalizing' &&
								saved.settlementRecordId === settlement.recordId
							)
								return obligation(saved);
						} catch {}
					} else {
						try {
							await txn.abort();
						} catch {}
					}
					if ([3, 120, 121].includes(Number((error as { code?: number })?.code))) continue;
					throw error;
				}
			}
			return null;
		}
		if (current.status !== 'running' || current.attemptId !== ref.attemptId) return null;
		const saved = await this.db.update<Row>('submissions', ref.submissionId, (r) => {
			if (!r || r.status !== 'running' || r.attemptId !== ref.attemptId) return null;
			if (r.settlementOutcome !== undefined && r.settlementOutcome !== settlement.record.outcome)
				return null;
			return {
				...r,
				status: 'terminalizing',
				settlementOutcome: r.settlementOutcome ?? settlement.record.outcome,
				settlementIntentError: r.settlementIntentError ?? recordError(settlement.record),
				settlementRecordId: settlement.recordId,
				settlementRecord: settlement.record,
			};
		});
		return saved ? obligation(saved) : null;
	}
	async finalizeSubmissionSettlement(
		ref: SubmissionAttemptRef,
		recordId: string,
		options?: { errorMessage?: string },
	) {
		const pending = await this.row(ref.submissionId);
		if (
			!pending ||
			pending.status !== 'terminalizing' ||
			pending.attemptId !== ref.attemptId ||
			pending.settlementRecordId !== recordId
		)
			return false;
		if (!pending.joinedInto) {
			const error =
				options?.errorMessage ??
				pending.settlementIntentError ??
				(pending.settlementRecord ? recordError(pending.settlementRecord) : undefined);
			if (
				!(await this.settleJoinedBackstop(
					ref.submissionId,
					pending.settlementOutcome ?? pending.settlementRecord?.outcome ?? 'completed',
					error,
				))
			)
				return false;
		}
		return Boolean(
			await this.db.update<Row>('submissions', ref.submissionId, (r) => {
				if (
					!r ||
					r.status !== 'terminalizing' ||
					r.attemptId !== ref.attemptId ||
					r.settlementRecordId !== recordId
				)
					return null;
				return {
					...r,
					status: 'settled',
					settledAt: Date.now(),
					error:
						options?.errorMessage ??
						r.settlementIntentError ??
						(r.settlementRecord ? recordError(r.settlementRecord) : undefined),
				};
			}),
		);
	}
	async completeSubmission(ref: SubmissionAttemptRef) {
		return this.settleHost(ref, 'completed');
	}
	async failSubmission(ref: SubmissionAttemptRef, error: unknown) {
		return this.settleHost(ref, 'failed', error instanceof Error ? error.message : String(error));
	}
	private async settleHost(
		ref: SubmissionAttemptRef,
		outcome: 'completed' | 'failed',
		error?: string,
	) {
		const host = await this.row(ref.submissionId);
		if (!host || host.status !== 'running' || host.attemptId !== ref.attemptId) return false;
		const intent = await this.db.update<Row>('submissions', ref.submissionId, (r) => {
			if (!r || r.status !== 'running' || r.attemptId !== ref.attemptId) return null;
			if (r.settlementOutcome && r.settlementOutcome !== outcome) return r;
			return {
				...r,
				settlementOutcome: r.settlementOutcome ?? outcome,
				settlementIntentError: r.settlementIntentError ?? error,
			};
		});
		if (!intent) return false;
		const finalOutcome = intent.settlementOutcome ?? outcome;
		const finalError =
			intent.settlementOutcome !== undefined ? intent.settlementIntentError : error;
		// Persist each joined row first and in admission order. The host remains
		// running until every backstop row has advanced, so retries resume safely.
		if (!(await this.settleJoinedBackstop(host.submissionId, finalOutcome, finalError)))
			return false;
		return Boolean(
			await this.db.update<Row>('submissions', ref.submissionId, (r) => {
				if (!owns(r, ref)) return null;
				return {
					...r!,
					status: 'settled',
					settlementOutcome: finalOutcome,
					settledAt: Date.now(),
					error: finalError,
				};
			}),
		);
	}
	private async settleJoinedBackstop(
		hostId: string,
		outcome: 'completed' | 'failed' | 'aborted',
		error?: string,
	) {
		const joins = (await this.rowsJoinedInto(hostId))
			.filter((r) => ['joining', 'joined', 'terminalizing'].includes(r.status))
			.sort(order);
		for (const join of joins) {
			if (join.status === 'terminalizing') return false;
			const progressed = await this.db.update<Row>('submissions', join.submissionId, (r) => {
				if (!r || r.joinedInto !== hostId) return null;
				if (r.status === 'joining') return { ...r, status: 'queued', joinedInto: undefined };
				return r.status === 'joined'
					? { ...r, status: 'settled', settlementOutcome: outcome, settledAt: Date.now(), error }
					: null;
			});
			if (!progressed) {
				const latest = await this.row(join.submissionId);
				if (latest?.status === 'terminalizing') return false;
			}
		}
		return true;
	}
	async settleQueuedSubmission(id: string, outcome: 'failed' | 'aborted', error: unknown) {
		return Boolean(
			await this.db.update<Row>('submissions', id, (r) =>
				!r || r.status !== 'queued'
					? null
					: {
							...r,
							status: 'settled',
							settlementOutcome: outcome,
							settledAt: Date.now(),
							error: error instanceof Error ? error.message : String(error),
						},
			),
		);
	}
	async claimJoinableSubmissions(hostRef: SubmissionAttemptRef, agentName: string) {
		const host = await this.row(hostRef.submissionId);
		if (!host || host.status !== 'running' || host.attemptId !== hostRef.attemptId) return [];
		const sequenceSnapshot = await this.db.get<{ value: number }>('meta', 'submission_sequence');
		if (!sequenceSnapshot) return [];
		const candidates = (await this.rowsForSession(host.sessionKey))
			.filter((r) => unsettled(r.status) && r.sequence > host.sequence)
			.sort(order);
		const claimed: AgentSubmission[] = [];
		for (const c of candidates) {
			if (c.status === 'joined' && c.joinedInto === host.submissionId) continue;
			if (c.status !== 'queued') break;
			if (
				c.canonicalReadyAt === null ||
				c.input.agent !== agentName ||
				c.abortRequestedAt !== undefined
			)
				break;
			const txn = this.db.transaction(3, 1);
			try {
				const [currentHost, sequence, current] = await Promise.all([
					this.db.getInTransaction<Row>('submissions', hostRef.submissionId, txn.policy),
					this.db.getInTransaction<{ value: number }>('meta', 'submission_sequence', txn.policy),
					this.db.getInTransaction<Row>('submissions', c.submissionId, txn.policy),
				]);
				if (
					!currentHost ||
					currentHost.value.status !== 'running' ||
					currentHost.value.attemptId !== hostRef.attemptId ||
					!sequence ||
					sequence.generation !== sequenceSnapshot.generation ||
					!current ||
					current.value.status !== 'queued' ||
					current.value.sessionKey !== host.sessionKey ||
					current.value.canonicalReadyAt === null ||
					current.value.input.agent !== agentName ||
					current.value.abortRequestedAt !== undefined
				) {
					await txn.abort();
					break;
				}
				const updated: Row = {
					...current.value,
					status: 'joining',
					joinedInto: host.submissionId,
				};
				await this.db.putInTransaction('submissions', c.submissionId, updated, txn.policy);
				await txn.commit();
				claimed.push(publicRow(await this.restore(updated)));
			} catch (error) {
				if (txn.isInDoubt()) {
					try {
						await txn.commit();
					} catch {
						// The host retry resolves an uncertain per-row claim.
					}
				} else {
					try {
						await txn.abort();
					} catch {}
				}
				if ([3, 120, 121].includes(Number((error as { code?: number })?.code))) break;
				throw error;
			}
		}
		return claimed;
	}
	async finalizeJoinedSubmission(host: SubmissionAttemptRef, id: string) {
		return this.joinTransition(host, id, (r) => ({
			...r,
			status: 'joined',
			inputAppliedAt: r.inputAppliedAt ?? Date.now(),
		}));
	}
	async revertJoiningSubmission(host: SubmissionAttemptRef, id: string) {
		return this.joinTransition(host, id, (r) => ({
			...r,
			status: 'queued',
			joinedInto: undefined,
		}));
	}
	private async joinTransition(hostRef: SubmissionAttemptRef, id: string, change: (r: Row) => Row) {
		const txn = this.db.transaction(2, 1);
		try {
			const [host, row] = await Promise.all([
				this.db.getInTransaction<Row>('submissions', hostRef.submissionId, txn.policy),
				this.db.getInTransaction<Row>('submissions', id, txn.policy),
			]);
			if (
				!host ||
				host.value.status !== 'running' ||
				host.value.attemptId !== hostRef.attemptId ||
				!row ||
				row.value.status !== 'joining' ||
				row.value.joinedInto !== hostRef.submissionId
			) {
				await txn.abort();
				return false;
			}
			await this.db.putInTransaction('submissions', id, change(row.value), txn.policy);
			await txn.commit();
			return true;
		} catch (error) {
			if (txn.isInDoubt()) {
				try {
					await txn.commit();
					return true;
				} catch {}
			} else {
				try {
					await txn.abort();
				} catch {}
			}
			if ([3, 120, 121].includes(Number((error as { code?: number })?.code))) return false;
			throw error;
		}
	}
	async listJoinedSubmissions(id: string) {
		return (await this.rowsJoinedInto(id))
			.filter((r) => ['joining', 'joined'].includes(r.status))
			.sort(order)
			.map(publicRow);
	}
	async renewLeases(ownerId: string, ids: string[]) {
		for (const id of ids)
			await this.db.update<Row>('submissions', id, (r) =>
				!r || r.status !== 'running' || r.ownerId !== ownerId
					? null
					: { ...r, leaseExpiresAt: Date.now() + LEASE_DURATION_MS },
			);
	}
	async listExpiredSubmissions() {
		const now = Date.now();
		return (await this.rowsByStatus('running'))
			.filter((r) => r.status === 'running' && r.leaseExpiresAt > 0 && r.leaseExpiresAt < now)
			.sort(order)
			.map(publicRow);
	}
}
function owns(r: Row | null, ref: SubmissionAttemptRef) {
	return Boolean(r && r.status === 'running' && r.attemptId === ref.attemptId);
}
function unsettled(s: string) {
	return ['queued', 'running', 'terminalizing', 'joining', 'joined'].includes(s);
}
function order(a: Row, b: Row) {
	return a.sequence - b.sequence;
}
function obligation(row: Row): SubmissionSettlementObligation {
	if (!row.attemptId || !row.settlementRecordId || !row.settlementRecord)
		throw new Error('[flue] Aerospike terminalizing row is missing its settlement obligation.');
	return {
		submissionId: row.submissionId,
		sessionKey: row.sessionKey,
		attemptId: row.attemptId,
		recordId: row.settlementRecordId,
		record: row.settlementRecord,
	};
}
function recordError(record: SubmissionSettledRecord): string | undefined {
	const error = (record as unknown as { error?: unknown }).error;
	if (typeof error === 'string') return error;
	if (error && typeof error === 'object' && 'message' in error)
		return String((error as { message: unknown }).message);
	return undefined;
}
function publicRow(r: Row): AgentSubmission {
	const {
		payload: _payload,
		settlementRecord: _sr,
		settlementRecordId: _si,
		settlementOutcome: _so,
		settlementIntentError: _se,
		hasAttachmentChunks: _hasAttachmentChunks,
		...result
	} = r;
	return result;
}
async function payloadScope(
	payload: string,
	chunks: readonly SubmissionChunkRow[],
): Promise<string> {
	const digest = await crypto.subtle.digest(
		'SHA-256',
		new TextEncoder().encode(JSON.stringify([payload, chunks])),
	);
	return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
}
