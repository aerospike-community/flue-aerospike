import type { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import { PersistedFormatVersionError } from '@flue/runtime/adapter';
export interface AerospikeRecord {
	key: AerospikeKey;
	bins: Record<string, unknown>;
	gen: number;
}
export interface AerospikeKey {
	ns: string;
	set: string;
	key: string | number | Buffer;
}
export interface AerospikeQuery {
	where(filter: unknown): void;
	foreach(
		policy?: object,
		dataCb?: (record: AerospikeRecord) => void,
		errorCb?: (error: Error) => void,
		endCb?: () => void,
	): { on(event: string, callback: (...args: unknown[]) => void): void };
}
export interface AerospikeTransaction {
	commit(): Promise<unknown>;
	abort(): Promise<unknown>;
}
/** Structural surface of the official driver; the adapter never creates or closes extra clients. */
export interface AerospikeClient {
	isConnected(): boolean;
	close(): void | Promise<void>;
	get(key: AerospikeKey, policy?: object): Promise<AerospikeRecord>;
	put(
		key: AerospikeKey,
		bins: Record<string, unknown>,
		meta?: object | null,
		policy?: object,
	): Promise<unknown>;
	remove(key: AerospikeKey, policy?: object): Promise<unknown>;
	query(namespace: string, set: string, options?: object): AerospikeQuery;
	commit(transaction: unknown): Promise<unknown>;
	abort(transaction: unknown): Promise<unknown>;
	createIndex(options: object): Promise<{ waitUntilDone?(interval?: number): Promise<unknown> }>;
	infoAll(request: string): Promise<Array<{ info: string }>>;
	batchRead(records: object[]): Promise<AerospikeBatchResult[]>;
	batchWrite(records: object[]): Promise<AerospikeBatchResult[]>;
}
export interface AerospikeBatchResult {
	status: number;
	record?: AerospikeRecord;
}
export interface AerospikeApi {
	Key: new (namespace: string, set: string, key: string | number) => AerospikeKey;
	Transaction: new (
		reads?: number,
		writes?: number,
	) => {
		getInDoubt(): boolean;
	};
	indexDataType: { STRING: number; INTEGER: number };
	filter: { equal(bin: string, value: string | number): unknown };
	operations: { write(bin: string, value: unknown): unknown };
	batchType: { BATCH_WRITE: number };
	BatchWritePolicy: new (policy: object) => object;
	policy: { exists: { CREATE: number } };
	status: { ERR_INDEX_FOUND: number; ERR_RECORD_EXISTS: number };
}
export interface AerospikeRunner {
	client: AerospikeClient;
	api: AerospikeApi;
}
export interface AerospikeOptions {
	namespace: string;
	setPrefix?: string;
}
export interface VersionedRow<T> {
	value: T;
	id: string;
	set: string;
	generation: number;
	key: AerospikeKey;
}

const NOT_FOUND = 2;
const GEN_MISMATCH = 3;
const MAX_CAS_ATTEMPTS = 64;
const MAX_BATCH_RECORDS = 1000;

export class AerospikeRecords {
	constructor(
		readonly client: AerospikeClient,
		readonly api: AerospikeApi,
		readonly namespace: string,
		readonly prefix: string,
	) {}
	key(set: string, id: string): AerospikeKey {
		const setName = `${this.prefix}${set}`;
		const digest = createHash('sha256')
			.update(JSON.stringify([setName, id]), 'utf8')
			.digest('hex');
		return new this.api.Key(this.namespace, setName, digest);
	}
	async get<T>(set: string, id: string): Promise<VersionedRow<T> | null> {
		const key = this.key(set, id);
		try {
			const record = await this.client.get(key);
			if (record.bins.logical_id !== id)
				throw new Error(`@flue/aerospike primary-key digest collision in ${set}.`);
			return {
				key,
				set,
				id,
				value: JSON.parse(String(record.bins.data)) as T,
				generation: record.gen,
			};
		} catch (error) {
			if (errorCode(error) === NOT_FOUND) return null;
			throw error;
		}
	}
	async getInTransaction<T>(
		set: string,
		id: string,
		policy: object,
	): Promise<VersionedRow<T> | null> {
		const key = this.key(set, id);
		try {
			const record = await this.client.get(key, policy);
			if (record.bins.logical_id !== id)
				throw new Error(`@flue/aerospike primary-key digest collision in ${set}.`);
			return {
				key,
				set,
				id,
				value: JSON.parse(String(record.bins.data)) as T,
				generation: record.gen,
			};
		} catch (error) {
			if (errorCode(error) === NOT_FOUND) return null;
			throw error;
		}
	}
	async put<T>(set: string, id: string, value: T, policy?: object): Promise<void> {
		await this.client.put(this.key(set, id), binsFor(value, id), null, policy);
	}
	async putInTransaction<T>(set: string, id: string, value: T, policy: object): Promise<void> {
		await this.client.put(this.key(set, id), binsFor(value, id), null, policy);
	}
	async getMany<T>(set: string, ids: readonly string[]): Promise<Array<VersionedRow<T> | null>> {
		const rows: Array<VersionedRow<T> | null> = [];
		for (let start = 0; start < ids.length; start += MAX_BATCH_RECORDS) {
			const page = ids.slice(start, start + MAX_BATCH_RECORDS);
			const results = await this.client.batchRead(
				page.map((id) => ({ key: this.key(set, id), readAllBins: true })),
			);
			rows.push(
				...page.map((id, index) => {
					const result = results[index];
					if (!result)
						throw new Error(`@flue/aerospike batch read returned no result for ${set}/${id}.`);
					if (result.status === NOT_FOUND) return null;
					if (result.status !== 0)
						throw new Error(
							`@flue/aerospike batch read failed for ${set}/${id} with status ${result.status}.`,
						);
					if (!result.record)
						throw new Error(`@flue/aerospike batch read succeeded without a record for ${set}/${id}.`);
					if (result.record.bins.logical_id !== id)
						throw new Error(`@flue/aerospike primary-key digest collision in ${set}.`);
					return {
						key: this.key(set, id),
						set,
						id,
						value: JSON.parse(String(result.record.bins.data)) as T,
						generation: result.record.gen,
					};
				}),
			);
		}
		return rows;
	}
	async createMany<T>(set: string, rows: readonly { id: string; value: T }[]): Promise<number[]> {
		const statuses: number[] = [];
		for (let start = 0; start < rows.length; start += MAX_BATCH_RECORDS) {
			const records = rows.slice(start, start + MAX_BATCH_RECORDS).map(({ id, value }) => ({
				type: this.api.batchType.BATCH_WRITE,
				key: this.key(set, id),
				ops: Object.entries(binsFor(value, id)).map(([bin, data]) =>
					this.api.operations.write(bin, data),
				),
				policy: new this.api.BatchWritePolicy({
					exists: this.api.policy.exists.CREATE,
				}),
			}));
			const results = await this.client.batchWrite(records);
			statuses.push(...results.map((result) => result.status));
		}
		return statuses;
	}
	async createInTransaction<T>(set: string, id: string, value: T, policy: object): Promise<void> {
		await this.client.put(this.key(set, id), binsFor(value, id), null, {
			...policy,
			exists: 1,
		});
	}
	async create<T>(set: string, id: string, value: T): Promise<boolean> {
		try {
			await this.client.put(this.key(set, id), binsFor(value, id), null, { exists: 1 });
			return true;
		} catch (error) {
			if (errorCode(error) === 5) return false;
			throw error;
		}
	}
	async remove(set: string, id: string): Promise<void> {
		try {
			await this.client.remove(this.key(set, id));
		} catch (error) {
			if (errorCode(error) !== NOT_FOUND) throw error;
		}
	}
	async replace<T>(row: VersionedRow<unknown>, value: T): Promise<boolean> {
		try {
			await this.client.put(row.key, binsFor(value, row.id), { gen: row.generation }, { gen: 1 });
			return true;
		} catch (error) {
			if (errorCode(error) === GEN_MISMATCH || errorCode(error) === NOT_FOUND) return false;
			throw error;
		}
	}
	async update<T>(
		set: string,
		id: string,
		change: (current: T | null) => T | null,
	): Promise<T | null> {
		for (let i = 0; i < MAX_CAS_ATTEMPTS; i++) {
			const row = await this.get<T>(set, id);
			const next = change(row?.value ?? null);
			if (next === null) return null;
			const saved = row ? await this.replace(row, next) : await this.create(set, id, next);
			if (saved) return next;
		}
		throw new Error(`@flue/aerospike conditional update did not converge for ${set}/${id}.`);
	}
	async listByBin<T>(
		set: string,
		bin: string,
		value: string | number,
	): Promise<Array<{ id: string; value: T; generation: number }>> {
		const query = this.client.query(this.namespace, `${this.prefix}${set}`);
		query.where(this.api.filter.equal(bin, value));
		return this.collect(query);
	}
	async list<T>(set: string): Promise<Array<{ id: string; value: T; generation: number }>> {
		return this.collect(this.client.query(this.namespace, `${this.prefix}${set}`));
	}
	private async collect<T>(
		query: AerospikeQuery,
	): Promise<Array<{ id: string; value: T; generation: number }>> {
		const rows: AerospikeRecord[] = [];
		await new Promise<void>((resolve, reject) => {
			const stream = query.foreach();
			stream.on('data', (record: unknown) => rows.push(record as AerospikeRecord));
			stream.on('error', (error: unknown) => reject(error));
			stream.on('end', () => resolve());
		});
		return rows.map((r) => ({
			id: String(r.bins.logical_id),
			value: JSON.parse(String(r.bins.data)) as T,
			generation: r.gen,
		}));
	}
	async migrate(formatVersion: number): Promise<void> {
		const namespaceConfig = await this.client.infoAll(
			`get-config:context=namespace;id=${this.namespace}`,
		);
		if (
			namespaceConfig.length === 0 ||
			namespaceConfig.some((node) => !node.info.includes('strong-consistency=true'))
		)
			throw new Error(
				`@flue/aerospike requires namespace "${this.namespace}" to use strong consistency.`,
			);
		const meta = await this.get<{ formatVersion: number }>('meta', 'format');
		if (!meta) {
			const existing = await Promise.all(
				[
					'submissions',
					'submission_chunks',
					'streams',
					'stream_batches',
					'stream_chunks',
					'attachments',
					'attachment_chunks',
				].map((set) => this.list(set)),
			);
			if (existing.some((rows) => rows.length > 0))
				throw new PersistedFormatVersionError({
					storedVersion: 'unversioned',
					supportedVersion: formatVersion,
				});
		} else if (meta.value.formatVersion !== formatVersion) {
			throw new PersistedFormatVersionError({
				storedVersion: String(meta.value.formatVersion),
				supportedVersion: formatVersion,
			});
		}
		for (const index of indexSpecs(this.prefix, this.api.indexDataType)) {
			try {
				const job = await this.client.createIndex({
					ns: this.namespace,
					set: `${this.prefix}${index.set}`,
					bin: index.bin,
					index: index.name,
					datatype: index.datatype,
				});
				await job.waitUntilDone?.(100);
			} catch (error) {
				if (errorCode(error) !== this.api.status.ERR_INDEX_FOUND) throw error;
			}
		}
		if (!meta && !(await this.create('meta', 'format', { formatVersion })))
			return this.migrate(formatVersion);
	}
	transaction(
		reads: number,
		writes: number,
	): {
		policy: { txn: unknown };
		commit: () => Promise<unknown>;
		abort: () => Promise<unknown>;
		isInDoubt: () => boolean;
	} {
		const txn = new this.api.Transaction(reads, writes);
		return {
			policy: { txn },
			commit: () => this.client.commit(txn),
			abort: () => this.client.abort(txn),
			isInDoubt: () => txn.getInDoubt(),
		};
	}
}
export function errorCode(error: unknown): number | undefined {
	return typeof error === 'object' && error !== null && 'code' in error
		? Number((error as { code: unknown }).code)
		: undefined;
}
function binsFor(value: unknown, logicalId: string): Record<string, unknown> {
	const bins: Record<string, unknown> = { data: JSON.stringify(value), logical_id: logicalId };
	if (value && typeof value === 'object') {
		const row = value as Record<string, unknown>;
		if (typeof row.status === 'string') bins.status = row.status;
		if (typeof row.path === 'string') bins.path = row.path;
		if (typeof row.sessionKey === 'string') bins.session_key = row.sessionKey;
		if (typeof row.sequence === 'number') bins.sequence = row.sequence;
		if (typeof row.producerSequence === 'number') bins.prod_seq = row.producerSequence;
		if (typeof row.joinedInto === 'string') bins.joined_into = row.joinedInto;
		if (typeof row.submissionId === 'string') bins.submission_id = row.submissionId;
	}
	return bins;
}

export function compositeId(...parts: readonly (string | number)[]): string {
	return JSON.stringify(parts);
}

function indexSpecs(prefix: string, types: AerospikeApi['indexDataType']) {
	const { STRING, INTEGER } = types;
	const specs = [
		{ set: 'submissions', bin: 'status', datatype: STRING },
		{ set: 'submissions', bin: 'session_key', datatype: STRING },
		{ set: 'submissions', bin: 'joined_into', datatype: STRING },
		{ set: 'submission_chunks', bin: 'submission_id', datatype: STRING },
		{ set: 'streams', bin: 'path', datatype: STRING },
		{ set: 'stream_batches', bin: 'path', datatype: STRING },
		{ set: 'stream_batches', bin: 'prod_seq', datatype: INTEGER },
	];
	return specs.map((spec) => ({
		...spec,
		name: `flue_${createHash('sha256')
			.update(JSON.stringify([prefix, spec.set, spec.bin]), 'utf8')
			.digest('hex')
			.slice(0, 24)}`,
	}));
}
