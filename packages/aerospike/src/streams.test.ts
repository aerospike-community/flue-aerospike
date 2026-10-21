import { describe, expect, it } from 'vitest';
import { AerospikeStreams } from './stores/streams.ts';
import type { AerospikeRecords } from './storage.ts';

class StreamRows {
	data = new Map<string, { value: unknown; generation: number }>();
	failNextCommit = false;
	writes = 0;
	listByBinCalls = 0;
	getManyCalls: Array<{ set: string; ids: readonly string[] }> = [];
	key(set: string, id: string) {
		return { set, key: id };
	}
	async get<T>(set: string, id: string) {
		const row = this.data.get(`${set}/${id}`);
		return row
			? { value: row.value as T, generation: row.generation, key: this.key(set, id) }
			: null;
	}
	async create<T>(set: string, id: string, value: T) {
		const key = `${set}/${id}`;
		if (this.data.has(key)) return false;
		this.data.set(key, { value, generation: 1 });
		return true;
	}
	async replace<T>(row: { key: { set: string; key: string }; generation: number }, value: T) {
		const key = `${row.key.set}/${row.key.key}`;
		const current = this.data.get(key);
		if (!current || current.generation !== row.generation) return false;
		this.data.set(key, { value, generation: current.generation + 1 });
		return true;
	}
	async getInTransaction<T>(set: string, id: string) {
		return this.get<T>(set, id);
	}
	async putInTransaction<T>(set: string, id: string, value: T, policy: { txn: Txn }) {
		policy.txn.pending.set(`${set}/${id}`, { set, id, value });
	}
	async listByBin<T>(set: string, bin: string, value: string | number) {
		this.listByBinCalls++;
		const property: Record<string, string> = { path: 'path' };
		const field = property[bin] ?? bin;
		return (await this.list<T>(set)).filter(
			(row) => row.value && (row.value as Record<string, unknown>)[field] === value,
		);
	}
	async getMany<T>(set: string, ids: readonly string[]) {
		this.getManyCalls.push({ set, ids });
		return Promise.all(ids.map((id) => this.get<T>(set, id)));
	}
	async list<T>(set: string) {
		return [...this.data]
			.filter(([key]) => key.startsWith(`${set}/`))
			.map(([key, row]) => ({
				id: key.slice(set.length + 1),
				value: row.value as T,
				generation: row.generation,
			}));
	}
	transaction(_reads: number, _writes: number) {
		const txn: Txn = {
			pending: new Map(),
			commit: async () => {
				this.writes += txn.pending.size;
				if (this.failNextCommit) {
					this.failNextCommit = false;
					throw new Error('commit failed');
				}
				for (const [key, row] of txn.pending) {
					const old = this.data.get(key);
					this.data.set(key, { value: row.value, generation: (old?.generation ?? 0) + 1 });
				}
			},
			abort: async () => {
				txn.pending.clear();
			},
			isInDoubt: () => false,
		};
		return {
			policy: { txn },
			commit: txn.commit,
			abort: txn.abort,
			isInDoubt: txn.isInDoubt,
		};
	}
}
type Txn = {
	pending: Map<string, { set: string; id: string; value: unknown }>;
	commit(): Promise<void>;
	abort(): Promise<void>;
	isInDoubt(): boolean;
};

describe('Aerospike conversation stream MRT append', () => {
	it('publishes a multi-record append at one offset and preserves UTF-8 chunk boundaries', async () => {
		const db = new StreamRows();
		const stream = new AerospikeStreams(db as unknown as AerospikeRecords);
		const path = 'agents/echo/one';
		await stream.createStream(path, { agentName: 'echo', instanceId: 'one' });
		const claim = await stream.acquireProducer(path, 'producer');
		const records = [
			{ id: 'one', text: '🧪'.repeat(80_000) },
			{ id: 'two', text: 'second' },
		];
		const written = await stream.append({
			path,
			...claim,
			producerSequence: 0,
			records: records as never,
		});
		const read = await stream.read(path);
		expect(read.batches).toEqual([{ offset: written.offset, records }]);
		expect(read.upToDate).toBe(true);
		expect(db.writes).toBeGreaterThan(3);
	});

	it('reads only the requested header page and batch-fetches its chunks', async () => {
		const db = new StreamRows();
		const stream = new AerospikeStreams(db as unknown as AerospikeRecords);
		const path = 'agents/echo/page';
		await stream.createStream(path, { agentName: 'echo', instanceId: 'page' });
		const claim = await stream.acquireProducer(path, 'producer');
		for (let producerSequence = 0; producerSequence < 3; producerSequence++) {
			await stream.append({
				path,
				...claim,
				producerSequence,
				records: [{ id: `record-${producerSequence}` }] as never,
			});
		}

		db.getManyCalls = [];
		const first = await stream.read(path, { offset: '-1', limit: 1 });
		expect(first.batches).toHaveLength(1);
		expect(first.batches[0]?.records).toEqual([{ id: 'record-0' }]);
		expect(first.upToDate).toBe(false);
		expect(db.listByBinCalls).toBe(0);
		expect(db.getManyCalls.map(({ set, ids }) => [set, ids.length])).toEqual([
			['stream_batches', 1],
			['stream_chunks', 1],
		]);

		const second = await stream.read(path, { offset: first.nextOffset, limit: 1 });
		expect(second.batches[0]?.records).toEqual([{ id: 'record-1' }]);
		expect(second.upToDate).toBe(false);
		const third = await stream.read(path, { offset: second.nextOffset, limit: 1 });
		expect(third.batches[0]?.records).toEqual([{ id: 'record-2' }]);
		expect(third.upToDate).toBe(true);
	});

	it('aborts all batch chunks and header when the MRT commit fails', async () => {
		const db = new StreamRows();
		const stream = new AerospikeStreams(db as unknown as AerospikeRecords);
		const path = 'agents/echo/abort';
		await stream.createStream(path, { agentName: 'echo', instanceId: 'abort' });
		const claim = await stream.acquireProducer(path, 'producer');
		db.failNextCommit = true;
		await expect(
			stream.append({
				path,
				...claim,
				producerSequence: 0,
				records: [
					{ id: 'first', body: 'one' },
					{ id: 'second', body: 'two' },
				] as never,
			}),
		).rejects.toThrow('commit failed');
		expect((await stream.getMeta(path))?.nextOffset).toBe('-1');
		expect((await stream.read(path)).batches).toEqual([]);
		expect(
			await stream.append({
				path,
				...claim,
				producerSequence: 0,
				records: [
					{ id: 'first', body: 'one' },
					{ id: 'second', body: 'two' },
				] as never,
			}),
		).toMatchObject({ offset: expect.stringContaining('_') });
		expect((await stream.read(path)).batches).toHaveLength(1);
	});
});
