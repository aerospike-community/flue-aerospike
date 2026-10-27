import { describe, expect, it, vi } from 'vitest';
import { AerospikeRecords } from './storage.ts';
import type { AerospikeApi, AerospikeClient, AerospikeRecord } from './storage.ts';

function record(id: string, value: unknown): AerospikeRecord {
	return {
		key: { ns: 'test', set: 'attachments', key: id },
		bins: { logical_id: id, data: JSON.stringify(value) },
		gen: 1,
	};
}

function records(results: Awaited<ReturnType<AerospikeClient['batchRead']>>) {
	const client = { batchRead: vi.fn(async () => results) } as unknown as AerospikeClient;
	const api = {
		Key: class {
			constructor(
				readonly ns: string,
				readonly set: string,
				readonly key: string,
			) {}
		},
	} as unknown as AerospikeApi;
	return new AerospikeRecords(client, api, 'test', '');
}

describe('Aerospike batch reads', () => {
	it('returns records and only maps record-not-found statuses to null', async () => {
		const db = records([
			{ status: 0, record: record('present', { value: 1 }) },
			{ status: 2 },
		]);

		await expect(db.getMany('attachments', ['present', 'missing'])).resolves.toMatchObject([
			{ id: 'present', value: { value: 1 } },
			null,
		]);
	});

	it('throws for per-key operational failures', async () => {
		const db = records([{ status: 9 }]);

		await expect(db.getMany('attachments', ['failed'])).rejects.toThrow(
			'batch read failed for attachments/failed with status 9',
		);
	});

	it('throws when a successful result has no record', async () => {
		const db = records([{ status: 0 }]);

		await expect(db.getMany('attachments', ['missing-record'])).rejects.toThrow(
			'batch read succeeded without a record for attachments/missing-record',
		);
	});
});
