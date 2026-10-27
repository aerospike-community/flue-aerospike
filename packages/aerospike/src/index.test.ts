import { describe, expect, it, vi } from 'vitest';
import { aerospike } from './index.ts';
import type { AerospikeApi, AerospikeRunner } from './storage.ts';

function runner(isConnected = true) {
	const client = {
		isConnected: vi.fn(() => isConnected),
		close: vi.fn(),
		get: vi.fn(async () => {
			throw Object.assign(new Error('missing'), { code: 2 });
		}),
		put: vi.fn(async () => undefined),
		remove: vi.fn(async () => undefined),
		query: vi.fn(() => ({
			foreach: () => ({
				on: (event: string, callback: () => void) => {
					if (event === 'end') callback();
				},
			}),
		})),
		commit: vi.fn(),
		abort: vi.fn(),
		createIndex: vi.fn(async () => ({ waitUntilDone: async () => undefined })),
		infoAll: vi.fn(async () => [{ info: 'strong-consistency=true' }]),
	};
	const api = {
		Key: class {
			constructor(
				readonly ns: string,
				readonly set: string,
				readonly key: string,
			) {}
		},
		Transaction: class {
			getInDoubt() {
				return false;
			}
		},
		indexDataType: { STRING: 0, INTEGER: 1 },
		filter: { equal: vi.fn(() => ({})) },
		status: { ERR_INDEX_FOUND: 200 },
	};
	return { client, api } as unknown as AerospikeRunner & {
		close: ReturnType<typeof vi.fn>;
		isConnected: ReturnType<typeof vi.fn>;
		api: AerospikeApi;
	};
}

describe('Aerospike adapter lifecycle', () => {
	it('validates configuration without touching the client', () => {
		const db = runner();
		expect(() => aerospike(db, { namespace: '' })).toThrow(/namespace/);
		expect(() => aerospike(db, { namespace: 'test', setPrefix: 'a'.repeat(50) })).toThrow(
			/setPrefix/,
		);
		expect(db.client.isConnected).not.toHaveBeenCalled();
	});
	it('requires a connected client and migrate before connect', async () => {
		const offline = aerospike(runner(false), { namespace: 'test' });
		await expect(offline.migrate()).rejects.toThrow(/connected client/);
		const db = runner();
		const adapter = aerospike(db, { namespace: 'test', setPrefix: 'unit_' });
		expect(() => adapter.connect()).toThrow(/migrate/);
		await adapter.migrate();
		expect(db.client.createIndex).toHaveBeenCalledTimes(7);
		expect(adapter.connect()).toHaveProperty('submissionStore');
		expect(() => adapter.connect()).toThrow(/once/);
		await adapter.close();
		await adapter.close();
		expect(db.client.close).toHaveBeenCalledTimes(1);
	});
	it('rejects a namespace that does not provide strong consistency', async () => {
		const db = runner();
		db.client.infoAll = vi.fn(async () => [{ info: 'strong-consistency=false' }]);
		await expect(aerospike(db, { namespace: 'test' }).migrate()).rejects.toThrow(
			/strong consistency/,
		);
	});
	it('does not construct, connect, or close a caller-owned client at factory time', () => {
		const db = runner();
		const adapter = aerospike(db, { namespace: 'test' });
		expect(db.client.isConnected).not.toHaveBeenCalled();
		expect(db.client.close).not.toHaveBeenCalled();
		expect(adapter).toHaveProperty('migrate');
	});
});
