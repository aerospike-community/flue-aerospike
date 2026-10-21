import { randomBytes } from 'node:crypto';
import { createRequire } from 'node:module';
import { afterAll, describe, it } from 'vitest';
import {
	defineAttachmentStoreContractTests,
	defineConversationStreamStoreContractTests,
	defineStoreContractTests,
} from '@flue/runtime/test-utils';
import { aerospike } from './index.ts';
import type { AerospikeApi, AerospikeClient, AerospikeRunner } from './storage.ts';

// Real-server parity suite. Keep it opt-in so a consumer's normal unit test
// command does not require an Aerospike daemon; CI/dev runs enable it against
// the local SC server with FLUE_AEROSPIKE_TEST=1.
const enabled = process.env.FLUE_AEROSPIKE_TEST === '1';
const activePrefix = `ft${randomBytes(5).toString('hex')}_`;
const namespace = process.env.AEROSPIKE_NAMESPACE ?? 'test';
const require = createRequire(import.meta.url);
type TestSdk = AerospikeApi & {
	connect(
		config: object,
	): Promise<
		AerospikeClient & { truncate(ns: string, set: string, beforeNanos: number): Promise<void> }
	>;
};
let sdk: TestSdk | undefined;
let sharedClient:
	| (AerospikeClient & {
			truncate(ns: string, set: string, beforeNanos: number): Promise<void>;
	  })
	| undefined;

async function testRunner(): Promise<AerospikeRunner> {
	sdk ??= require('aerospike') as TestSdk;
	if (!sharedClient) {
		sharedClient = await sdk.connect({
			hosts: process.env.AEROSPIKE_HOSTS ?? '127.0.0.1:3200',
		});
	}
	const client = new Proxy(sharedClient, {
		get(target, property) {
			if (property === 'close') return async () => {};
			const value = Reflect.get(target, property, target) as unknown;
			return typeof value === 'function' ? value.bind(target) : value;
		},
	});
	return { client, api: sdk };
}

function backend() {
	let adapters: ReturnType<typeof aerospike>[] = [];
	async function open() {
		const adapter = aerospike(await testRunner(), {
			namespace,
			setPrefix: activePrefix,
		});
		await adapter.migrate();
		adapters.push(adapter);
		return adapter.connect();
	}
	return {
		open,
		async cleanup() {
			await Promise.all(adapters.map((a) => a.close()));
			adapters = [];
			if (sharedClient) {
				for (const set of [
					'meta',
					'submissions',
					'submission_chunks',
					'streams',
					'stream_batches',
					'stream_chunks',
					'attachments',
					'attachment_chunks',
				]) {
					await sharedClient.truncate(namespace, `${activePrefix}${set}`, 0);
				}
			}
		},
	};
}
if (enabled) {
	const submissions = backend();
	defineStoreContractTests('Aerospike AgentSubmissionStore', {
		create: async () => (await submissions.open()).submissionStore,
		cleanup: submissions.cleanup,
	});
	const streams = backend();
	defineConversationStreamStoreContractTests('Aerospike ConversationStreamStore', {
		create: async () => {
			const stores = await streams.open();
			return { stream: stores.conversationStreamStore, submissionStore: stores.submissionStore };
		},
		cleanup: streams.cleanup,
	});
	const attachments = backend();
	defineAttachmentStoreContractTests('Aerospike AttachmentStore', {
		create: async () => (await attachments.open()).attachmentStore,
		cleanup: attachments.cleanup,
	});
	afterAll(async () => {
		if (sharedClient) await sharedClient.close();
	});
} else {
	describe.skip('Aerospike store contracts (set FLUE_AEROSPIKE_TEST=1)', () => {
		it('requires a local Aerospike server', () => {});
	});
}
