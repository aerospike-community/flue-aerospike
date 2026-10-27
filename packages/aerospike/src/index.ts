import type {
	AgentSubmissionStore,
	AttachmentStore,
	ConversationStreamStore,
	PersistenceAdapter,
	PersistenceStores,
} from '@flue/runtime/adapter';
import { FLUE_FORMAT_VERSION } from '@flue/runtime/adapter';
import { AerospikeAttachments } from './stores/attachments.ts';
import { AerospikeStreams } from './stores/streams.ts';
import { AerospikeSubmissions } from './stores/submissions.ts';
import { AerospikeRecords, type AerospikeOptions, type AerospikeRunner } from './storage.ts';

export type {
	AerospikeApi,
	AerospikeClient,
	AerospikeOptions,
	AerospikeRunner,
} from './storage.ts';

export interface AerospikeAdapter extends PersistenceAdapter {
	migrate(): Promise<void>;
	connect(): PersistenceStores;
	close(): Promise<void>;
}

/** Configure Flue with an application-owned client and the matching Node SDK module. */
export function aerospike(runner: AerospikeRunner, options: AerospikeOptions): AerospikeAdapter {
	const { client, api } = runner;
	if (
		!options ||
		typeof options.namespace !== 'string' ||
		!/^[A-Za-z0-9_]{1,32}$/.test(options.namespace)
	)
		throw new TypeError('@flue/aerospike requires a 1-32 character Aerospike namespace.');
	const prefix = options.setPrefix ?? 'flue_';
	if (!/^[A-Za-z0-9_]{1,40}$/.test(prefix))
		throw new TypeError(
			'@flue/aerospike setPrefix must be 1-40 alphanumeric/underscore characters.',
		);
	const records = new AerospikeRecords(client, api, options.namespace, prefix);
	let migrated = false;
	let connected = false;
	let closed = false;
	const assertConnected = () => {
		if (closed) throw new TypeError('@flue/aerospike adapter is closed.');
		if (!client.isConnected()) throw new TypeError('@flue/aerospike requires a connected client.');
	};
	return {
		async migrate() {
			assertConnected();
			await records.migrate(FLUE_FORMAT_VERSION);
			migrated = true;
		},
		connect() {
			assertConnected();
			if (!migrated) throw new TypeError('@flue/aerospike connect() requires migrate() first.');
			if (connected) throw new TypeError('@flue/aerospike connect() may only be called once.');
			connected = true;
			const submissionStore = new AerospikeSubmissions(records);
			return {
				submissionStore: submissionStore as AgentSubmissionStore,
				conversationStreamStore: new AerospikeStreams(records) as ConversationStreamStore,
				attachmentStore: new AerospikeAttachments(records) as AttachmentStore,
			};
		},
		async close() {
			if (closed) return;
			closed = true;
			await client.close();
		},
	};
}
