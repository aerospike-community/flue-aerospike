import type {
	AttachmentStore,
	GetAttachmentInput,
	PutAttachmentInput,
	StoredAttachment,
} from '@flue/runtime/adapter';
import {
	AttachmentConflictError,
	copyAttachmentBytes,
	sameAttachmentRef,
	verifyAttachmentBytes,
} from '@flue/runtime/adapter';
import { AerospikeRecords, compositeId } from '../storage.ts';

const CHUNK_SIZE = 256 * 1024;
type Header = {
	streamPath: string;
	conversationId: string;
	attachment: PutAttachmentInput['attachment'];
	count: number;
};
export class AerospikeAttachments implements AttachmentStore {
	constructor(private readonly db: AerospikeRecords) {}
	async put(input: PutAttachmentInput): Promise<void> {
		await verifyAttachmentBytes(input.attachment, input.bytes);
		const key = compositeId(input.streamPath, input.attachment.id);
		const generation = input.attachment.digest;
		const chunks = Math.ceil(input.bytes.byteLength / CHUNK_SIZE);
		const chunkKeys = Array.from({ length: chunks }, (_, i) =>
			compositeId(input.streamPath, input.attachment.id, generation, i),
		);
		const header: Header = {
			streamPath: input.streamPath,
			conversationId: input.conversationId,
			attachment: input.attachment,
			count: chunks,
		};
		const existing = await this.db.get<Header>('attachments', key);
		if (existing) {
			if (
				!sameAttachmentRef(existing.value.attachment, input.attachment) ||
				existing.value.streamPath !== input.streamPath ||
				existing.value.conversationId !== input.conversationId ||
				existing.value.count !== chunks
			)
				throw new AttachmentConflictError({
					path: input.streamPath,
					attachmentId: input.attachment.id,
				});
			const oldChunks = await this.db.getMany<{ base64: string }>('attachment_chunks', chunkKeys);
			for (let i = 0; i < chunks; i++) {
				const old = oldChunks[i];
				if (
					!old ||
					!equal(
						base64ToBytes(old.value.base64),
						input.bytes.subarray(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE),
					)
				)
					throw new AttachmentConflictError({
						path: input.streamPath,
						attachmentId: input.attachment.id,
					});
			}
			return;
		}
		// Digest-scoped immutable rows keep concurrent conflicting puts from
		// overwriting the winner's bytes. Unreferenced generations are harmless.
		const oldChunks = await this.db.getMany<{ base64: string }>('attachment_chunks', chunkKeys);
		const missing: Array<{ id: string; value: { base64: string } }> = [];
		for (let i = 0; i < chunks; i++) {
			const value = copyAttachmentBytes(input.bytes.subarray(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE));
			const old = oldChunks[i];
			if (old && !equal(base64ToBytes(old.value.base64), value))
				throw new AttachmentConflictError({
					path: input.streamPath,
					attachmentId: input.attachment.id,
				});
			if (!old) missing.push({ id: chunkKeys[i]!, value: { base64: bytesToBase64(value) } });
		}
		const statuses = await this.db.createMany('attachment_chunks', missing);
		if (statuses.some((status) => status !== 0 && status !== this.db.api.status.ERR_RECORD_EXISTS))
			throw new Error('@flue/aerospike batch attachment chunk write failed.');
		if (statuses.includes(this.db.api.status.ERR_RECORD_EXISTS)) {
			const winners = await this.db.getMany<{ base64: string }>(
				'attachment_chunks',
				missing.map((row) => row.id),
			);
			for (let i = 0; i < winners.length; i++) {
				const winner = winners[i];
				const expected = base64ToBytes(missing[i]!.value.base64);
				if (!winner || !equal(base64ToBytes(winner.value.base64), expected))
					throw new AttachmentConflictError({
						path: input.streamPath,
						attachmentId: input.attachment.id,
					});
			}
		}
		if (!(await this.db.create('attachments', key, header))) {
			const winner = await this.db.get<Header>('attachments', key);
			if (
				!winner ||
				!sameAttachmentRef(winner.value.attachment, input.attachment) ||
				winner.value.conversationId !== input.conversationId
			)
				throw new AttachmentConflictError({
					path: input.streamPath,
					attachmentId: input.attachment.id,
				});
		}
	}
	async get(input: GetAttachmentInput): Promise<StoredAttachment | null> {
		const key = compositeId(input.streamPath, input.attachmentId);
		const row = await this.db.get<Header>('attachments', key);
		if (
			!row ||
			row.value.conversationId !== input.conversationId ||
			row.value.streamPath !== input.streamPath
		)
			return null;
		const chunkKeys = Array.from({ length: row.value.count }, (_, i) =>
			compositeId(input.streamPath, input.attachmentId, row.value.attachment.digest, i),
		);
		const chunks = await this.db.getMany<{ base64: string }>('attachment_chunks', chunkKeys);
		const parts: Uint8Array[] = [];
		let length = 0;
		for (let i = 0; i < row.value.count; i++) {
			const chunk = chunks[i];
			if (!chunk)
				throw new Error(`@flue/aerospike attachment ${input.attachmentId} has incomplete content.`);
			const bytes = base64ToBytes(chunk.value.base64);
			parts.push(bytes);
			length += bytes.byteLength;
		}
		const bytes = new Uint8Array(length);
		let offset = 0;
		for (const part of parts) {
			bytes.set(part, offset);
			offset += part.byteLength;
		}
		await verifyAttachmentBytes(row.value.attachment, bytes);
		return { attachment: { ...row.value.attachment }, bytes };
	}
}
function equal(a: Uint8Array, b: Uint8Array) {
	return a.length === b.length && a.every((x, i) => x === b[i]);
}
function bytesToBase64(bytes: Uint8Array) {
	let binary = '';
	for (let i = 0; i < bytes.length; i += 0x8000)
		binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
	return btoa(binary);
}
function base64ToBytes(value: string) {
	const binary = atob(value);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
	return bytes;
}
