# Aerospike persistence model

This is the schema guide and derived schema summary for the Flue adapter. The
implementation and its real-server contract suite live beside this document.

## Contract inputs and assumptions

- Flue exposes no global submission-volume ceiling. The submission ledger and
  canonical stream therefore have no configured lifetime TTL or arbitrary
  global row cap.
- Stream offsets and producer sequences are non-negative safe integers in the
  runtime representation. Aerospike stores them as integer bins; increment or
  conversion must reject values outside JavaScript's safe-integer range.
- The runtime bounds serialized stream data by JavaScript string length
  (`12 * 1024 * 1024` UTF-16 code units), not UTF-8 bytes. The adapter must
  enforce that same limit before any durable write, then encode the string as
  UTF-8 for storage. UTF-8 output can be as large as three bytes per UTF-16
  code unit (for example, BMP characters), so use a conservative 36 MiB
  encoded-byte budget. With 256 KiB byte chunks, the bound is 144 chunks,
  plus the batch header and stream metadata record: at most 146 transaction
  writes. This remains well below the server's 4096-record transaction limit.
- Attachment size is not capped by the Flue contract. Attachments are split
  into 256 KiB chunks, batch-written in bounded groups, and made visible only
  by a completed immutable manifest. An interrupted write can leave
  unreachable digest-scoped chunks; a future maintenance process may reclaim
  them, but reads never return partial content.
- Strong consistency is required for the namespace because stream publication
  uses multi-record transactions. Release compatibility remains gated on
  confirmation of MRT availability in the chosen released Aerospike edition.

## Schema guide

All records live in the caller-selected namespace and in adapter-prefixed
sets. The adapter format version and admission sequence live in the `meta` set.
The adapter rejects an unknown or newer format before interpreting payloads.
Migrations create required indexes before stamping the new version, so an
interrupted index setup cannot make the schema appear ready.

The physical primary key is `SHA-256(JSON.stringify([prefixedSet,
logicalId]))`. Composite logical IDs are JSON arrays, never delimiter-joined
strings. Every record also stores its unhashed logical identity in `logical_id`;
a digest-key hit whose stored identity differs fails closed.

| Set                 | Logical record                                         | Primary-key tuple                                           | Important bins                                                           |
| ------------------- | ------------------------------------------------------ | ----------------------------------------------------------- | ------------------------------------------------------------------------ |
| `meta`              | Format stamp, admission sequence, fold checkpoints     | `"format"`, `"submission_sequence"`, `"checkpoint:" + path` | `data` JSON; one record per control item                                 |
| `submissions`       | One durable submission row                             | `submissionId`                                              | `data`, `status`, `session_key`, `sequence`, `joined_into`, `logical_id` |
| `submission_chunks` | One persisted attachment chunk in a submission payload | `[submissionId, scope, itemId, index]`                      | `data`, `submission_id`, `logical_id`                                    |
| `streams`           | One canonical stream and its producer state            | `path`                                                      | `data`, `path`, `logical_id`                                             |
| `stream_batches`    | One immutable batch header per offset                  | `[path, offset]`                                            | `data`, `path`, `prod_seq`, `logical_id`                                 |
| `stream_chunks`     | One JSON-encoded UTF-8 fragment of a batch             | `[path, offset, index]`                                     | `data`, `logical_id`                                                     |
| `attachments`       | Immutable attachment manifest                          | `[streamPath, attachmentId]`                                | `data`, `logical_id`                                                     |
| `attachment_chunks` | Immutable attachment content chunk                     | `[streamPath, attachmentId, digest, index]`                 | `data`, `logical_id`                                                     |

The key encoder must validate input types and include all components in the
stored identity bins. UTF-8 strings are encoded exactly; identifiers are not
normalized or case-folded.

### Access paths, relationships, and indexes

- Submission lookup and mutation use the deterministic submission primary
  key. Session ordering is by the durable global `sequence`; runnable-head
  selection scans/subselects queued and blocking statuses and orders by that
  sequence. Claim writes are conditional on the observed generation and
  expected status. A stale generation or changed status loses without mutation.
- Recovery/status operations use the status index; session ordering and abort
  enumeration use the session-key index; joined-row recovery uses the
  `joined_into` index. Stream batch reads and exact retries use the path index.
  Sequence ordering is evaluated after the bounded status/session query.
  Submission attachment hydration uses the `submission_id` index.
- A stream metadata record owns the identity and producer epoch. Batch records
  are ordered by offset and are immutable. A batch header is the visibility
  boundary: readers return a batch only if the header exists and all chunks
  validate.
- An attachment manifest owns its conversation association and content
  identity. Reads require both the stream path and conversation id; chunks
  alone are never discoverable.
- Idempotent secondary indexes are created on submission status, session key,
  joined host, submission-chunk owner, stream path, and stream producer
  sequence. Index names are deterministic hashes of the set/bin contract.
  Exact submission/stream/attachment lookups remain primary-key reads.

## Atomicity, retries, and recovery

### Submission ledger

Submission admission is idempotent by submission id and compares the full
persisted payload on replay. Sequence assignment and row creation must be
atomic with respect to the sequence allocator; a missing/duplicate sequence
must not reorder admitted work. Per-row state transitions use generation
conditional writes and compare every relevant gate (status, attempt id, owner,
join host) in the same update.

Session abort is a per-row idempotent operation: enumerate unsettled session
rows, and set `abortRequestedAt` with first-writer-wins conditional updates.
Retries converge on the same stamp and never change terminal status. This is
not a cross-row atomic operation: a failure may leave some rows stamped and
others unstamped until the caller retries. No store-level claim of atomic
all-session stamping is made.

`claimJoinableSubmissions` observes the host and queued session prefix in
sequence order. It uses a small MRT per delivery to read the current host,
sequence allocator, and candidate, then conditionally changes one row from
`queued` to `joining`. It stops at the first non-joinable or concurrently
changed row. Once a row is `joining`, another claimant stops instead of
skipping it, so a competing host cannot claim a later row out of order. A
partial claim is recoverable: `listJoinedSubmissions` returns `joining`
stragglers, and runtime recovery either confirms their canonical input or
reverts an unapplied join to `queued`.

Joined submission settlement normally uses the runtime's durable
per-submission settlement outbox before the host is settled. Thus a crash
partway through outbox publication leaves the host running and recovery can
resume the remaining joined rows. The store-level fan-out is a per-row
idempotent backstop, not one globally atomic operation. Before changing a
joined row, persist the host outcome/error intent with a bounded transaction
that also reserves that child. The host cannot adopt a conflicting outcome
while the intent is pending; attempt replacement preserves the intent. The
backstop settles joined rows and reverts joining stragglers one at a time, and
only settles the host after all children advance. The shared contract suite and
the interrupted-progress unit test cover the first-outcome retry behavior.
`terminalizing` reservations and canonical settlement-record ordering remain
unchanged.

### Canonical stream

Stream identity creation and producer acquisition are conditional on the
stream metadata generation. Reacquiring a producer advances its epoch and
incarnation as prescribed by the runtime, fencing stale writers.

Append serializes the entire batch and checks the runtime's UTF-16 code-unit
limit before encoding/chunking it, all before beginning the transaction. One
MRT must include the stream metadata update, batch header, and every chunk. It
validates identity, incarnation, producer id/epoch, producer sequence,
expected contiguous offset, exact-retry content, and submission authorization
before commit. An exact retry returns the prior offset only when the stored
producer sequence and complete payload match; conflicting sequence reuse
fails. Readers expose only committed batch headers and validate all chunk
indexes/counts before decoding. A failed/aborted MRT leaves no visible subset.
Subscription notifications are local wake-up hints; durable reads remain
authoritative.

### Attachments

Writes verify declared size and SHA-256 digest before storage. Chunks are
written under content-addressed keys and the immutable manifest is created
last. Exact retries compare conversation, attachment identity, digest, size,
and bytes; reuse with different content or conversation raises
`AttachmentConflictError`. Reads verify chunk order, final byte length, and
digest and return copied bytes and metadata. Unreferenced chunks are never
returned.

## Record sizing, growth, and hot keys

- A stream batch uses 256 KiB UTF-8 byte chunks. The runtime's 12 Mi UTF-16
  code-unit bound permits up to 36 MiB of encoded bytes (144 chunks); the batch
  header and stream metadata are included in the 146-record MRT write budget.
- Attachment chunks are 256 KiB before base64 encoding and are batch-written
  in groups of at most 1000 records. The manifest stores the chunk count.
  There is no contract-level total attachment ceiling; practical maximums are
  constrained by available memory, batch count, and application ingress
  limits. Orphan content chunks are not visible without a manifest.
- Submission payloads remain one row per submission; exceptionally large
  payloads must use Flue's persisted attachment placement rather than an
  unbounded record bin.
- Stream history and submission rows are durable and have no TTL. Immutable
  attachment chunks/manifests likewise have no TTL while referenced.
- The global admission sequence allocator and each stream's metadata record
  are serialized coordination points. The admission allocator is intentionally
  shared to preserve the runtime's global ordering contract; stream metadata
  is a per-conversation hot key for producer/offset serialization. Load and
  contention measurements are required before any sharding proposal, which
  must preserve ordering and transaction semantics.

## Rejected alternatives and reopen criteria

- Cross-record record UDFs are not a substitute for transactions: record UDFs
  are scoped to one record and stream UDFs are read-only.
- A global submission cap is not introduced because Flue's contract has none.
- A single ever-growing stream record is rejected because conversation history
  is unbounded and would exceed the safe record size.
- A single record per stream batch is rejected because payload size can exceed
  an individual record limit; bounded chunk records are published atomically.
- This design must be reopened if MRT support is unavailable in a target
  release edition, if client feature parity differs, if the supported maximum
  record size/chunk policy changes, or if shared contracts require a stronger
  externally observable atomicity guarantee than the conditional per-row
  recovery paths provide.
