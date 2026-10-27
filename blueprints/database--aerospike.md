---
{ "kind": "database", "version": 1, "website": "https://aerospike.com" }
---

# Add Aerospike Persistence to Flue

You are an AI coding agent configuring the first-party `@flue/aerospike`
adapter for a Node-target Flue project. The adapter persists accepted
submissions, canonical conversation streams, and immutable attachment payloads.
It does not store application business data.

## Check the target first

`db.ts` is a Node-target concern. Cloudflare deployments use Durable Object
SQLite and reject a custom `db.ts`. If this project targets Cloudflare, stop
and tell the user.

## Install the adapter and client

Install `@flue/aerospike` and the official `aerospike` Node.js client. Reuse
the application's existing compatible client version when present. The adapter
does not bundle or create the client: the application owns its cluster
configuration, credentials, client policies, connection lifecycle, and one
client per process.

## Configure `db.ts`

Use the project's existing secret convention for the host address. Do not
hardcode credentials or read environment variables inside an adapter wrapper.
Connect once, then pass both the client and its matching SDK module:

```ts title="src/db.ts"
import Aerospike from 'aerospike';
import { aerospike } from '@flue/aerospike';

const client = await Aerospike.connect({
  hosts: process.env.AEROSPIKE_HOSTS,
});

export default aerospike(
  { client, api: Aerospike },
  { namespace: process.env.AEROSPIKE_NAMESPACE ?? 'flue' },
);
```

The selected namespace must be configured for strong consistency. The adapter
uses multi-record transactions (MRT) to publish each bounded conversation
append atomically.

## Edition compatibility gate

MRT requires strong consistency and is currently an Enterprise-only server
capability. The released `aerospike/aerospike-server` Community Edition image
does not provide the required capability, so it is not a working target for
this adapter today. Do not claim Community Edition support unless Aerospike
confirms MRT/strong-consistency availability there. For production, select a
licensed Enterprise deployment and verify its current released container
reference with Aerospike before provisioning; this blueprint intentionally
does not guess an Enterprise image tag.

## Migrations and ownership

Flue calls `migrate()` before connecting the stores. Migration creates the
adapter-prefixed secondary indexes idempotently, checks the format version,
and records the schema marker only after index setup. Keep the default `flue_`
set prefix unless multiple independent Flue databases share a namespace.

The app owns the client and passes it in the runner. Flue calls the adapter's
`close()` during shutdown; do not create another Aerospike client per request.
All adapter data remains in the configured namespace and prefixed sets.

## Verify

1. Confirm the target is Node.js and the namespace is strong-consistency.
2. Typecheck and build the application.
3. Start against a non-production Enterprise namespace and confirm
   `migrate()` creates the indexes.
4. Run the adapter's real-server contract suite against a disposable SC
   namespace; do not point tests at production data.
5. Restart and verify accepted submissions and conversation history remain
   available.

Do not use vector search or Aerospike Graph APIs for this adapter; Flue's
persistence access patterns are key/value and bounded stream operations.

## Upgrade Guide

### Version 1

Initial version. Community Edition compatibility remains an explicit release
gate while MRT availability is unresolved.
