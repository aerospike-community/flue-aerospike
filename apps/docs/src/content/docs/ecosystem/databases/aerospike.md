---
title: Aerospike
description: Persist Flue submissions, conversations, and attachments in Aerospike.
package:
  name: '@flue/aerospike'
  href: https://www.npmjs.com/package/@flue/aerospike
lastReviewedAt: 2026-10-21
---

## Quickstart

Add Aerospike persistence to an existing Node-target Flue project with the
[Aerospike](https://aerospike.com) blueprint:

```sh
flue add database aerospike
```

The blueprint installs `@flue/aerospike`, uses the application's official
`aerospike` Node.js client, and creates a source-root `db.ts`.

## Edition compatibility gate

This adapter publishes conversation appends with Aerospike multi-record
transactions (MRT), which require a strong-consistency namespace. Strong
consistency and MRT are currently Enterprise-only. The released
`aerospike/aerospike-server` Community Edition image does not meet this
requirement, so it is not a supported deployment target for this adapter
today. Do not use it for Flue persistence unless Aerospike confirms that MRT
and strong consistency are available in that edition.

For production, use a licensed Enterprise deployment and verify its current
released container reference and namespace configuration directly with
Aerospike. The adapter does not select a server edition or container tag.
Community Edition compatibility remains a release gate pending confirmation
of the server capability.

## Configure

The app constructs and owns one Aerospike client, then injects both the client
and its SDK module. This keeps connection, TLS, credentials, and policy
decisions in the application's configuration layer:

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

Use the application's established secret convention instead of committing
credentials. The namespace must be configured for strong consistency. Keep the
default `flue_` set prefix unless several independent Flue databases share the
namespace.

## What gets stored

The adapter persists Flue runtime state only:

- accepted direct prompts and dispatched submissions;
- durable attempt, lease, abort, join, and settlement state;
- canonical conversation batches and their bounded UTF-8 chunks;
- immutable attachment manifests and content chunks.

Application business data, model credentials, and external tool side effects
remain application-owned. The adapter uses primary-key reads for exact
lookups, secondary-index queries for status/session/stream access paths, batch
operations for attachment chunks, and an MRT for each stream append. The
12 MiB Flue append bound requires at most 146 transaction writes with the
current chunk size.

## Migrations and lifecycle

Flue calls `migrate()` before connecting the stores. Migration creates
adapter-prefixed indexes idempotently and verifies the persisted format
version. The adapter closes the injected client when Flue shuts down. Use one
client per process; do not construct one per request.

## Deployment target

This package is for Node.js deployments. Cloudflare deployments use Flue's
built-in Durable Object SQLite persistence and reject a custom `db.ts`.

The adapter does not use Aerospike Vector or Graph features.
