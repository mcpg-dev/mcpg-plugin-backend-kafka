# Kafka Binding — `dev.mcpg.backend.kafka`

> class `backend` · `native` · package `mcpg-plugin-backend-kafka` · artifact `libmcpg_plugin_backend_kafka.so` · BUSL-1.1

Backend binding plugin for the MCPG gateway that dispatches calls as a
Kafka request/reply round-trip: the call payload is produced to a
request topic with a unique `correlation_id` header, and the first reply
on the response topic carrying that same correlation id is returned to
the caller. The same artifact also ships a watch strategy that turns any
message on a Kafka topic into a resource-change notification. Reach for
it when the system behind your MCP surface is a Kafka consumer group
rather than a synchronous endpoint. It declares `network_outbound` as a
required capability, so the matching `plugins[]` entry has to grant it or
the gateway refuses the plugin at boot, and it carries the
`rdkafka`/librdkafka dependency so the gateway binary does not.

## What it does
- Dispatches `kind: kafka` calls as a produce-then-consume round trip
  correlated by a UUID `correlation_id` record header; non-matching
  messages on the response topic are skipped, not consumed as replies.
- Returns the reply payload verbatim, capped at `max_response_bytes`,
  with the gateway's truncation flag set when the cap bites.
- Fails the call with a timeout once `timeout_ms` elapses without a
  correlated reply.
- Forwards the gateway's request headers (including W3C trace context)
  as record headers, and adds `idempotency-key` and
  `idempotency-scope-hash` when the gateway supplies an idempotency
  hint. This is application-level idempotency; it does not touch
  librdkafka's own `enable.idempotence`.
- Supports per-caller SASL: a `${cred://issuer/target}` token in
  `bootstrap_servers`, `sasl_username`, or `sasl_password` resolves per
  caller identity and selects a cached producer/consumer pair keyed on
  the resolved bundle.
- Ships a second entity — a `watch_strategy` for `kind: kafka_topic`,
  identifying itself as `dev.mcpg.watch.kafka_topic` — that consumes a
  topic and emits a watch event per message so subscribers receive
  `notifications/resources/updated`.
- Supports the SASL mechanisms that ride the SSL/crypto library: PLAIN,
  SCRAM-SHA-256, SCRAM-SHA-512, and OAUTHBEARER. GSSAPI/Kerberos is out
  of scope — no Cyrus SASL dependency is linked.

## Configuration
Two levels. The `plugins:` entry loads the cdylib and carries the
connection-level `config:` block shared by every Kafka binding in the
gateway; the per-call topics and limits live in each binding's
`backend:` block, keyed by the `kind: kafka` discriminator.

```yaml
plugins:
  - id: dev.mcpg.backend.kafka
    class: backend
    kind: native
    source:
      path: ./plugins/libmcpg_plugin_backend_kafka.so
    granted_capabilities:
      - network_outbound
    config:
      bootstrap_servers: "kafka-1:9092,kafka-2:9092"
      group_id: mcpg

mcp:
  capabilities:
    tools:
      - name: events.enrich
        description: Enrich an event through the enrichment worker.
        backend:
          kind: kafka
          request_topic: enrich.requests
          response_topic: enrich.responses
          timeout_ms: 10000
          max_response_bytes: 65536
```

Plugin-level `config:`:

| Field | Type | Default | Description |
|---|---|---|---|
| `bootstrap_servers` | string | `""` | Broker list shared by every Kafka binding in this gateway. |
| `group_id` | string | `mcpg` | Consumer group used for the reply consumers. Not a per-binding field. |

Unknown fields are rejected, and a malformed `config:` block fails the
plugin closed rather than silently falling back to defaults.

Per-binding `backend:` spec:

| Field | Type | Default | Description |
|---|---|---|---|
| `request_topic` | string | — (required) | Topic requests are produced to. Non-empty, no NUL bytes, at most 249 characters. |
| `response_topic` | string | — (required) | Topic replies are consumed from. Same validation as `request_topic`. |
| `timeout_ms` | u64 | `10000` | Budget for the whole round trip. Must be greater than 0. |
| `max_response_bytes` | usize | `65536` | Reply payload cap; oversized replies are truncated. Must be greater than 0. |
| `bootstrap_servers` | string | plugin-level value | Optional override. A plaintext value that differs from a non-empty plugin-level broker list is rejected; a value carrying a `${cred://…}` token is allowed and resolved per caller. |
| `sasl_username` | string | unset | SASL username. May carry `${cred://issuer/target}`. |
| `sasl_password` | string | unset | SASL password. May carry `${cred://issuer/target}`. |
| `security_protocol` | string | unset | librdkafka `security.protocol`, e.g. `SASL_SSL` or `SASL_PLAINTEXT`. |
| `sasl_mechanism` | string | unset | librdkafka `sasl.mechanism`, e.g. `SCRAM-SHA-256` or `PLAIN`. |

## Security
`request_topic` and `response_topic` are transport-only routing facts, so
registration rejects any `cred://` reference in them — a resolved secret
must never land in a topic name. The connection fields are the opposite:
`bootstrap_servers`, `sasl_username`, and `sasl_password` legitimately
carry credentials, and only the `${cred://issuer/target}` token form
resolves. A bare `cred://…` outside `${}` is treated as a literal
password string and is never sent to the credential issuer.

Kafka bindings have no request-argument templating, so the strings a
credential token can appear in are config-origin by construction — a
caller cannot introduce a credential reference through a request
argument. Because all bindings share one consumer group, a per-binding
`bootstrap_servers` override that merely disagrees with the plugin-level
brokers is rejected at boot rather than silently opening a second
connection.

## Change-watching
The bundled watch strategy is selected on a resource binding's `watch:`
block and is independent of which backend serves the resource itself:

```yaml
mcp:
  capabilities:
    resources:
      - name: config.app_settings
        description: Application configuration.
        uri: "config://app/settings"
        mime_type: application/json
        backend:
          kind: http
          url: "https://config.internal/app"
          method: get
        watch:
          strategy:
            type: kafka_topic
            topic: config-change-events
            group_id: mcpg-resource-watcher
```

`topic` is required; `group_id` defaults to `mcpg-resource-watcher`.
Unknown fields are rejected. Any message on the topic counts as a
change — the payload is not inspected. The watch consumer is a separate
consumer from the request/reply path, so passive watching never
interferes with correlated-reply offset handling.

## Observability
Each call records the `mcpg_kafka_binding_call_ms` histogram, labelled by
`backend`, and increments `mcpg_kafka_binding_calls_total` with an
`outcome` label of `ok` or `error`. Failures add a bounded `error_kind`
label — `profile_not_found`, `transport`, `timeout`, or `invalid_spec`.

## MCP surfaces & composition
The binding is declared per capability under `mcp.capabilities.*`; the
same `backend:` block shape works on every surface.

### As a pipeline step
`kind: kafka` is pipeline-capable. Step keys other than `id` and
`input_transform` flatten into the spec.

```yaml
backend:
  kind: pipeline
  steps:
    - kind: kafka
      id: enrich
      request_topic: enrich.requests
      response_topic: enrich.responses
```

### As a resource
```yaml
mcp:
  capabilities:
    resources:
      - name: inventory.snapshot
        description: Current inventory snapshot from the inventory worker.
        uri: "inventory://snapshot"
        mime_type: application/json
        backend:
          kind: kafka
          request_topic: inventory.requests
          response_topic: inventory.responses
```

### As a prompt
```yaml
mcp:
  capabilities:
    prompts:
      - name: events.explain
        description: Explain an event to an operator.
        prompt_arguments:
          - name: event_id
            required: true
        backend:
          kind: kafka
          request_topic: explain.requests
          response_topic: explain.responses
```

### Schemas & annotations
Every binding accepts the MCP descriptor fields as siblings of
`backend:` — `title`, `input_schema`, `output_schema`, `icons`, and
`annotations` (`read_only`, `destructive`, `idempotent`, `open_world`).
A sibling `retry:` block (`max_attempts` default `3`,
`initial_backoff_ms` default `200`, `retry_on_transport_error` default
true) governs gateway-side retries, and `governance:` carries the trust
floor and CEL authorization for the surface.

## Build
Default feature set is OFF (avoids `mcpg_plugin_register` linker
collisions in the workspace build); opt in to the cdylib:

```bash
cargo build -p mcpg-plugin-backend-kafka --features cdylib-export --release   # → target/release/libmcpg_plugin_backend_kafka.so
```

librdkafka is built from source through rdkafka's `cmake-build` feature
with vendored zlib and libcurl, so the build needs a C toolchain and
CMake but no system librdkafka. The Cyrus SASL (`sasl2-sys`) dependency
is deliberately absent, which is what keeps the cross-compiled and
statically linked lanes buildable; the cost is that GSSAPI/Kerberos is
unavailable.

Releases publish a platform-agnostic OCI artifact, so a `plugins:` entry
can set `source.oci` to
`ghcr.io/mcpg-dev/source-code/plugins/backend-kafka:protocol-1` instead
of `source.path` and let the gateway resolve the right os/arch/libc
build for its host.

## Testing
```bash
cargo test -p mcpg-plugin-backend-kafka
```

The unit suite runs offline and needs no broker: it covers config
parsing and fail-closed behaviour, spec validation, topic and
credential-grammar rules, outbound header construction, and the
per-caller credential-resolution path against a stub host.

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Backend binding reference: <https://mcpg.dev/docs/reference/backends>
- Pipeline step kinds: <https://mcpg.dev/docs/reference/pipeline-steps>
- Licence terms for this plugin — BUSL-1.1, with an Additional Use Grant
  for production use and a Change License of Apache-2.0: [LICENSE](LICENSE)
- Other messaging and network bindings: `libs/plugins/backend/nats`, `libs/plugins/backend/http`
