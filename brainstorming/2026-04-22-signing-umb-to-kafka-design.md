# ART-14447: Sign Without UMB — Migrate Signing Transport from UMB to Kafka

## Summary

UMB is being decommissioned in Q3 2026 (code-complete deadline: end of Q2 2026). The only ART
interaction with UMB is in the signing flow during release promotion. This design replaces the
STOMP-based UMB transport with Kafka, using IT Platform's Messaging Bridge to maintain
compatibility with RADAS (which still consumes from UMB).

## Context

### Current Flow

`AsyncSignatory` in `pyartcd/pyartcd/signatory.py` signs OCP artifacts by:

1. Connecting to UMB via STOMP (`AsyncUMBClient` in `pyartcd/pyartcd/umb_client.py`)
2. Sending a signing request (base64-encoded artifact + metadata) to
   `/topic/VirtualTopic.eng.art.artifact.sign`
3. Subscribing to a consumer queue
   `/queue/Consumer.{sa}.{sub}.VirtualTopic.eng.robosignatory.art.sign`
4. Waiting for RADAS to return the signed artifact on that queue
5. Decoding the base64 response and writing the signature file

This is called from `pyartcd/pyartcd/pipelines/promote.py` to sign JSON digest claims
(container image signatures) and message digests (sha256sum.txt.gpg).

### Why Messaging Bridge (Not Direct Signing Server or Waiting for RADAS on Kafka)

- **RADAS's migration to Kafka is TBD** — the RADAS team hasn't decided their path yet
  (see CLOUDWF-11222). Waiting on them risks missing the Q2 deadline.
- **Direct signing server API** (bypassing RADAS) is unexplored and would require
  reimplementing RADAS business logic — too much risk under deadline pressure.
- **Messaging Bridge** is already in production, recommended by IT Platform for exactly this
  use case, and forwards messages bidirectionally between UMB topics and Kafka topics. The
  message format stays identical.

## Design

### Transport Abstraction

Introduce a `SigningTransport` protocol that `AsyncSignatory` delegates to, keeping all signing
logic in one place:

```python
class SigningTransport(Protocol):
    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def send(self, destination: str, body: str) -> None: ...
    async def subscribe(self, queue: str, subscription_id: str) -> AsyncIterator: ...
    async def ack(self, message_id: str, subscription_id: str) -> None: ...
```

Two implementations:

- **`UMBTransport`** — wraps the existing `AsyncUMBClient` (extracts what `AsyncSignatory`
  already does today). `ack()` maps directly to STOMP's per-message acknowledgement.
- **`KafkaTransport`** — wraps `Retriable-Kafka-Client`, mapping `send()` to the Kafka
  producer and `subscribe()` to the Kafka consumer on the response topic. `ack()` commits
  the Kafka offset. Offsets are committed only after a response with matching `request_id`
  is successfully validated and processed. This provides at-least-once delivery; duplicate
  responses are tolerated via existing `request_id` correlation (duplicates are discarded
  since the corresponding `Future` has already been resolved).

`AsyncSignatory.__init__` accepts a `transport: SigningTransport` instead of URI/cert/key
directly. The factory logic in `promote.py` decides which transport to create based on
`--signing-transport`.

Benefits:
- `AsyncSignatory` signing logic doesn't change — zero risk
- Each transport is independently testable
- When UMB is decommissioned, delete `UMBTransport` and `umb_client.py`
- No `if/else kafka/umb` scattered through signing code

### Async Bridging for KafkaTransport

`Retriable-Kafka-Client` uses synchronous `confluent_kafka` with a thread-based executor pool
model. Our signing flow is async (asyncio). `KafkaTransport` bridges this by running the Kafka
consumer poll loop in a thread and feeding messages into an `asyncio.Queue` — the same pattern
`AsyncUMBClient` already uses to bridge `stomp.py`'s threading model into asyncio.

### Files Changed

| File | Change |
|---|---|
| `pyartcd/pyartcd/signing_transport.py` (new) | `SigningTransport` protocol, `UMBTransport`, `KafkaTransport` |
| `pyartcd/pyartcd/kafka_client.py` (new) | Async Kafka client wrapping `Retriable-Kafka-Client` |
| `pyartcd/pyartcd/signatory.py` | Refactor `AsyncSignatory` to accept `SigningTransport` instead of UMB-specific params |
| `pyartcd/pyartcd/constants.py` | Add `KAFKA_BROKERS` dict (per environment) and Kafka topic names |
| `pyartcd/pyartcd/pipelines/promote.py` | `sign_artifacts()` reads `--signing-transport` flag, creates appropriate transport |
| `pyartcd/pyartcd/pipelines/sync_rhcos.py` | Same transport selection for RHCOS message digest signing |
| CLI wiring (promote and sync_rhcos pipelines) | Add `--signing-transport` option (choices: `umb`, `kafka`, default: `umb`) |

### Kafka Authentication & Configuration

Kafka uses SASL username/password auth (unlike UMB's certificate-based auth):

- **Broker URLs** — `KAFKA_BROKERS` in `constants.py`, keyed by environment (prod/stage/qa/dev)
- **Credentials** — environment variables `KAFKA_USERNAME` and `KAFKA_PASSWORD`, following the
  same pattern as `SIGNING_CERT`/`SIGNING_KEY` for UMB
- **Consumer group ID** — `artcd-signing-{uuid}` per instance, ensuring no two promote runs
  steal each other's responses. Ephemeral groups are cleaned up by Kafka automatically.
- **Topics** — Kafka topic names set up during Messaging Bridge onboarding, stored in
  `constants.py`

Topic mapping (exact names TBD during onboarding):

| UMB | Kafka |
|---|---|
| `/topic/VirtualTopic.eng.art.artifact.sign` (send) | TBD during bridge setup |
| `/queue/Consumer.{sa}.{sub}.VirtualTopic.eng.robosignatory.art.sign` (receive) | TBD during bridge setup |

Request/response correlation: the existing `request_id` field in the message body is used to
match responses. On Kafka, the consumer receives all messages on the response topic and filters
by `request_id`, discarding unrelated messages — same as the current UMB flow.

### Toggle Mechanism

New CLI flag `--signing-transport` on the promote pipeline:

- Choices: `umb`, `kafka`
- Default: `umb` (safe rollout)
- Mirrors the existing `--signing-env` pattern (prod/stage/qa/dev)
- Set in Jenkins/Tekton pipeline configuration
- Removed after UMB decommissioning (along with UMB code)

### Error Handling & Resilience

| Failure Mode | Behavior |
|---|---|
| **Connection failure** | `KafkaTransport` lets the error bubble up and fail the pipeline, same as `UMBTransport` |
| **Stale messages** | Same 1-hour staleness check on message timestamp — transport-independent |
| **Signing failure** | RADAS returning `signing_status != "success"` raises `SignatoryServerError` — unchanged |
| **Response timeout** | NEW: configurable timeout (default 10 minutes) on both transports. Raises `TimeoutError` instead of hanging indefinitely. This fixes a pre-existing issue in the UMB path too. |
| **Consumer group conflicts** | Unique group ID per instance (`artcd-signing-{uuid}`) prevents response stealing |
| **Kafka offset-commit semantics** | `KafkaTransport` commits offsets only after a response with matching `request_id` is successfully validated and processed. Unmatched or stale messages are not committed until a valid response is found. This provides at-least-once delivery; duplicates are tolerated via `request_id` correlation. |

### Testing Strategy

**Unit tests:**

- `KafkaTransport` — mock `Retriable-Kafka-Client` producer/consumer, verify send/subscribe/ack
- `UMBTransport` — refactor existing `test_signatory.py` tests to use the wrapper
- `AsyncSignatory` with transport injection — signing logic works identically with either transport
- Timeout — both transports raise clear error when no response arrives
- Stale message filtering — existing coverage adapted for both transports

**Integration/manual testing:**

1. Stage: set up Kafka + Bridge on stage, run `--signing-transport kafka --signing-env stage`
2. Verify end-to-end: request → Kafka → Bridge → UMB → RADAS → UMB → Bridge → Kafka → response
3. Prod: flip to `--signing-transport kafka --signing-env prod`

## Alternative Considered: Move Signing to a Konflux Task

Instead of migrating the transport layer inside pyartcd, an alternative approach is to move
the signing step itself into a Konflux Tekton Task. The promote pipeline (Jenkins) would
trigger a Konflux TaskRun, passing signing parameters, and wait for the signed artifacts to
come back.

### How It Would Work

1. A **Tekton Task** is deployed in the ART Konflux tenant that handles signing requests
   (initially via RADAS/UMB or Kafka, eventually via the direct signing server API)
2. `promote.py` uses the existing `KonfluxClient` (from `artcommon/artcommonlib/konflux/`)
   to create a TaskRun with parameters: pullspecs, digests, version, sig_keyname
3. The TaskRun produces signature files (either as OCI artifacts or pushes directly to S3)
4. `promote.py` waits for the TaskRun to complete, then continues with publishing

This pattern already exists in ART: `ocp4_konflux.py` shells out to doozer, which uses
`KonfluxImageBuilder` and `KonfluxClient` to create PipelineRuns via the Kubernetes API and
watch them to completion. The machinery for Konflux orchestration is proven.

### Where Signing Happens Today

Signing via RADAS/UMB (`AsyncSignatory`) is used in two pipelines:

| Pipeline | What It Signs |
|---|---|
| `promote.py` | JSON digest claims (container image signatures) and message digests (sha256sum.txt.gpg) |
| `sync_rhcos.py` | RHCOS message digests |

Both would need to be migrated if this approach is taken. Sigstore/cosign signing
(`SigstoreSignatory`) is unrelated — it doesn't use UMB and is unaffected.

### Comparison

| | Approach A: Kafka Transport Swap | Approach B: Move Signing to Konflux |
|---|---|---|
| **Scope** | Replace transport layer in existing pyartcd code | Build new Tekton Task + pyartcd→Konflux orchestration |
| **Effort** | ~1-2 weeks | Significantly more — Tekton Task definition, cross-system orchestration for a non-build use case, S3 publishing from Konflux or artifact handoff |
| **UMB dependency** | ART owns the Kafka migration | Konflux Task owns the RADAS interaction |
| **Future benefit** | Will need another migration when direct signing lands | Automatically gets direct signing when [KONFLUX-11077](https://redhat.atlassian.net/browse/KONFLUX-11077) delivers — the Tekton Task switches internally, promote doesn't change |
| **Complexity** | Low — well-understood transport swap | Medium — new cross-system orchestration pattern, Konflux Task needs RADAS access/credentials, distributed lock coordination across Jenkins and Konflux |
| **Q2 2026 deadline** | Comfortable margin | Tight — depends on Tekton Task development + integration testing |
| **Signing credentials** | Stay in Jenkins/pipeline secrets | Move to Konflux (cleaner long-term) |
| **Debugging** | Signing failures visible in promote job logs | Signing failures require checking Konflux TaskRun logs |
| **sync_rhcos.py** | Also migrated with the same transport abstraction | Also needs migration — or becomes a second signing path |

### Recommendation

**Approach A (Kafka transport swap) is recommended for the Q2 deadline.** It's lower risk,
lower effort, and gets ART off UMB on time. Approach B is the better long-term architecture —
when KONFLUX-11077 delivers direct signing, moving signing into a Konflux Task becomes the
natural evolution. At that point, the Tekton Task uses the direct signing server API (no
message bus at all), and promote simply triggers and waits.

A possible phased path:
1. **Now (Q2 2026):** Kafka transport swap (this spec) — meets the UMB deadline
2. **When KONFLUX-11077 is proven (Q3-Q4 2026):** Move signing into a Konflux Task using the
   direct signing API — eliminates Kafka, RADAS, and the Messaging Bridge from ART's stack

## Rollout Plan

### Phase 1 — Code (this PR)

- Add `SigningTransport` protocol, `UMBTransport`, `KafkaTransport`
- Add `kafka_client.py` wrapping `Retriable-Kafka-Client`
- Refactor `AsyncSignatory` to accept transport injection
- Add `--signing-transport` flag (default: `umb`) to both `promote.py` and `sync_rhcos.py`
- Add response timeout to both transports
- Unit tests

### Phase 2 — Infrastructure Onboarding (non-code)

- Request Kafka access from IT Platform (IT Managed Kafka User Guide)
- Create Kafka topics for signing request/response
- Request Messaging Bridge sync for RADAS VirtualTopics
- Obtain Kafka credentials, store in pipeline secrets

### Phase 3 — Validate on Stage

- Run promote on stage with `--signing-transport kafka`
- Run `sync_rhcos` signing on stage with `--signing-transport kafka`
- Verify full round-trip through the bridge for both pipelines

### Phase 4 — Prod Cutover

- Flip Jenkins/Tekton configs to `--signing-transport kafka`
- Keep `umb` available as fallback
- Monitor several successful releases

### Phase 5 — Cleanup (after UMB decommissioned)

- Remove `--signing-transport` flag, `UMBTransport`, `umb_client.py`
- Remove `stomp.py` dependency
- Remove `UMB_BROKERS` from constants
- Notify IT Platform to remove Messaging Bridge sync
- Optionally inline `KafkaTransport` if abstraction no longer adds value

### Future: Direct Signing Server (Phase 6)

The Konflux team is actively developing direct access to the signing server, bypassing both
RADAS and UMB/Kafka entirely ([KONFLUX-11077](https://redhat.atlassian.net/browse/KONFLUX-11077),
[KONFLUX-8400](https://redhat.atlassian.net/browse/KONFLUX-8400)). As of April 2026, the PoC
is nearly complete, with the RADAS team (Martin Sikora, Wai Cheang) working on the API. Once
this is proven in Konflux, ART could adopt the same direct signing API — replacing
`KafkaTransport`, the Messaging Bridge dependency, and RADAS from the flow entirely.

This is not in scope for ART-14447 because:
- The direct API is not yet available outside of Konflux pipelines
- ART's promote pipeline runs in Jenkins/Tekton, not as a Konflux pipeline
- The Q2 2026 deadline doesn't allow time to wait for or integrate an unfinished API
- Rate-limiting concerns (RADAS currently throttles requests) have not been resolved yet

When the direct signing API stabilizes, a follow-up ticket should evaluate migrating ART to it.
This would be the cleanest long-term solution — no message bus dependency at all.

### Timeline

Phase 1 is ~1-2 weeks of development. Phases 2-3 can overlap. Q2 deadline (end of June)
provides comfortable margin.

## References

- Jira: [ART-14447](https://redhat.atlassian.net/browse/ART-14447)
- UMB Decommissioning plan: [Google Doc](https://docs.google.com/document/d/1k0ch92nck9vFotretm3O2mPVhPIEmzyvidRsEvBXcQ0)
- RADAS to Kafka: [Google Doc](https://docs.google.com/document/d/1j0L-C9KCQQqXtMc0tgSs8DgetZRXkW40FqZr1zm3B_s)
- Retriable Kafka Client: [GitHub](https://github.com/release-engineering/Retriable-Kafka-Client)
- RADAS Kafka decision: CLOUDWF-11222
- UMB decommission epic: ITESPLAT-4153
- Konflux direct signing (containers): [KONFLUX-11077](https://redhat.atlassian.net/browse/KONFLUX-11077)
- Konflux direct signing (RPMs): [KONFLUX-8400](https://redhat.atlassian.net/browse/KONFLUX-8400)
