# ART-14447: Sign Without UMB — Migrate Off RADAS/UMB for Release Signing

## Summary

UMB is being decommissioned in Q3 2026. The only ART interaction with UMB is in the signing
flow during release promotion, where `AsyncSignatory` sends requests to RADAS over
STOMP/UMB and waits for signed artifacts.

Based on feedback from the RADAS team: RADAS has received an extension to stay on UMB with
reduced support until Q1 2027, and the plan is to sunset RADAS entirely rather than migrate
it to Kafka. The RADAS team recommends exploring direct signing server access, which is
reportedly simpler than a Kafka migration and is already being adopted by Konflux
([KONFLUX-11077](https://redhat.atlassian.net/browse/KONFLUX-11077)).

**Updated recommendation:** Explore direct signing server access as the primary approach.
The Kafka transport swap remains a viable fallback if direct signing doesn't pan out in time.

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
(container image signatures) and message digests (sha256sum.txt.gpg). It is also used in
`pyartcd/pyartcd/pipelines/sync_rhcos.py` for RHCOS message digest signing.

Sigstore/cosign signing (`SigstoreSignatory`) is a separate mechanism that does not use UMB
and is unaffected by this work.

### Timeline Constraints

- **UMB decommissioned:** Q3 2026 (general), but RADAS has an extension until Q1 2027
- **UMB code-complete deadline:** Originally end of Q2 2026, but with the RADAS extension
  there is more flexibility
- **Konflux direct signing PoC:** Nearly complete as of April 2026
  ([KONFLUX-11077](https://redhat.atlassian.net/browse/KONFLUX-11077))
- **RADAS sunset:** Planned for 2027 — the RADAS team prefers to sunset rather than migrate
  to Kafka

## Recommended Approach: Direct Signing Server Access

### Rationale

Feedback from the RADAS team indicates that calling the signing server directly (bypassing
RADAS and UMB entirely) is straightforward and likely simpler than migrating to Kafka. The
main complexity is working with ProdSec to get approval and access. Konflux is already in
the process of switching to direct signing
([KONFLUX-11077](https://redhat.atlassian.net/browse/KONFLUX-11077),
[KONFLUX-8400](https://redhat.atlassian.net/browse/KONFLUX-8400)).

This approach is the cleanest long-term solution:
- No message bus dependency at all (no UMB, no Kafka, no Messaging Bridge)
- No RADAS dependency (which is being sunset anyway)
- Aligns with the direction Konflux is already taking
- Fewer moving parts and infrastructure to maintain

### What Needs to Be Explored

Before committing to this approach, the following must be clarified:

1. **Signing server API:** What is the interface? REST, gRPC, or something else? What
   authentication is required?
2. **ProdSec approval:** What is the process and timeline for getting ART approved for
   direct access?
3. **ART's signing use cases:** Can all of ART's signing operations (JSON digest claims,
   message digests) be handled by direct signing, or are some RADAS-specific?
4. **Rate limiting:** RADAS currently acts as a throttling layer. Does the signing server
   have its own rate limits, and does ART's volume require any throttling on our side?
5. **Environments:** Does the signing server have stage/prod environments matching ART's
   needs?
6. **Credentials and access:** What credentials are needed, and how are they provisioned?

### High-Level Design (Pending Exploration)

If direct signing is viable, the implementation would:

- Replace `AsyncSignatory` (which wraps RADAS/UMB request/response) with a new
  `DirectSignatory` that calls the signing server API directly
- Keep the same public interface (`sign_json_digest`, `sign_message_digest`) so callers
  in `promote.py` and `sync_rhcos.py` don't change
- Use `--signing-transport` CLI flag (choices: `umb`, `direct`, default: `umb`) to allow
  incremental rollout, same toggle pattern as the Kafka approach
- Add a configurable response timeout (default 10 minutes) — fixes a pre-existing issue
  in the UMB path where there is no timeout
- Include solid unit tests and docstrings per the acceptance criteria

### Rollout Plan (Direct Signing)

#### Phase 1 — Exploration and ProdSec Approval

- Investigate signing server API (interface, auth, environments)
- Coordinate with ProdSec for approval and access
- Validate that ART's signing use cases are supported
- Assess rate-limiting requirements

#### Phase 2 — Implementation

- Implement `DirectSignatory` (or refactor `AsyncSignatory`) with the signing server API
- Add `--signing-transport` flag (choices: `umb`, `direct`, default: `umb`) to both
  `promote.py` and `sync_rhcos.py`
- Add response timeout to both paths
- Unit tests

#### Phase 3 — Validate on Stage

- Run promote on stage with `--signing-transport direct`
- Run `sync_rhcos` signing on stage with `--signing-transport direct`
- Verify end-to-end signing and signature publishing

#### Phase 4 — Prod Cutover

- Flip Jenkins/Tekton configs to `--signing-transport direct`
- Keep `umb` available as fallback
- Monitor several successful releases

#### Phase 5 — Cleanup

- Remove `--signing-transport` flag, `AsyncSignatory`, `AsyncUMBClient`, `umb_client.py`
- Remove `stomp.py` dependency
- Remove `UMB_BROKERS` from constants

## Fallback: Kafka Transport Swap via Messaging Bridge

If direct signing doesn't pan out (ProdSec approval delays, API not ready, etc.), the Kafka
transport swap remains a viable fallback that can be implemented within the timeline.

### How It Works

IT Platform's Messaging Bridge (already in production) bidirectionally syncs UMB topics to
Kafka topics. ART migrates from STOMP to Kafka; the bridge forwards messages to/from RADAS
on UMB. The message format stays identical — only the transport changes.

### Design

Introduce a `SigningTransport` protocol that `AsyncSignatory` delegates to:

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

`Retriable-Kafka-Client` uses synchronous `confluent_kafka` with a thread-based executor
pool model. `KafkaTransport` bridges this into asyncio by running the Kafka consumer poll
loop in a thread and feeding messages into an `asyncio.Queue` — the same pattern
`AsyncUMBClient` already uses to bridge `stomp.py`'s threading model.

### Files Changed (Kafka Approach)

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

### Error Handling & Resilience (Kafka Approach)

| Failure Mode | Behavior |
|---|---|
| **Connection failure** | `KafkaTransport` lets the error bubble up and fail the pipeline, same as `UMBTransport` |
| **Stale messages** | Same 1-hour staleness check on message timestamp — transport-independent |
| **Signing failure** | RADAS returning `signing_status != "success"` raises `SignatoryServerError` — unchanged |
| **Response timeout** | NEW: configurable timeout (default 10 minutes) on both transports. Raises `TimeoutError` instead of hanging indefinitely. This fixes a pre-existing issue in the UMB path too. |
| **Consumer group conflicts** | Unique group ID per instance (`artcd-signing-{uuid}`) prevents response stealing |
| **Kafka offset-commit semantics** | `KafkaTransport` commits offsets only after a response with matching `request_id` is successfully validated and processed. Unmatched or stale messages are not committed until a valid response is found. This provides at-least-once delivery; duplicates are tolerated via `request_id` correlation. |

### Kafka Rollout Plan

1. **Infrastructure onboarding** — Kafka access, topics, Messaging Bridge sync
2. **Code** — transport abstraction, `--signing-transport` flag, timeout, unit tests
3. **Validate on stage** — both `promote.py` and `sync_rhcos.py`
4. **Prod cutover** — flip to `--signing-transport kafka`, monitor
5. **Cleanup** — remove UMB code after decommissioning

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

### Comparison of All Three Approaches

| | Direct Signing Server | Kafka Transport Swap | Move to Konflux Task |
|---|---|---|---|
| **Scope** | Replace RADAS with direct API calls | Replace STOMP with Kafka transport | Build Tekton Task + cross-system orchestration |
| **Effort** | TBD (depends on API complexity) | ~1-2 weeks | Significantly more |
| **Message bus dependency** | None | Kafka + Messaging Bridge | Depends on what the Task uses internally |
| **RADAS dependency** | None | Still depends on RADAS (via bridge) | Depends on implementation |
| **Future-proof** | Yes — this is the target architecture | Temporary — needs another migration later | Yes — if built on direct signing |
| **ProdSec approval** | Required | Not required | Required (for direct signing) |
| **Q2 2026 deadline** | Depends on ProdSec timeline | Comfortable margin | Tight |
| **Signing credentials** | New signing server credentials | Kafka username/password | In Konflux |
| **sync_rhcos.py** | Same approach applies | Same transport abstraction | Needs separate migration |

## Decision

**Explore direct signing server access first.** This is the cleanest solution and aligns
with the direction Konflux is already taking. The RADAS UMB extension to Q1 2027 removes
the immediate urgency, giving time to work through ProdSec approval and integration.

If direct signing is blocked or delayed beyond the timeline, fall back to the Kafka
transport swap, which can be implemented quickly.

## Testing Strategy (Applies to Both Approaches)

**Unit tests:**

- Mock the signing server (or Kafka client) and verify signing logic
- Test timeout behavior — both transports raise clear error when no response arrives
- Test stale message filtering
- Existing `test_signatory.py` tests adapted for the new transport

**Integration/manual testing:**

1. Stage environment first with the new transport
2. Verify both `promote.py` and `sync_rhcos.py` signing paths
3. Prod cutover after successful stage validation

## References

- Jira: [ART-14447](https://redhat.atlassian.net/browse/ART-14447)
- UMB Decommissioning plan: [Google Doc](https://docs.google.com/document/d/1k0ch92nck9vFotretm3O2mPVhPIEmzyvidRsEvBXcQ0)
- RADAS to Kafka: [Google Doc](https://docs.google.com/document/d/1j0L-C9KCQQqXtMc0tgSs8DgetZRXkW40FqZr1zm3B_s)
- Retriable Kafka Client: [GitHub](https://github.com/release-engineering/Retriable-Kafka-Client)
- RADAS Kafka decision: CLOUDWF-11222
- UMB decommission epic: ITESPLAT-4153
- Konflux direct signing (containers): [KONFLUX-11077](https://redhat.atlassian.net/browse/KONFLUX-11077)
- Konflux direct signing (RPMs): [KONFLUX-8400](https://redhat.atlassian.net/browse/KONFLUX-8400)
- Signing Server Operations Guide: [Confluence](https://redhat.atlassian.net/wiki/spaces/PRODSEC/pages/289239814/Signing+Server+Operations+Guide)
- ProdSec overview of Konflux direct signing: [Google Doc](https://docs.google.com/document/d/1TqRfCKI_XdHG4npLSdEEw-2Tj21nKF3ANgvf70DMKHc)
