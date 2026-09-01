# ART-14447: Sign Without UMB — Direct Signing Server Access

## Summary

UMB is being decommissioned. The only ART interaction with UMB is in the release-signing
flow, where `AsyncSignatory` sends requests to RADAS over STOMP/UMB and waits for signed
artifacts.

ART will move to the direct signing route: ART will no longer send signing requests through
RADAS and UMB. The exact direct integration point is still to be confirmed after the signing
server team approves access. The options are to trigger an appropriate Tekton pipeline in the
`signing` repository or to use its signing client and verification code as a reference for a
direct ART integration.

The signing team is also developing a generic-signing pipeline that signs files regardless of
file type. That may be a particularly good fit for ART's JSON digest claims and message
digests, but its merge request is not reviewed yet.

The Kafka transport swap is retained only as a contingency. It is not the target architecture
and should not be implemented in parallel with the direct-signing work.

## Context

### Current Flow

`AsyncSignatory` in `pyartcd/pyartcd/signatory.py` signs OCP artifacts by:

1. Connecting to UMB via STOMP (`AsyncUMBClient` in `pyartcd/pyartcd/umb_client.py`)
2. Sending a signing request (base64-encoded artifact and metadata) to
   `/topic/VirtualTopic.eng.art.artifact.sign`
3. Subscribing to a consumer queue
   `/queue/Consumer.{sa}.{sub}.VirtualTopic.eng.robosignatory.art.sign`
4. Waiting for RADAS to return the signed artifact on that queue
5. Decoding the base64 response and writing the signature file

This is called from `pyartcd/pyartcd/pipelines/promote.py` to sign JSON digest claims
(container image signatures) and message digests (`sha256sum.txt.gpg`). It is also used in
`pyartcd/pyartcd/pipelines/sync_rhcos.py` for RHCOS message-digest signing.

Sigstore/cosign signing (`SigstoreSignatory`) is a separate mechanism that does not use UMB
and is unaffected by this work.

### Constraints and Recent Feedback

- The UMB migration deadline is **the end of September 2026**.
- RADAS has an extension to remain on UMB until Q1 2027, but that extension does not cover
  every other service that interacts with UMB. ART must therefore plan against the September
  deadline and request an extension from the UMB team if direct signing is at risk.
- The RADAS team plans to sunset RADAS rather than migrate it to Kafka. Continuing to use
  RADAS through a Messaging Bridge would be a temporary solution with another migration later.
- Konflux is already moving to direct signing, so its work is a useful reference.
- The signing team confirmed that consumers may either trigger one of the Tekton pipelines in
  the `signing` repository or use the repository's signing and verification code as reference.
- A generic-signing pipeline is being developed in
  [signing MR !92](https://gitlab.cee.redhat.com/signing/signing/-/merge_requests/92). It is
  not reviewed yet, so ART must not make delivery depend on it until its availability and
  interface are confirmed.
- Direct signing requires a dedicated service account with its own Kerberos principal and
  keytab, possibly one per environment, plus explicit signing permissions from the signing
  server team.

## Decision: Direct Signing

ART will pursue direct signing server access as the primary and intended solution. This
bypasses both RADAS and the message bus, aligns with the signing team's and Konflux's direction,
and avoids adding a Kafka dependency for a service that is planned for retirement.

Direct signing may mean invoking a signing-repository Tekton pipeline or calling the approved
signing interface from ART. Both choices remove UMB/RADAS from the ART request path. The
technical choice must be made only after the signing server team confirms the supported
interface, authentication model, and ART's signing permissions.

### Approval and Access Are Phase Zero

No technical implementation should start before access approval. The first action is to open a
Jira request in the `SIGNSERVER` project asking for:

- A service account for ART with its own Kerberos principal and keytab.
- Separate credentials where required for stage, production, or other environments.
- Permission to perform ART's signing operations on the required signing keys.
- The supported direct-signing interface, authentication requirements, and onboarding steps.

[`SIGNSERVER-2309`](https://redhat.atlassian.net/browse/SIGNSERVER-2309) is an example of a
signing-permission request created for Konflux. It may be access-restricted, but it can serve as
a template when filing the ART request.

### Signing Repository Investigation

After approval, use the signing repository as the primary technical reference:

- Inspect the existing Tekton pipelines and determine whether ART can trigger one directly.
- Evaluate the generic-signing pipeline from
  [MR !92](https://gitlab.cee.redhat.com/signing/signing/-/merge_requests/92) once its interface
  and review status are known.
- Review the signing and signature-verification code in the repository.
- Review [RADAS's ART consumer](https://gitlab.cee.redhat.com/signing/radas/-/blob/master/radas/artconsumer.py#L79),
  which concentrates the current ART-facing request and response behavior in one place.

The investigation must answer:

1. Is the supported entry point a REST API, gRPC API, Tekton pipeline trigger, or another
   interface?
2. What request and response format represents JSON digest claims and message digests?
3. How are authentication, Kerberos credentials, key selection, and signing permissions
   configured?
4. Which stage and production endpoints are available?
5. What timeout, retry, rate-limit, and failure semantics apply?
6. Does the generic-signing pipeline support all ART use cases, or does ART need a narrower
   integration?

## Proposed ART Design

The implementation should keep the existing signing boundary stable while replacing the
RADAS/UMB mechanism behind it:

- Introduce or refactor a `DirectSignatory` that exposes the existing
  `sign_json_digest` and `sign_message_digest` methods.
- Keep callers in `promote.py` and `sync_rhcos.py` unchanged where possible.
- If the approved entry point is a Tekton pipeline, create and monitor the relevant TaskRun or
  PipelineRun and retrieve or publish the resulting signature files according to the signing
  team's contract.
- If the approved entry point is a direct client API, isolate request construction, credential
  handling, response validation, and error translation in the signatory implementation.
- Do not embed credentials in source or pipeline arguments. Use the approved service-account,
  Kerberos-principal, and keytab provisioning mechanism for each environment.
- Add a configurable response timeout, with a default of 10 minutes unless the signing team
  specifies a different limit.
- Keep the current UMB path available during staged rollout. A temporary switch such as
  `--signing-transport direct|umb` may be used in `promote.py` and `sync_rhcos.py`; its name and
  final configuration location should be confirmed once the direct integration shape is known.
- Remove the switch and the UMB implementation after the direct path is proven in production.

The design should not carry over UMB-specific concepts such as consumer queues, message
acknowledgements, or stale-message filtering unless the approved direct interface actually
requires equivalent behavior.

### Candidate Integration Modes

| Mode | Description | Assessment |
|---|---|---|
| **Signing-repository pipeline** | ART triggers an approved Tekton pipeline and waits for the signed output. | Prefer if the generic-signing pipeline supports ART's inputs and has a stable interface. |
| **Direct signing client** | ART calls the approved signing interface using the signing repository's code as reference. | Use if pipeline triggering adds unnecessary orchestration or the generic pipeline is not ready. |
| **Kafka through Messaging Bridge** | ART replaces STOMP with Kafka while RADAS remains behind the bridge. | Contingency only; requires continued RADAS dependency and a later migration. |

## Rollout Plan

### Phase 0 — Signing Server Approval and Access

- File the `SIGNSERVER` Jira request, using `SIGNSERVER-2309` as a reference if accessible.
- Request ART's service account, Kerberos principal, keytab, environment credentials, and key
  permissions.
- Confirm the supported direct interface and the signing team's operational requirements.
- If approval or provisioning cannot complete before the September deadline, request an UMB
  extension immediately rather than assuming the RADAS extension applies to ART.

### Phase 1 — Direct-Signing Discovery

- Inspect the signing repository's pipelines, signing code, and verification code.
- Evaluate the generic-signing MR !92 and its expected contract.
- Compare the pipeline-trigger and direct-client modes using ART's two signing use cases.
- Validate the request/response format and signature verification in a non-production
  environment.

### Phase 2 — Implementation

- Implement `DirectSignatory` or the equivalent direct integration.
- Preserve the existing signatory methods and publishing behavior.
- Add the temporary direct/UMB rollout switch if needed.
- Add timeout, retry, authentication, and error handling required by the approved interface.
- Add unit tests and docstrings before stage validation.

### Phase 3 — Stage Validation

- Run `promote.py` with direct signing in stage.
- Run `sync_rhcos.py` message-digest signing in stage.
- Verify that produced signatures are valid and published to the expected locations.
- Exercise signing failures, timeouts, invalid responses, credential failures, and retry
  behavior.

### Phase 4 — Production Cutover

- Enable direct signing in the production Jenkins/Tekton configuration.
- Keep UMB available as an immediate rollback while monitoring several successful releases.
- Complete cutover by the end of September 2026, or obtain an explicit UMB extension if that
  date cannot be met.

### Phase 5 — Cleanup

- Remove the temporary rollout switch and UMB-specific signatory path.
- Remove `AsyncUMBClient`, `umb_client.py`, the `stomp.py` dependency, and `UMB_BROKERS` once
  no ART workflow depends on them.
- Update operational documentation and remove obsolete credentials/configuration.

## Contingency: Kafka Transport via Messaging Bridge

Use this option only if direct signing is blocked by approval, provisioning, or an unavailable
interface and the UMB team confirms that a temporary extension is possible.

IT Platform's Messaging Bridge can synchronize UMB topics with Kafka. ART would replace its
STOMP client with Kafka while the bridge continues forwarding the existing message format to
RADAS. This could preserve the current signing behavior, but it keeps ART dependent on RADAS
and introduces another migration when RADAS is sunset.

If activated, the contingency design would:

- Introduce a transport abstraction so the existing `AsyncSignatory` logic can run over UMB or
  Kafka.
- Use a unique Kafka consumer group per signing run and correlate responses by the existing
  `request_id`.
- Commit Kafka offsets only after a matching response has been validated and processed.
- Add a bounded response timeout to both transports.
- Onboard Kafka topics, credentials, and bridge mappings with IT Platform before code rollout.

The Kafka plan is deliberately not the current implementation target. No Kafka code or
infrastructure work should begin unless the direct route is formally blocked or the schedule
requires this contingency.

## Alternatives Considered

### Continue Through RADAS and Kafka

This would be a smaller transport change, but it preserves a dependency on RADAS even though
the RADAS team plans to sunset it. It is therefore a contingency rather than a long-term
solution.

### Move Signing into an ART-Owned Konflux Task

ART could create its own Tekton Task or PipelineRun and have that task perform signing. This is
still compatible with the direct-signing decision, but it adds orchestration and ownership for
ART. Triggering an existing approved pipeline in the signing repository should be preferred if
it provides the required contract. An ART-owned task remains an option if the signing team
recommends it after access approval.

## Testing Strategy

### Unit Tests

- Mock the approved signing API or pipeline client and verify request construction.
- Cover the happy path for both JSON digest claims and message digests.
- Verify response parsing, signature-file handling, and existing publishing behavior.
- Cover signing failures, invalid responses, timeouts, credential failures, and retries.
- Verify that callers in `promote.py` and `sync_rhcos.py` retain the expected behavior.
- Add docstrings that explain the direct interface, credential assumptions, and lifecycle.

### Integration and Manual Testing

1. Validate the approved direct entry point in stage.
2. Run both ART signing paths in stage.
3. Verify signatures using the signing repository's verification code or the existing ART
   verification tooling.
4. Confirm successful publication and rollback behavior.
5. Cut over to production only after the stage results and signing-server team approval are
   recorded.

## References

- Jira: [ART-14447](https://redhat.atlassian.net/browse/ART-14447)
- Signing repository: [signing](https://gitlab.cee.redhat.com/signing/signing)
- Generic signing pipeline: [signing MR !92](https://gitlab.cee.redhat.com/signing/signing/-/merge_requests/92)
- Current ART consumer reference: [radas/artconsumer.py](https://gitlab.cee.redhat.com/signing/radas/-/blob/master/radas/artconsumer.py#L79)
- Example signing-permission request: [SIGNSERVER-2309](https://redhat.atlassian.net/browse/SIGNSERVER-2309)
- UMB decommissioning plan: [Google Doc](https://docs.google.com/document/d/1k0ch92nck9vFotretm3O2mPVhPIEmzyvidRsEvBXcQ0)
- RADAS to Kafka: [Google Doc](https://docs.google.com/document/d/1j0L-C9KCQQqXtMc0tgSs8DgetZRXkW40FqZr1zm3B_s)
- Retriable Kafka Client: [GitHub](https://github.com/release-engineering/Retriable-Kafka-Client)
- RADAS Kafka decision: CLOUDWF-11222
- UMB decommission epic: ITESPLAT-4153
- Konflux direct signing (containers): [KONFLUX-11077](https://redhat.atlassian.net/browse/KONFLUX-11077)
- Konflux direct signing (RPMs): [KONFLUX-8400](https://redhat.atlassian.net/browse/KONFLUX-8400)
- Signing Server Operations Guide: [Confluence](https://redhat.atlassian.net/wiki/spaces/PRODSEC/pages/289239814/Signing+Server+Operations+Guide)
- ProdSec overview of Konflux direct signing: [Google Doc](https://docs.google.com/document/d/1TqRfCKI_XdHG4npLSdEEw-2Tj21nKF3ANgvf70DMKHc)
