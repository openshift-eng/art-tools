# ART-14447: Sign Without UMB — Direct Signing Server Access

## Summary

UMB is being decommissioned. The only ART interaction with UMB is in the release-signing
flow, where `AsyncSignatory` sends requests to RADAS over STOMP/UMB and waits for signed
artifacts.

ART will move to the direct signing route: ART will no longer send signing requests through
RADAS and UMB. The promote flow runs on a Jenkins agent, not as a separate ART Tekton task. If
ART invokes a direct client, it will run in that Jenkins job; alternatively, Jenkins may trigger
an appropriate Tekton pipeline in the `signing` repository. The exact direct integration point
is still to be confirmed after the signing server team approves access.

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
- ART's promote signing runs on a long-lived Jenkins agent. The existing keytab infrastructure
  in the ART Kubernetes tasks must not be assumed to cover this path.
- A generic-signing pipeline is being developed in
  [signing MR !92](https://gitlab.cee.redhat.com/signing/signing/-/merge_requests/92). It is
  not reviewed yet, so ART must not make delivery depend on it until its availability and
  interface are confirmed.
- Direct signing requires a dedicated service account with its own Kerberos principal and
  keytab, possibly one per environment, plus explicit signing permissions from the signing
  server team.
- Direct signing from Jenkins also requires ProdSec/network approval for the agent's network
  origin. The signing server team needs the fixed internal egress IP range from which ART
  requests will originate. If Jenkins triggers a signing-repository pipeline instead, confirm
  the network origin and allowlist requirements for that pipeline.

## Decision: Direct Signing

ART will pursue direct signing server access as the primary and intended solution. This
bypasses both RADAS and the message bus, aligns with the signing team's and Konflux's direction,
and avoids adding a Kafka dependency for a service that is planned for retirement.

Direct signing may mean invoking a signing-repository Tekton pipeline or calling the approved
signing interface from ART. Both choices remove UMB/RADAS from the ART request path. The
technical choice must be made only after the signing server team confirms the supported
interface, authentication model, and ART's signing permissions.

### Selected Approach for Promote on BuildVM

For the promote workflow running on buildvm, select the direct-client model: implement a thin
`DirectSignatory` that invokes the signing team's `rh-signing-client` as a subprocess. The
wrapper should preserve the current `sign_json_digest` and `sign_message_digest` interface and
leave the existing promote orchestration and signature publishing behavior largely unchanged.

This is preferred for buildvm because:

- `rh-signing-client` is expected to be installable as an RPM, directly on buildvm from
  Brewroot. No container image, Tekton Task, or `InternalRequest` CR is needed.
- Buildvm already has internal network access for Brew, dist-git, and the UMB brokers. The
  signing team must confirm direct signing-server reachability and any allowlist requirements,
  but no new Kubernetes network path is expected.
- Kerberos is already initialized at pipeline startup by `buildlib.groovy`. The direct signing
  flow must run a second `kinit` with the dedicated signing keytab/principal and an isolated
  `KRB5CCNAME`; it must not clobber the buildvm ticket used by the rest of the build.
- A small wrapper, approximately the size of the current signing boundary, can handle `kinit`,
  temporary input/output files, the `rh-signing-client` subprocess, timeout/error translation,
  and ccache/keytab cleanup without importing the signing team's implementation into ART.
- `promote.py` should select this implementation behind the temporary
  `--signing-transport direct` switch, retaining `umb` for rollback during the rollout.

The exact RPM name, version, Brewroot location, command-line options, signing modes, and output
behavior remain subject to confirmation by the signing team. The `rh-signing-client` referenced
here must also be distinguished from similarly named RHOAI/TAS tooling.

For this buildvm use case, do not select the other direct-integration options unless the signing
team requires them:

- **Library dependency on `signing.git`:** importing or packaging implementation code from
  another team's repository would add coupling for a relatively small wrapper that ART can
  maintain locally.
- **Tekton pipeline delegation:** this adds cross-system orchestration, artifact transfer, and
  TaskRun monitoring. It is useful for Konflux's multi-tenant Kubernetes environment, where the
  outer task cannot hold signing credentials, but buildvm is a controlled machine that can
  install the client and receive the signing keytab directly.

### Implications of the Two Direct-Signing Execution Models

Both models are direct signing: the difference is where the signing client, Kerberos identity,
network access, and signing-server interaction live.

#### A. Invoke a Direct Client from the Jenkins Promote Job

In this model, ART's `DirectSignatory` runs a signing-team client or CLI, such as
`rh-signing-client` if the signing team confirms it is the supported tool. The Jenkins promote
job remains responsible for the signing session:

- The signing client must be installed and available on the Jenkins agent, together with any
  required GPG/Kerberos runtime dependencies.
- Jenkins must receive an environment-specific signing keytab through a secret-file credential
  or approved secret manager. The keytab must not be left permanently on the agent.
- ART must create a unique ccache, run `kinit` for the dedicated principal, pass
  `KRB5CCNAME` to every signing subprocess, and run `kdestroy` plus file cleanup in all exit
  paths.
- The Jenkins agent's fixed egress IP range must be approved and allowlisted for direct access
  to the signing server.
- ART owns request construction, temporary artifact files, timeout/retry behavior, response or
  signature validation, and translation of client failures into pipeline failures.
- The signing operation is local to the promote job, so there is no TaskRun/PipelineRun
  orchestration or second artifact-transfer boundary.

This is the selected model for promote on buildvm. It is likely the smaller ART code change and
avoids an external pipeline dependency. It also places the signing keytab and signed-artifact
handling on a long-lived Jenkins agent, making credential cleanup, workspace cleanup, process
isolation, and auditability particularly important.

#### B. Jenkins Triggers a Tekton Pipeline in the Signing Repository

In this model, Jenkins is only the orchestrator. It submits a request to an approved pipeline
in the `signing` repository and waits for the pipeline result:

- The signing client, Kerberos keytab, ccache, and signing-server network access live inside
  the signing pipeline's execution environment, not on the Jenkins agent.
- The signing-server ACL and network allowlist apply to the pipeline's egress origin. Jenkins
  separately needs permission to trigger and observe the pipeline.
- ART must define a safe input/output contract for both JSON digest claims and message digests:
  how artifacts and metadata are submitted, how signatures are returned, and how large inputs
  are handled.
- `DirectSignatory` must create and monitor the TaskRun or PipelineRun, handle cancellation and
  timeout, map pipeline failure conditions to useful ART errors, and retrieve the resulting
  signature files.
- The signing team owns the pipeline implementation, its signing client, Kerberos lifecycle,
  and operational updates. ART must pin or otherwise control the pipeline version and account
  for availability and interface changes.
- Stage validation tests the complete Jenkins-to-Tekton-to-signing path rather than only a
  local client invocation.

This model keeps the long-lived Jenkins agent away from the signing keytab and avoids
reimplementing the signing-server protocol, but it is not selected for the buildvm promote
workflow. It introduces cross-system orchestration and an additional artifact-transfer and
failure boundary. The generic-signing pipeline from
[MR !92](https://gitlab.cee.redhat.com/signing/signing/-/merge_requests/92) should be evaluated
only if the direct-client approach is unavailable or the signing team requires pipeline
execution, and only once it has a reviewed, stable interface.

The choice between these models affects the access request: model A needs a principal and
keytab usable by Jenkins, while model B needs the signing pipeline's service account and
principal plus Jenkins credentials for pipeline triggering. In either case, ART must obtain
signing permissions for the principal that actually contacts the signing server.

### Approval and Access Are Phase Zero

No technical implementation should start before access approval. The first action is to open a
Jira request in the `SIGNSERVER` project asking for:

- A service account for ART with its own Kerberos principal and keytab.
- Separate credentials where required for stage, production, or other environments.
- Permission to perform ART's signing operations on the required signing keys.
- The supported direct-signing interface, authentication requirements, and onboarding steps.
- Network approval for the fixed Jenkins/ITUP egress IP range used by promote jobs, or for the
  signing-repository pipeline if that pipeline performs the direct call.

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
- Confirm whether the signing team's `rh-signing-client` is the supported direct client for
  ART. Available implementation guidance indicates that RADAS uses it internally to write an
  input artifact, invoke signing, and read the resulting signature, but the binary, package,
  options, and supported signing modes must be confirmed with the signing team. Do not confuse
  it with any similarly named RHOAI/TAS client.

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

## Jenkins Credential and Kerberos Lifecycle

The direct-signing implementation runs inside the Jenkins promote job on a long-lived agent.
The keytab must therefore be treated as a short-lived Jenkins build credential, not as a
permanent file on the agent.

For each signing session:

1. Materialize the environment-specific signing keytab through a Jenkins secret-file
   credential or an equivalent approved secret manager.
2. Create a unique credential cache, for example
   `FILE:/tmp/krb5cc-art-signing-<build-id>`.
3. Run a second `kinit` with the dedicated principal and keytab, explicitly targeting that
   cache; do not reuse the default cache initialized by `buildlib.groovy`.
4. Invoke the direct signing client with `KRB5CCNAME` pointing to the cache. All concurrent
   artifacts in that signing session may share the same cache.
5. In a `finally`/cleanup block, run `kdestroy` for that cache and remove the temporary keytab
   and cache files.

The signing ccache should not replace or mutate the default Kerberos cache used by unrelated
Jenkins work. The existing `DISTGIT_KEYTAB_*` settings and the
`synced-exd-ocp-buildvm-bot-prod-keytab` Kubernetes secret are for other ART operations and
should remain separate. The current UMB signing credentials (`SIGNING_CERT` and `SIGNING_KEY`)
also remain unchanged while the fallback path is supported.

If direct signing uses a signing-repository Tekton pipeline instead, the keytab/ccache lifecycle
belongs inside that signing pipeline, while Jenkins receives only trigger/observe credentials.
The Jenkins-specific lifecycle above applies only when ART invokes the direct client itself.

## Proposed ART Design

The implementation should keep the existing signing boundary stable while replacing the
RADAS/UMB mechanism behind it:

- Introduce or refactor a `DirectSignatory` that exposes the existing
  `sign_json_digest` and `sign_message_digest` methods.
- Keep callers in `promote.py` and `sync_rhcos.py` unchanged where possible.
- If the approved entry point is a Tekton pipeline, create and monitor the relevant TaskRun or
  PipelineRun and retrieve or publish the resulting signature files according to the signing
  team's contract.
- If the approved entry point is a direct client API or CLI, isolate request construction,
  credential handling, response validation, and error translation in the signatory
  implementation. If the signing team confirms `rh-signing-client`, prefer invoking that
  supported client rather than reimplementing its HTTP/Kerberos protocol.
- Do not embed credentials in source or pipeline arguments. Use the approved Jenkins secret
  credential or secret-manager integration for each environment.
- Add a configurable response timeout, with a default of 10 minutes unless the signing team
  specifies a different limit.
- Ensure every direct signing subprocess inherits the signing session's `KRB5CCNAME`, while
  unrelated Jenkins processes do not.
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
| **Signing client/CLI** | ART invokes the signing team's supported direct client from the Jenkins promote job on buildvm, using an isolated Kerberos ccache. | **Selected for promote** if `rh-signing-client` or an equivalent supported tool is available on buildvm. |
| **Signing-repository pipeline** | ART triggers an approved Tekton pipeline and waits for the signed output. | Not selected for buildvm promote; use only if the signing team requires it or the direct client is unavailable. |
| **Direct API client** | ART calls the approved signing interface using the signing repository's code as reference. | Use only if the signing team supports this integration and no maintained client/CLI is available. |
| **Kafka through Messaging Bridge** | ART replaces STOMP with Kafka while RADAS remains behind the bridge. | Contingency only; requires continued RADAS dependency and a later migration. |

## Rollout Plan

### Phase 0 — Signing Server Approval and Access

- File the `SIGNSERVER` Jira request, using `SIGNSERVER-2309` as a reference if accessible.
- Request ART's service account, Kerberos principal, keytab, environment credentials, and key
  permissions.
- Start the ProdSec/network approval through `#help-signing-server` and provide the fixed
  Jenkins/ITUP egress IP range.
- Confirm the supported direct interface and the signing team's operational requirements.
- If approval or provisioning cannot complete before the September deadline, request an UMB
  extension immediately rather than assuming the RADAS extension applies to ART.

### Phase 1 — Direct-Signing Discovery

- Inspect the signing repository's pipelines, signing code, and verification code.
- Evaluate the generic-signing MR !92 and its expected contract.
- Confirm whether the supported `rh-signing-client` can run on the Jenkins agent and document
  its exact package, arguments, signing modes, and output behavior.
- Compare the client/CLI, pipeline-trigger, and direct-client modes using ART's two signing use
  cases.
- Validate the request/response format and signature verification in a non-production
  environment.

### Phase 2 — Implementation

- Implement `DirectSignatory` or the equivalent direct integration.
- Preserve the existing signatory methods and publishing behavior.
- Add the Jenkins keytab injection, isolated ccache, `kinit`, `KRB5CCNAME`, and cleanup lifecycle
  around the direct signing session.
- Add the temporary direct/UMB rollout switch if needed.
- Add timeout, retry, authentication, and error handling required by the approved interface.
- Add unit tests and docstrings before stage validation.

### Phase 3 — Stage Validation

- Run `promote.py` with direct signing in stage.
- Run `sync_rhcos.py` message-digest signing in stage.
- Verify that produced signatures are valid and published to the expected locations.
- Exercise signing failures, timeouts, invalid responses, credential failures, and retry
  behavior.
- Verify that the keytab is not left on the agent and that the signing ccache is destroyed after
  the job, including failure and cancellation paths.

### Phase 4 — Production Cutover

- Enable direct signing in the production Jenkins configuration, or in the approved
  signing-repository pipeline configuration if that pipeline performs the direct call.
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
- Mock or test the Kerberos lifecycle: explicit ccache selection, inherited `KRB5CCNAME`, and
  cleanup on success and failure.
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
