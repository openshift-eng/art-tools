# Artcommon

Common package used by Doozer, Elliott, pyartcd

## ART PipelineRun component reporting

`artcommonlib.pipeline_report` publishes version 1 JSON component build annotations only when `TASKRUN_NAME`, `TASKRUN_NAMESPACE`, and `TEKTON_PIPELINERUN_NAME` identify an ART Tekton invocation. It uses the ART cluster's `oc` credentials; Konflux clients retain their separate credentials. Reporting errors are nonfatal, with bounded command timeouts and a retry cooldown.

Each component uses `art.openshift.io/component-build-<sha256(type:name)[:20]>`. Its report contains `version`, `type` (`image`, `bundle`, `fbc`), `name` (distgit key), `displayName`, `status`, `reason`, `pipelineRuns`, and `updatedAt`. Each build attempt records its name, namespace, URL, attempt number, native build status, and reason. The latest native build attempt determines the component's build result; later EC, ITS, or release failures do not alter that result and are not reported here. Preparation failures before a build are Failed with no PipelineRun link. Reused, skipped, and dry-run builds remain distinct from successful builds triggered by this ART run.

Parallel components patch separate annotation keys. `art.openshift.io/component-report` records schema version, partial reporting, and reasons for skips or capacity limits. Reports reserve annotation space and stop publishing oversized updates while retaining earlier links. The UI treats unfinished build results as Unknown after ART completion.

Testing plan: run the shared reporter and affected Doozer/pyartcd suites plus the UI suites in its separate PR. After deployment, inspect fresh successful and failed image, bundle, and FBC builds, all retry links, preparation failures, reused/skipped builds, dry runs, archived runs, rebuild isolation, and reporting/capacity failures. Confirm post-build EC, ITS, or release failures leave a successful native build result intact. Existing runs are not backfilled.
