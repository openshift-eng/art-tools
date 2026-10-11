# Artcommon

Common package used by Doozer, Elliott, pyartcd

## ART PipelineRun component reporting

`artcommonlib.pipeline_report` publishes version 1 JSON component annotations only when `TASKRUN_NAME`, `TASKRUN_NAMESPACE`, and `TEKTON_PIPELINERUN_NAME` identify an ART Tekton invocation. It uses the ART cluster's `oc` credentials; Konflux clients retain their separate credentials. Reporting errors are nonfatal, with bounded command timeouts and a retry cooldown.

Each component uses `art.openshift.io/component-build-<sha256(type:name)[:20]>`. Its report contains `version`, `type` (`image`, `bundle`, `fbc`), `name` (distgit key), `displayName`, `stages`, `resources`, and optional NVRs and release snapshot members. Stages carry `category`, `status`, `required`, `reason`, and `updatedAt`; resource records carry kind, namespace, name, UID when observed, role, URL, attempt, status, and available reasons/timestamps. Parallel components patch separate keys. A subsequent Doozer invocation loads the existing report before adding bundle stage-release results.

`art.openshift.io/component-report` records schema version, partial reporting, and reasons for skips or capacity limits. Reports reserve annotation space and stop publishing oversized updates while retaining earlier links. The UI treats unfinished stages as Unknown after ART completion. Release PipelineRuns discovered through release status use their actual namespaces; legacy completion timestamps do not establish a successful PipelineRun result. A successful build and failed required delivery remain separate stages. Optional standalone stage releases retain their existing nonfatal behavior.

Testing plan: run `artcommon/tests/test_pipeline_report.py`, the affected Doozer backend/CLI and pyartcd pipeline suites, and the UI suites in its separate PR. After deploying both changes, exercise fresh successful and failed runs of the three pipelines, inspect updates while active and in Tekton Results, verify all retry and check links, bundle stage failure/FBC exclusion, reused builds, dry runs, rebuild isolation, reporting failure, and annotation capacity warnings. Existing runs are not backfilled.
