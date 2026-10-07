# ART Pipelines UI

This application lists ART tenant Pipelines and PipelineRuns from the live OpenShift cluster and Tekton Results. Each Pipeline has a PipelineRuns tab with an exact Pipeline filter. It supports starting a Pipeline and rebuilding an earlier PipelineRun with edited parameters. The rebuild form uses the current Pipeline definition and fills missing run parameters from its current defaults. Run logs come from cluster pods while available and Tekton Results after pruning; HTTP and HTTPS URLs in logs are clickable.

The backend uses the OpenShift OAuth token forwarded by its sidecar. Kubernetes and Tekton Results enforce each user's namespace permissions. The app does not persist run data or user tokens. The OAuth client requests `user:full`, which delegates all of a user's existing RBAC permissions, including secret access if that user has it, to the app. To add a tenant namespace, update `PIPELINE_NAMESPACES` in the GitOps ConfigMap.

## Development

The backend is in `backend/pipeline_ui`; install `backend/requirements.txt` and run `uvicorn pipeline_ui.app:app` from `backend`. The frontend is in `frontend`; run `npm ci` and `npm run build`. Browser requests need an OpenShift OAuth proxy in front of the backend.

## Pilot and promotion

Apply the manifests in `../config/argocd/project/art-pipelines-ui` directly for the pilot, leaving the Deployment until last. Once the Route exists, run `python3 art-cluster/pipelines/ui/bootstrap_oauth.py` from the repository root to create a dedicated OAuth client and namespace Secret. The client requests `user:full`, which a service account OAuth client cannot request. The bootstrap script reuses existing credentials on subsequent runs and keeps them out of Git. Then apply the Deployment. Before enabling the Argo CD application on a new cluster, create the Route and run the same bootstrap script.

The BuildConfig's Git `contextDir` is `art-cluster/pipelines/ui`. To build unmerged local code safely, stage only the `ui` directory under that same relative path in a temporary directory, excluding `node_modules` and `dist`, then run `oc start-build art-pipelines-ui -n art-pipelines-ui --from-dir=<staging-root> --follow`. After the code and manifests are merged into `openshift-eng/art-tools:main`, build from Git with `oc start-build art-pipelines-ui -n art-pipelines-ui --follow` and create the Argo CD application in `../config/argocd/apps/art-pipelines-ui.yaml` to adopt the same resources. The Deployment follows the `art-pipelines-ui:latest` ImageStreamTag; a successful new build updates its image automatically. Argo CD ignores that image field so a sync does not roll it back.

For acceptance, verify that a reader sees only authorized namespaces, a user with PipelineRun create permission can start and rebuild a harmless test Pipeline, and an archived run's logs load after its pod is removed. The `art-rhosdt-tenant` Results API has returned logs for a completed run during development.
