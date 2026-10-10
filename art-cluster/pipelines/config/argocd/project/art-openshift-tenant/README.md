# OpenShift assembly promotion

`promote-assembly` runs `artcd promote`, ported from
[the Jenkins job](https://github.com/openshift-eng/aos-cd-jobs/blob/master/jobs/build/promote-assembly/Jenkinsfile).
It exposes the release options, selects stage signing for dry runs and production
signing otherwise, and supplies the email configuration used by promotion.
`dry-run` defaults to `true`. `no-multi` and `multi-only` are mutually exclusive.
Parameters are passed as arguments to the Python wrapper.

The shared `artcd` Task mounts promotion secrets optionally, and the wrapper
reports missing credentials before invoking promotion. Supply the credentials
below before running this pipeline. Jenkins credentials are needed for RHCOS
sync even when `skip-build-microshift` is enabled.

## Secret setup

Existing tenant secrets supply Slack, Jira, Redis, GitHub, GitLab, Kerberos, and
Quay auth. `01_externalsecrets.yaml` additionally reuses the release-dev Quay
publishing credentials from `art-cd`, using the existing AWS path
`art/prod/openshift-release-dev+art_quay_dev@quay.io-dockerconfigjson-plaintext`.
The wrapper derives `QUAY_USERNAME` and `QUAY_PASSWORD` from this Docker config.

The `bugzilla-apikey` ExternalSecret reads the complete INI configuration from
`art/prod/konflux/bugzilla-apikey` into its `bugzillarc` key. The shared `artcd`
Task mounts this file read-only at `/etc/bugzillarc`, which python-bugzilla reads
when Elliott checks blocker bugs, including during dry runs. The AWS secret must
contain a `[bugzilla.redhat.com]` section with a nonempty `api_key`. Verify that
this ExternalSecret is Ready before starting an `artcd` TaskRun.

`01_externalsecrets.yaml` syncs `jenkins-credentials` from
`art/prod/jenkins/jenkins-service-account` and
`art/prod/jenkins/jenkins-service-account-token`, mapping them to
`jenkins-service-account` and `jenkins-token` respectively.

The `promote-credentials` ExternalSecret syncs each credential below from
`art/prod/jenkins/<Jenkins credential ID>`. Each source is read as a complete
secret value, matching the existing Jenkins credential mappings in this tenant.
Check that the promotion ExternalSecrets are Ready after GitOps sync before
running promotion.

| Secret key | Jenkins credential / purpose |
| --- | --- |
| `app-ci-kubeconfig` | `art-publish.app.ci.kubeconfig`: app.ci payload publishing and registry login |
| `art-cd-kubeconfig` | `art-cluster-art-cd-pipeline-kubeconfig`: start the doomsday pipeline in `art-cd` |
| `aws-credentials` | `aws-credentials-file`: mirror publishing; include default and cloudflare profiles |
| `cloudflare-endpoint` | `s3-art-srv-enterprise-cloudflare-endpoint` |
| `signing-prod.crt` | `0xffe138e-openshift-art-bot.crt` |
| `signing-prod.key` | `0xffe138e-openshift-art-bot.key` |
| `signing-stage.crt` | `0xffe138d-nonprod-openshift-art-bot.crt` |
| `signing-stage.key` | `0xffe138d-nonprod-openshift-art-bot.key` |
| `kms-prod-credentials` | `kms_prod_release_signing_creds_file` |
| `kms-prod-key-id` | `kms_prod_release_signing_key_id` |
| `kms-stage-credentials` | `kms_stage_release_signing_creds_file` |
| `kms-stage-key-id` | `kms_stage_release_signing_key_id` |
| `rekor-url` | `signing_rekor_url` |

Only the selected signing environment is required. Legacy signing credentials
are unnecessary with `skip-signing=true`; KMS and Rekor credentials are
unnecessary with `skip-sigstore=true`. Mirror credentials are unnecessary only
when both `skip-mirror-binaries` and `skip-signing` are true. The ART kubeconfig
is required for production assemblies that trigger doomsday backup.

## Google Cloud signature publishing

Signature publishing uses
`openshift-art-mirror-publish-b@openshift-release.iam.gserviceaccount.com`.
The existing `GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-sa/sa.json` supplies
BigQuery access. `gsutil` reads the publishing credential through a separate
Boto config, while Jenkins uses its existing VM Cloud SDK configuration.

The `gcs-publish-credentials` ExternalSecret reads the complete service-account
JSON from AWS Secrets Manager at
`art/prod/jenkins/openshift-art-mirror-publish-b`. It preserves the JSON as
`gcs-publish-adc.json` and adds `gcs-publish.boto`, which points to the mounted
JSON at `/tmp/promote-credentials/gcs-publish-adc.json`. The shared `artcd`
Task projects both files read-only into `/tmp/promote-credentials`.
The promotion wrapper sets `BOTO_CONFIG` to the mounted config and validates
both files when `dry-run=false` and `skip-signing=false`.

1. On the Jenkins VM, select AWS credentials for the ART account used by
   `main-secret-store`, then upload the existing publishing key. Use the file
   directly so its contents are kept out of command arguments and output:

   ```bash
   aws secretsmanager create-secret \
     --region us-east-1 \
     --name art/prod/jenkins/openshift-art-mirror-publish-b \
     --description "OpenShift release GCS signature publishing service account" \
     --secret-string file:///home/jenkins/.config/gcloud/legacy_credentials/openshift-art-mirror-publish-b@openshift-release.iam.gserviceaccount.com/adc.json \
     --query ARN --output text
   ```

   If this secret already exists, update it with `put-secret-value`, using
   `--secret-id` in place of `--name` and omitting `--description`.
   Ensure `main-secret-store` can read this secret. Its value is the original
   JSON document; no extra JSON property or base64 encoding is needed.

2. Merge the [gsutil installation change](https://github.com/openshift-eng/art-tools/pull/3671)
   and this credential change, then sync the `art-openshift-tenant` Argo CD application.
   Wait for the publishing ExternalSecret to become Ready:

   ```bash
   oc -n art-openshift-tenant wait externalsecret/gcs-publish-credentials \
     --for=condition=Ready --timeout=120s
   ```

3. Rebuild `art-cd:base` from the merged code to install `gsutil`:

   ```bash
   oc -n art-cd start-build art-cd-base --follow
   ```

   Its image change triggers `art-cd-update`, which publishes the new
   `quay.io/redhat-user-workloads/ocp-art-tenant/art-cd:latest`. Wait for that
   build to complete. If it does not trigger, run
   `oc -n art-cd start-build art-cd-update --follow`.

4. In a new task using the updated image and publishing mount, verify
   `gsutil version -l` and the configured Boto path. Verify the mounted JSON's
   `client_email` matches the publishing account without printing the key.
   Validate an upload of a disposable file to an agreed nonproduction prefix;
   listing a public bucket or running `artcd --dry-run` does not establish
   authenticated write access.

5. Start a new `promote-assembly` run after secret and image rollout. Confirm
   the GCS signature copies complete. Setting `art-tools-commit` alone
   does not install the missing `gsutil` executable in an old image.

## Testing plan

1. Parse all tenant YAML and compile embedded Python. Check parameter forwarding,
   flag mapping, signing selection, conflicting multi options, and missing secret
   errors with subprocess calls mocked.
2. Validate the manifests against the cluster using server dry-run.
3. After secret sync, verify each ExternalSecret is Ready and all required secret
   keys exist. Ensure the runtime image includes this change's doomsday dry-run
   guard, or set `art-tools-commit` to its branch.
4. Run `promote-assembly` against a test assembly with `dry-run=true`; inspect the
   TaskRun logs and stage signing behavior before a production promotion.

Promotion still uses Jenkins for downstream MicroShift and RHCOS jobs. Email
delivery requires connectivity to `smtp.corp.redhat.com`, and legacy signing
requires access to its signing service. TaskRun logs provide the console output;
Jenkins workspace artifact archiving is not configured here.
