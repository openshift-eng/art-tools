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

`01_externalsecrets.yaml` syncs `jenkins-credentials` from
`art/prod/jenkins/jenkins-service-account` and
`art/prod/jenkins/jenkins-service-account-token`, mapping them to
`jenkins-service-account` and `jenkins-token` respectively.

The `promote-credentials` ExternalSecret syncs each credential below from
`art/prod/jenkins/<Jenkins credential ID>`. Each source is read as a complete
secret value, matching the existing Jenkins credential mappings in this tenant.
Check that both new ExternalSecrets are Ready after GitOps sync before running
promotion.

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
