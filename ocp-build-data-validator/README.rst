ocp-build-data-validator
========================

.. image:: https://badge.fury.io/py/rh-ocp-build-data-validator.svg
  :target: https://badge.fury.io/py/rh-ocp-build-data-validator
.. image:: https://codecov.io/gh/openshift/ocp-build-data-validator/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/openshift/ocp-build-data-validator

Validation of `ocp-build-data <https://gitlab.cee.redhat.com/openshift-art/ocp-build-data>`__
Image & RPM declarations

Installation
------------

::

    $ pip install rh-ocp-build-data-validator

Usage
-----

Validating a single file:

::

    $ validate-ocp-build-data path/to/ocp-build-data/images/or/rpms.yml

Validating the whole `ocp-build-data <https://gitlab.cee.redhat.com/openshift-art/ocp-build-data>`__
repository:

::

    $ validate-ocp-build-data path/to/ocp-build/data/{images,rpms}/*

Validations
-----------

-  YAML Format
-  YAML Schema (supported schemas: **Image** and **RPM**)
-  Presence of corresponding `DistGit <http://pkgs.devel.redhat.com>`__ repository
   (needs to match YAML filename)

   -  Presence of corresponding **branch** on `DistGit <http://pkgs.devel.redhat.com>`__

-  Presence of `GitHub <https://github.com>`__ repository (if declared)

   -  Presence of **branch** on `GitHub <https://github.com>`__ (if declared)
   -  Presence of **dockerfile** on `GitHub <https://github.com>`__ (if declared)
   -  Presence of **manifests-dir** on `GitHub <https://github.com>`__ (if declared)

-  Stream is supported (if declared, value must match one of the entries in ``streams.yml``)
-  Member exists (if declared, member must be another existing declaration under ``images/``)
-  Parent ``from.member`` references must not form cycles or self-references,
   matching Doozer's existing ancestry validation. Group variables are resolved
   before checking; WIP/disabled images do not introduce blocking cycles.
-  Go major/minor versions selected by ``from.builder`` and
   ``content.source.ci_alignment.streams_prs.ci_build_root`` match when both can
   be inferred from metadata. Resolves group variables, stream aliases, and
   member base-image chains; compares pinned builder images with CI upstream
   image tags. No registry queries are made, and unversioned toolchains are skipped.

Production FBC catalogs
-----------------------

The shared ``validator.ci`` entry point checks that the production operator index
``registry.redhat.io/redhat/redhat-operator-index:v<major>.<minor>`` exists for
every distinct ``OCP_TARGET_VERSIONS`` entry in ``group.yml``. This matches the
production index prerequisite in ``artcd build-fbc``. It inspects manifests with
``skopeo inspect --raw`` without downloading image layers or requiring a local
architecture match. Install ``skopeo`` and pass a Docker/Podman pull secret with
``--registry-auth-file /path/to/auth.json`` when running locally. Without this
option, Skopeo uses its normal credential lookup.

Missing manifests fail validation. Authentication failures, network failures and
timeouts also fail, with distinct diagnostics rather than an assertion that a
catalog is missing. Groups without target versions make no registry requests.
``--schema-only`` remains offline and skips this check. Catalog availability does
not establish that an operator's package or bundle is already in the catalog.

Artifact lockfile resources
---------------------------

Image metadata under ``konflux.cachi2.artifact_lockfile.resources`` accepts
generic URL resources and Maven resources. Maven resources use the Hermeto
generic-lockfile fields ``type: maven`` and ``attributes`` containing
``repository_url``, ``group_id``, ``artifact_id``, and ``version``. An optional
``filename`` preserves the path expected by the image Dockerfile.

Shared CI RPA validation
------------------------

From a trusted art-tools checkout, validate a build-data release branch::

    uv run python -I -m validator.ci \
      --repo /path/to/build-data-release-branch \
      --rpa-root /path/to/konflux-release-data/config \
      --application <application>

This checks delivery repositories against stage and production
ReleasePlanAdmissions. It ports the repository/component presence
contract from the existing `OCP delivery-repos Prow step
<https://github.com/openshift/release/blob/38a9915664e8c834b568f855e7e30072ad71b210/ci-operator/step-registry/ocp-art/validate/delivery-repos/ocp-art-validate-delivery-repos-commands.sh>`__.

Both environments must contain every ``delivery.delivery_repo_names`` entry,
any ``bundle_delivery_repo_name``, and the expected image component. Registry
hosts are removed before comparing repository paths. Repository presence and
component presence are independent: the original check neither requires a
repository to belong to that particular component nor requires a separate bundle
component. Disabled images and ``for_release: false`` images are excluded.
Even without delivery repositories, a release image still needs its component.

The shared entry point adds application-based RPA discovery for LPs, validates
malformed RPA mappings, honors explicit Doozer component-name overrides, and
excludes WIP images consistently with its non-blocking WIP policy. These are
explicit extensions to the OCP step, not behavior copied from its shell script.
The original OCP step is unchanged; migrating its callers is separate work.

Public source checks use anonymous GitHub web URLs, following the existing OCP
validator; the Prow job does not require a GitHub token. Optional
``--github-token-file`` authentication remains available for local validation of
truly private sources, using the GitHub API. Missing or inaccessible sources are
reported as errors rather than silently skipped.

Source validation uses the most specific ``group.yml`` ``public_upstreams``
mapping in place of the private mirror, including any ``public_branch`` override.
Without a mapping it checks the declared source. An explicit ``url_pull`` fork
remains the actual source to validate. The token-free presubmit does not separately
verify private mirrors.
