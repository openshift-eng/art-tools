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
ReleasePlanAdmissions for the selected application. In both environments, the
expected image component must contain every ``delivery.delivery_repo_names``
entry in its own repository mapping. If ``bundle_delivery_repo_name`` is set,
the expected bundle component must exist and contain that repository in its own
mapping. A repository listed under another component does not satisfy the check.
Registry hosts are removed before comparing repository paths. Image and bundle
component names honor Doozer's explicit overrides.

Disabled images, WIP images and ``for_release: false`` images are excluded from
RPA checks. The shared entry point discovers LP RPAs by application and reports
malformed mappings. The original OCP shell step is unchanged; migrating its
callers is separate work.

Source validation checks both ``content.source.git.url`` (the midstream) and
``content.source.git.web`` (the public upstream). When ``web`` is absent, the
most specific ``group.yml`` ``public_upstreams`` mapping supplies the public
repository. A mapping never replaces the midstream check. Its ``public_branch``
override applies only to the public upstream; otherwise each repository must
have a usable target or fallback branch independently. Both sources must contain
the configured Dockerfile (``Dockerfile`` by default) and any manifests directory.
An explicit ``url_pull`` fork is also checked without exempting either declared
source. Identical repository/branch checks are deduplicated.

Public sources can be checked anonymously using GitHub web URLs. Private
midstreams require ``--github-token-file /path/to/token`` with read access to
those repositories; authenticated checks use the GitHub API. CI callers must
mount an appropriate credential and pass this option before adopting these
checks for private sources. Missing or inaccessible sources fail validation;
a public upstream cannot hide an inaccessible midstream. ``--schema-only``
continues to skip remote source checks.
