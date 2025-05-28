from typing import Optional, Sequence

from pyartcd.cli import cli
from pyartcd.pipelines import (
    advisory_drop,
    brew_scan_osh,
    build_fbc,
    build_microshift,
    build_microshift_bootc,
    build_rhcos,
    build_sync,
    check_bugs,
    cleanup_locks,
    fbc_import_from_index,
    gen_assembly,
    images_health,
    ocp4,
    ocp4_konflux,
    ocp4_scan,
    ocp4_scan_konflux,
    olm_bundle,
    olm_bundle_konflux,
    operator_sdk_sync,
    prepare_release,
    prepare_release_konflux,
    promote,
    quay_doomsday_backup,
    rebuild,
    rebuild_golang_rpms,
    review_cvp,
    scan_fips,
    scan_for_kernel_bugs,
    sigstore_sign,
    tag_rpms,
    tarball_sources,
    update_golang,
)
from pyartcd.pipelines.scheduled import schedule_ocp4_scan, schedule_ocp4_scan_konflux


def main(args: Optional[Sequence[str]] = None):
    # pylint: disable=no-value-for-parameter
    cli()


if __name__ == "__main__":
    main()
