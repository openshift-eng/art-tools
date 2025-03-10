from typing import Optional, Sequence

from pyartcd.cli import cli
from pyartcd.pipelines import (
    build_microshift_bootc, build_microshift, check_bugs, gen_assembly, ocp4_konflux, prepare_release, promote, rebuild,
    review_cvp, tarball_sources, build_sync, build_rhcos, ocp4_scan, ocp4_scan_konflux, images_health,
    operator_sdk_sync, olm_bundle, ocp4, scan_for_kernel_bugs, tag_rpms, advisory_drop, cleanup_locks, brew_scan_osh,
    sigstore_sign, update_golang, rebuild_golang_rpms, scan_fips, quay_doomsday_backup,
    olm_bundle_konflux, build_fbc
)
from pyartcd.pipelines.scheduled import schedule_ocp4_scan, schedule_ocp4_scan_konflux


def main(args: Optional[Sequence[str]] = None):
    # pylint: disable=no-value-for-parameter
    cli()


if __name__ == "__main__":
    main()
