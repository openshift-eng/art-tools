import json
import logging
from dataclasses import dataclass, field
from io import StringIO
from typing import Optional

import click
import yaml
from artcommonlib.exectools import cmd_gather_async

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.verify_common import (
    VerifyResultBase,
    handle_verify_result,
    verify_output_option,
)

LOGGER = logging.getLogger(__name__)

STAGE_FBC_REPO = "quay.io/openshift-art/stage-fbc-fragments"

# Known CSV version overrides: operators whose CSV version intentionally
# differs from the OCP version for certain releases.
# Format: {package_name: {ocp_version: csv_version_prefix}}
CSV_VERSION_OVERRIDES: dict[str, dict[str, str]] = {
    # Branches 4.12-4.17 transitioned to OLM catalog version 4.18
    # https://github.com/openshift/local-storage-operator
    "local-storage-operator": {f"4.{minor}": "4.18" for minor in range(12, 18)},
    # Builds from release-4.18 branch for OCP 4.16-4.17
    # https://github.com/openshift/csi-operator (Dockerfile.samba)
    "smb-csi-driver-operator": {f"4.{minor}": "4.18" for minor in range(16, 18)},
}


@dataclass
class OperatorCsvResult:
    package: str
    channel: str
    csv_version: str
    match: bool
    override: Optional[str] = None


@dataclass
class VerifyCsvVersionsResult(VerifyResultBase):
    expected_version: str = ""
    catalog_image: str = ""
    operators: list[OperatorCsvResult] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return not self.error and bool(self.operators) and all(o.match for o in self.operators)

    @property
    def mismatches(self) -> list[OperatorCsvResult]:
        return [o for o in self.operators if not o.match]

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "expected_version": self.expected_version,
            "catalog_image": self.catalog_image,
            "error": self.error,
            "summary": {
                "total": len(self.operators),
                "match": sum(1 for o in self.operators if o.match),
                "mismatch": len(self.mismatches),
            },
            "operators": [
                {
                    "package": o.package,
                    "channel": o.channel,
                    "csv_version": o.csv_version,
                    "match": o.match,
                    **({"override": o.override} if o.override else {}),
                }
                for o in self.operators
            ],
        }

    def render_text(self) -> str:
        lines = [
            f"CSV version check for: {self.catalog_image}",
            f"Expected OCP version prefix: {self.expected_version}.",
            "",
        ]

        if self.error:
            lines.append(f"ERROR: {self.error}")
            return "\n".join(lines)

        lines.append(f"{'PACKAGE':<45} {'CSV VERSION':<35} STATUS")
        lines.append(f"{'-------':<45} {'-----------':<35} ------")

        for o in self.operators:
            if o.match and o.override:
                status = f"MATCH (override: {o.override})"
            elif o.match:
                status = "MATCH"
            else:
                status = "MISMATCH"
            lines.append(f"{o.package:<45} {o.csv_version:<35} {status}")

        matches = sum(1 for o in self.operators if o.match)
        mismatches = len(self.mismatches)
        lines.append("")
        lines.append(f"Results: {matches} match, {mismatches} mismatch (total: {len(self.operators)})")

        if not self.passed:
            lines.append("")
            if not self.operators:
                lines.append("FAILED: No operators found in catalog")
            else:
                lines.append(
                    f"FAILED: {mismatches} operator(s) have CSV versions that do not match OCP {self.expected_version}"
                )
        else:
            lines.append("")
            lines.append(f"PASSED: All operator CSV versions match OCP {self.expected_version}")

        return "\n".join(lines)


def _find_head_bundle(channel_entries: list[dict]) -> Optional[str]:
    """Find the head (latest) bundle in a channel — the entry not replaced by any other.

    Only considers replaces/skips edges; skipRange is intentionally excluded
    because it does not define discrete replacement relationships.
    """
    all_names = {e["name"] for e in channel_entries}
    replaced = set()
    for entry in channel_entries:
        if entry.get("replaces"):
            replaced.add(entry["replaces"])
        for skip in entry.get("skips", []):
            replaced.add(skip)
    heads = all_names - replaced
    if len(heads) == 1:
        return heads.pop()
    if len(heads) > 1:
        LOGGER.warning("Multiple head bundles found: %s — channel graph is ambiguous", sorted(heads))
    return None


async def render_and_check_csv_versions(catalog_image: str, expected_version: str) -> VerifyCsvVersionsResult:
    result = VerifyCsvVersionsResult(
        expected_version=expected_version,
        catalog_image=catalog_image,
    )

    LOGGER.info("Rendering FBC catalog: %s", catalog_image)
    try:
        rc, out, err = await cmd_gather_async(
            ["opm", "render", "-o", "yaml", "--", catalog_image],
            check=False,
        )
        if rc != 0:
            result.error = f"opm render failed (rc={rc}): {err.strip()}"
            return result
    except Exception as e:
        result.error = f"Failed to run opm render: {e}"
        return result

    blobs = list(yaml.safe_load_all(StringIO(out)))
    LOGGER.info("Parsed %d FBC entries", len(blobs))

    packages: dict[str, str] = {}
    channels: dict[tuple[str, str], list[dict]] = {}
    bundle_versions: dict[str, str] = {}

    for blob in blobs:
        schema = blob.get("schema")
        if schema == "olm.package":
            packages[blob["name"]] = blob.get("defaultChannel", "")
        elif schema == "olm.channel":
            key = (blob["package"], blob["name"])
            channels[key] = blob.get("entries", [])
        elif schema == "olm.bundle":
            for prop in blob.get("properties", []):
                if prop.get("type") == "olm.package":
                    value = prop["value"]
                    if isinstance(value, str):
                        value = json.loads(value)
                    bundle_versions[blob["name"]] = value.get("version", "")
                    break

    for pkg_name, default_channel in sorted(packages.items()):
        entries = channels.get((pkg_name, default_channel), [])

        if not entries:
            LOGGER.warning("Package %s: no entries in default channel %s", pkg_name, default_channel)
            result.operators.append(
                OperatorCsvResult(
                    package=pkg_name,
                    channel=default_channel,
                    csv_version="UNRESOLVED (no entries)",
                    match=False,
                )
            )
            continue

        head_bundle = _find_head_bundle(entries)
        if not head_bundle:
            LOGGER.warning("Package %s: could not determine head bundle", pkg_name)
            result.operators.append(
                OperatorCsvResult(
                    package=pkg_name,
                    channel=default_channel,
                    csv_version="UNRESOLVED (ambiguous head)",
                    match=False,
                )
            )
            continue

        csv_version = bundle_versions.get(head_bundle, "unknown")

        override = CSV_VERSION_OVERRIDES.get(pkg_name, {}).get(expected_version)
        effective_version = override or expected_version
        match = csv_version.startswith(f"{effective_version}.")

        result.operators.append(
            OperatorCsvResult(
                package=pkg_name,
                channel=default_channel,
                csv_version=csv_version,
                match=match,
                override=override,
            )
        )

    return result


@cli.command("verify-csv-versions", short_help="Verify CSV versions in FBC catalog match OCP version")
@click.option(
    "--catalog-image",
    default=None,
    help=f"FBC catalog image pullspec. Defaults to {STAGE_FBC_REPO}:ocp-MAJOR.MINOR",
)
@verify_output_option
@click.pass_obj
@click_coroutine
async def verify_csv_versions_cli(runtime, catalog_image, output):
    """Verify that CSV versions of operators in an FBC catalog match the expected OCP version.

    Renders the FBC catalog image with opm, extracts the head CSV version
    for each package's default channel, and checks that it starts with the
    OCP major.minor version derived from --group.

    Example:

    \b
        elliott --group openshift-4.22 verify-csv-versions
        elliott --group openshift-4.22 verify-csv-versions \\
            --catalog-image quay.io/example/my-catalog:v4.22
    """
    runtime.initialize()
    major, minor = runtime.get_major_minor_fields()
    expected_version = f"{major}.{minor}"

    if not catalog_image:
        catalog_image = f"{STAGE_FBC_REPO}:ocp-{expected_version}"

    LOGGER.info("Checking CSV versions in %s against OCP %s", catalog_image, expected_version)
    result = await render_and_check_csv_versions(catalog_image, expected_version)
    handle_verify_result(result, output)
