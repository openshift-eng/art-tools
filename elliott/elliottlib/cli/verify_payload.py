import json
import logging
from typing import Dict, List, Optional

import click
from artcommonlib import rhcos
from artcommonlib.assembly import assembly_config_struct

from elliottlib import Runtime
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.constants import errata_url
from elliottlib.errata import get_advisory_nvrs, get_brew_build, get_raw_erratum
from elliottlib.util import get_nvrs_from_release


class VerifyPayloadPipeline:
    def __init__(self, runtime: Runtime, payload_or_imagestream: str, to_file: bool = False):
        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.payload_or_imagestream = payload_or_imagestream
        self.to_file = to_file

        self.all_payload_nvrs: Dict[str, tuple] = {}
        self.all_advisory_nvrs = {}

    async def run(self):
        rhcos_images = {c['name'] for c in rhcos.get_container_configs(self.runtime)}
        self.all_payload_nvrs = await get_nvrs_from_release(self.payload_or_imagestream, rhcos_images, self.logger)

        if self.runtime.build_system == 'brew':
            results = await self.check_brew_payload()
        else:
            results = await self.check_konflux_payload()

        self.logger.info("Summary results:")
        click.echo(json.dumps(results, indent=4))

        if self.to_file:
            with open('summary_results.json', 'w') as fp:
                json.dump(results, fp, indent=4)
            self.logger.info("Wrote out summary results to summary_results.json")

    def _get_image_advisory_id(self):
        """
        Get the image advisory ID from the assembly definition.
        """

        assembly_group_config = assembly_config_struct(
            self.runtime.get_releases_config(), self.runtime.assembly, "group", {}
        )
        return assembly_group_config.get('advisories', {}).get('image', None)

    async def check_brew_payload(self):
        # Get the advisory ID from the assembly definition
        advisory = self._get_image_advisory_id()
        if not advisory:
            raise click.UsageError("No image advisory ID found in assembly definition.")

        self.all_advisory_nvrs = get_advisory_nvrs(advisory)
        self.logger.info("Found {} builds".format(len(self.all_advisory_nvrs)))

        missing_in_errata = {}
        payload_doesnt_match_errata = {}
        in_pending_advisory = []
        in_shipped_advisory = []
        results = {
            'missing_in_advisory': missing_in_errata,
            'payload_advisory_mismatch': payload_doesnt_match_errata,
            "in_pending_advisory": in_pending_advisory,
            "in_shipped_advisory": in_shipped_advisory,
        }

        self.logger.info("Analyzing %s images to consider from payload", len(self.all_payload_nvrs))

        self._check_payload_match_errata(payload_doesnt_match_errata, missing_in_errata)
        self._check_missing_in_errata(missing_in_errata, in_shipped_advisory, in_pending_advisory)

        return results

    def _check_payload_match_errata(self, payload_doesnt_match_errata, missing_in_errata):
        for image, vr_tuple in self.all_payload_nvrs.items():
            vr = f"{vr_tuple[0]}-{vr_tuple[1]}"
            imagevr = f"{image}-{vr}"
            self.logger.info("Cross-checking from payload: %s", imagevr)

            if image not in self.all_advisory_nvrs:
                missing_in_errata[image] = imagevr
                self.logger.warning(f"{imagevr} in payload not found in advisory")

            elif image in self.all_advisory_nvrs and vr != self.all_advisory_nvrs[image]:
                self.logger.warning(
                    f"{image} from payload has version {vr} which does not match {self.all_advisory_nvrs[image]} from advisory"
                )
                payload_doesnt_match_errata[image] = {
                    'payload': vr,
                    'errata': self.all_advisory_nvrs[image],
                }

    def _check_missing_in_errata(self, missing_in_errata, in_shipped_advisory, in_pending_advisory):
        if missing_in_errata:  # check if missing images are already shipped or pending to ship
            advisory_nvrs: Dict[int, List[str]] = {}  # a dict mapping advisory numbers to lists of NVRs
            self.logger.info(f"Checking if {len(missing_in_errata)} missing images are shipped...")

            for nvr in missing_in_errata.copy().values():
                # get the list of advisories that this build has been attached to
                build = get_brew_build(nvr)

                # filter out dropped advisories
                advisories = [ad for ad in build.all_errata if ad["status"] != "DROPPED_NO_SHIP"]
                if not advisories:
                    self.logger.warning(f"Build {nvr} is not attached to any advisories.")
                    continue

                for advisory in advisories:
                    if advisory["status"] == "SHIPPED_LIVE":
                        self.logger.info(f"Missing build {nvr} has been shipped with advisory {advisory}.")
                    else:
                        self.logger.warning(f"Missing build {nvr} is in another pending advisory.")
                    advisory_nvrs.setdefault(advisory["id"], []).append(nvr)

                name = nvr.rsplit("-", 2)[0]
                del missing_in_errata[name]

            if advisory_nvrs:
                self.logger.info(f"Getting information of {len(advisory_nvrs)} advisories...")
                for advisory, nvrs in advisory_nvrs.items():
                    advisory_obj = get_raw_erratum(advisory)
                    adv_type, adv_info = next(iter(advisory_obj["errata"].items()))
                    item = {
                        "id": advisory,
                        "type": adv_type.upper(),
                        "url": errata_url + f"/{advisory}",
                        "summary": adv_info["synopsis"],
                        "state": adv_info["status"],
                        "nvrs": nvrs,
                    }
                    if adv_info["status"] == "SHIPPED_LIVE":
                        in_shipped_advisory.append(item)
                    else:
                        in_pending_advisory.append(item)

    async def check_konflux_payload(self):
        return {}


@cli.command("verify-payload", short_help="Verify payload contents match advisory builds")
@click.argument("payload_or_imagestream")
@click.option('--to-file', default=False, is_flag=True, help='Write results to file.')
@click.pass_obj
@click_coroutine
async def verify_payload(runtime, payload_or_imagestream, to_file):
    """Cross-check that the builds present in PAYLOAD or Imagestream match the builds
    attached to ADVISORY. The payload is treated as the source of
    truth. If something is absent or different in the advisory it is
    treated as an error with the advisory.

    \b
        PAYLOAD_OR_IMAGESTREAM - Full pullspec of the payload or imagestream to verify
        ADVISORY - Numerical ID of the advisory

    Two checks are made:

    \b
     1. Missing in Advisory - No payload/imagestream components are absent from the given advisory

     2. Payload/imagestream Advisory Mismatch - The version-release of each payload/imagestream item match what is in the advisory

    Results are summarily printed at the end of the run. They are also
    written out to summary_results.json.

         Verify builds in the given payload/imagestream match the builds attached to advisory 41567

     \b
        $ for paylaod: elliott -g openshift-1 verify-payload quay.io/openshift-release-dev/ocp-release:4.1.0-rc.6 41567
     \b
        $ for imagestream: elliott -g openshift-1 verify-payload 4.1-art-assembly-rc.6 41567

    """

    runtime.initialize()
    await VerifyPayloadPipeline(runtime, payload_or_imagestream, to_file).run()
