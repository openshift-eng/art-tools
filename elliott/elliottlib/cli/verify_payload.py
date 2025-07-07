import json
import logging

import click
from typing import Dict, List

from artcommonlib import rhcos
from artcommonlib.format_util import green_prefix, yellow_prefix, green_print, red_print, yellow_print
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.constants import errata_url
from elliottlib.errata import get_advisory_nvrs, get_brew_build, get_raw_erratum
from elliottlib.util import get_nvrs_from_release

LOGGER = logging.getLogger(__name__)


@cli.command("verify-payload", short_help="Verify payload contents match advisory builds")
@click.argument("payload_or_imagestream")
@click.argument('advisory', type=int)
@click.pass_obj
@click_coroutine
async def verify_payload(runtime, payload_or_imagestream, advisory):
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
    rhcos_images = {c['name'] for c in rhcos.get_container_configs(runtime)}
    all_advisory_nvrs = get_advisory_nvrs(advisory)

    click.echo("Found {} builds".format(len(all_advisory_nvrs)))

    all_payload_nvrs = await get_nvrs_from_release(payload_or_imagestream, rhcos_images, LOGGER)

    missing_in_errata = {}
    payload_doesnt_match_errata = {}
    in_pending_advisory = []
    in_shipped_advisory = []
    output = {
        'missing_in_advisory': missing_in_errata,
        'payload_advisory_mismatch': payload_doesnt_match_errata,
        "in_pending_advisory": in_pending_advisory,
        "in_shipped_advisory": in_shipped_advisory,
    }

    green_prefix("Analyzing data: ")
    click.echo("{} images to consider from payload".format(len(all_payload_nvrs)))

    for image, vr_tuple in all_payload_nvrs.items():
        vr = f"{vr_tuple[0]}-{vr_tuple[1]}"
        imagevr = f"{image}-{vr}"
        yellow_prefix("Cross-checking from payload: ")
        click.echo(imagevr)
        if image not in all_advisory_nvrs:
            missing_in_errata[image] = imagevr
            click.echo(f"{imagevr} in payload not found in advisory")
        elif image in all_advisory_nvrs and vr != all_advisory_nvrs[image]:
            click.echo(
                f"{image} from payload has version {vr} which does not match {all_advisory_nvrs[image]} from advisory"
            )
            payload_doesnt_match_errata[image] = {
                'payload': vr,
                'errata': all_advisory_nvrs[image],
            }

    if missing_in_errata:  # check if missing images are already shipped or pending to ship
        advisory_nvrs: Dict[int, List[str]] = {}  # a dict mapping advisory numbers to lists of NVRs
        green_print(f"Checking if {len(missing_in_errata)} missing images are shipped...")
        for nvr in missing_in_errata.copy().values():
            # get the list of advisories that this build has been attached to
            build = get_brew_build(nvr)
            # filter out dropped advisories
            advisories = [ad for ad in build.all_errata if ad["status"] != "DROPPED_NO_SHIP"]
            if not advisories:
                red_print(f"Build {nvr} is not attached to any advisories.")
                continue
            for advisory in advisories:
                if advisory["status"] == "SHIPPED_LIVE":
                    green_print(f"Missing build {nvr} has been shipped with advisory {advisory}.")
                else:
                    yellow_print(f"Missing build {nvr} is in another pending advisory.")
                advisory_nvrs.setdefault(advisory["id"], []).append(nvr)
            name = nvr.rsplit("-", 2)[0]
            del missing_in_errata[name]
        if advisory_nvrs:
            click.echo(f"Getting information of {len(advisory_nvrs)} advisories...")
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

    green_print("Summary results:")
    click.echo(json.dumps(output, indent=4))
    with open('summary_results.json', 'w') as fp:
        json.dump(output, fp, indent=4)
    green_prefix("Wrote out summary results: ")
    click.echo("summary_results.json")
