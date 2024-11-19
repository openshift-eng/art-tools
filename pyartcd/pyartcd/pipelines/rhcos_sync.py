import click
import os
import traceback

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.util import get_ocp_version_from_group, isolate_major_minor_in_group, flatten_list
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib import exectools
from pyartcd import oc
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from pyartcd.util import (get_assembly_type,
                          isolate_el_version_in_release,
                          load_group_config,
                          load_releases_config,
                          default_release_suffix,
                          get_release_name_for_assembly,
                          get_microshift_builds)


SUPPORTED_ARCHES = ["x86_64", "ppc64le", "s390x", "aarch64"]


class RhcosSyncPipeline:
    def __init__(self,
                 runtime: Runtime,
                 group: str,
                 payload_tag_or_pullspec: str,
                 force: bool,
                 no_latest: bool = False):
        self.runtime = runtime
        self.group = group
        self.payload_tag_or_pullspec = payload_tag_or_pullspec
        self.force = force
        self.slack_client = runtime.new_slack_client()
        self.no_latest = no_latest
        self._logger = runtime.logger

        self._working_dir = self.runtime.working_dir.absolute()
        # determines OCP version
        self._ocp_version = get_ocp_version_from_group(group)

    async def run(self):
        # Make sure our api.ci token is fresh
        await oc.registry_login(self.runtime)

    async def sync_to_mirror(self, arch, el_target, pullspec):
        arch = brew_arch_for_go_arch(arch)
        pullspec_file = self._working_dir / "bootc-pullspec.txt"
        with open(pullspec_file, "w") as f:
            f.write(pullspec)

        release_name = get_release_name_for_assembly(self.group, self.releases_config, self.assembly)
        ocp_dir = "ocp-dev-preview" if self.assembly_type == AssemblyTypes.PREVIEW else "ocp"

        major, minor = self._ocp_version
        # This is where we sync microshift artifacts on mirror. Refer to microshift_sync job
        # The paths should be the same in both of these places
        release_path = f"/pub/openshift-v4/{arch}/microshift/{ocp_dir}/{release_name}/{el_target}"
        latest_path = f"/pub/openshift-v4/{arch}/microshift/{ocp_dir}/latest-{major}.{minor}/{el_target}"
        cloudflare_endpoint_url = os.environ['CLOUDFLARE_ENDPOINT']

        async def _run_for(s3_path):
            cmd = [
                "aws", "s3", "sync",
                "--no-progress",
                "--exact-timestamps",
                "--exclude", "*",
                "--include", "bootc-pullspec.txt",
                str(self._working_dir),
                f"s3://art-srv-enterprise{s3_path}",
            ]
            if self.runtime.dry_run:
                cmd.append("--dryrun")
            await exectools.cmd_assert_async(cmd)

            # Sync to Cloudflare as well
            cmd.extend([
                "--profile", "cloudflare",
                "--endpoint-url", cloudflare_endpoint_url,
            ])
            await exectools.cmd_assert_async(cmd)

        await _run_for(release_path)
        await _run_for(latest_path)

    async def download_images(self, image_urls):
        for url in image_urls:
            cmd = [
                "curl", "-L", "--fail", "--retry", "5", "-O", url
            ]
            await exectools.cmd_assert_async(cmd)

            # rename files to indicate the release they match (including arch suffix by tradition)
            # also create an unversioned symlink to enable consistent incoming links.
            for filename in os.listdir(self._working_dir):
                new_filename = filename.replace(self.build_id, self.release_name_with_arch)
                link = filename.replace(self.build_id, "")
                # skip files that did not change i.e. did not have build id in their name
                if new_filename == filename:
                    continue
                os.rename(filename, new_filename)
                os.symlink(new_filename, link)

            # Some customer portals point to the deprecated `rhcos-installer` names rather than `rhcos-live`.
            # Fix those links.
            for f in os.listdir(self._working_dir):
                if f.startswith('rhcos-live-'):
                    installer_link = f.replace('rhcos-live-', 'rhcos-installer-')
                    os.symlink(os.readlink(f), installer_link)

    @staticmethod
    async def gen_sha256(filenames):
        cmd = ["sha256sum", *filenames]
        _, out, _ = await exectools.cmd_gather_async(cmd)
        with open("sha256sum.txt", "w") as f:
            f.write(out)



@cli.command("rhcos-sync")
@click.option("--force", is_flag=True,
              help="Sync even if a sync is not needed")
@click.option("--no-latest", is_flag=True,
              help="Do not sync to the latest directory")
@click.argument("payload_tag_or_pullspec", metavar='TAG_OR_PULLSPEC',
                nargs=1, required=True)
@pass_runtime
@click_coroutine
async def rhcos_sync(runtime: Runtime, group: str, force: bool, no_latest: bool, payload_tag_or_pullspec: str):
    pipeline = RhcosSyncPipeline(runtime=runtime, group=group, payload_tag_or_pullspec=payload_tag_or_pullspec,
                                 force=force, no_latest=no_latest, slack_client=slack_client)
    await pipeline.run()
