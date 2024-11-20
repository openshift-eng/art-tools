import click
import os
from tempfile import TemporaryDirectory

from artcommonlib import exectools
from pyartcd import oc
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from pyartcd.util import mirror_to_s3
from doozerlib.util import isolate_components_in_release_tag


SUPPORTED_ARCHES = ["x86_64", "ppc64le", "s390x", "aarch64"]


class RhcosSyncPipeline:
    def __init__(self,
                 runtime: Runtime,
                 force: bool,
                 no_latest: bool,
                 pullspec: str):
        self.runtime = runtime
        self.pullspec = pullspec
        self.force = force
        self.slack_client = runtime.new_slack_client()
        self.no_latest = no_latest
        self._logger = runtime.logger
        self._ocp_version = None

        self._working_dir = self.runtime.working_dir.absolute()
        self.download_dir = self._working_dir / f"staging-{group}"

    async def run(self):
        if ":" not in self.pullspec:
            raise ValueError("Invalid pullspec: should contain a tag")
        tag = self.pullspec.split(":")[-1]
        self._ocp_version, arch, priv = isolate_components_in_release_tag(tag)

        # Make sure our api.ci token is fresh
        await oc.registry_login(self.runtime)

        _, out, _ = await exectools.cmd_gather_async([
            "oc", "adm", "release", "info", "-o", "template", "--template", "{{ .metadata.version }}", self.pullspec
        ])
        release_name = out.strip()
        if release_name == "<no value>":
            raise ValueError(f"Could not determine release name for pullspec {self.pullspec}")

        await fetch_rhcos_build_from_pullspec()

        self.download_dir.mkdir(parents=True)
        await self.download_images()

        # generate sha256sums
        cmd = ["sha256sum", *filenames]
        _, out, _ = await exectools.cmd_gather_async(cmd, cwd=self.download_dir)
        with open(self.download_dir / "sha256sum.txt", "w") as f:
            f.write(out)

        # generate rhcosID

    async def fetch_rhcos_build_from_pullspec(self):
        _, out, _ = await exectools.cmd_gather_async([
            "oc", "adm", "release", "info", "--image-for", "installer", self.pullspec
        ])
        installer_image = out.strip()

        # use temp directory
        with TemporaryDirectory() as tmpdir:
            # extract the image
            await exectools.cmd_assert_async([
                "oc", "image", "extract", "--path", f"/manifests/:{tmpdir}", installer_image
            ])

            # read the image
            with open(f"{tmpdir}/coreos-bootimages.yaml") as f:
                contents = yaml.safe_load(f)
                stream = contents["data"]["stream"]
                json_contents = json.loads(stream)
                release = json_contents["architectures"][arch]["artifacts"]["qemu"]["release"]

        return release

    async def download_images(self):
        for url in image_urls:
            cmd = ["curl", "-L", "--fail", "--retry", "5", "-O", url]
            await exectools.cmd_assert_async(cmd, cwd=self.download_dir)

            # rename files to indicate the release they match (including arch suffix by tradition)
            # also create an unversioned symlink to enable consistent incoming links.
            for filename in os.listdir(self.download_dir):
                new_filename = filename.replace(self.build_id, self.release_name_with_arch)
                link = filename.replace(self.build_id, "")
                # skip files that did not change i.e. did not have build id in their name
                if new_filename == filename:
                    continue
                os.rename(filename, new_filename)
                os.symlink(new_filename, link)

            # Some customer portals point to the deprecated `rhcos-installer` names rather than `rhcos-live`.
            # Fix those links.
            for f in os.listdir(self.download_dir):
                if f.startswith('rhcos-live-'):
                    installer_link = f.replace('rhcos-live-', 'rhcos-installer-')
                    os.symlink(os.readlink(f), installer_link)

    async def sync_to_mirror(self):
        release_path = f"/pub/openshift-v4/{arch}/microshift/{ocp_dir}/{release_name}/{el_target}"
        latest_path = f"/pub/openshift-v4/{arch}/microshift/{ocp_dir}/latest-{major}.{minor}/{el_target}"

        async def _run_for(s3_path):
            await mirror_to_s3(
                source=self._working_dir,
                dest=f"s3://art-srv-enterprise{s3_path}",
                exclude="*",
                include="bootc-pullspec.txt",
                dry_run=self.runtime.dry_run
            )

        await _run_for(release_path)
        await _run_for(latest_path)


@cli.command("rhcos-sync")
@click.option("--force", is_flag=True,
              help="Sync even if a sync is not needed")
@click.option("--no-latest", is_flag=True,
              help="Do not sync to the latest directory")
@click.argument("pullspec", metavar='PULLSPEC', nargs=1, required=True)
@pass_runtime
@click_coroutine
async def rhcos_sync(runtime: Runtime, force: bool, no_latest: bool, pullspec: str):
    pipeline = RhcosSyncPipeline(runtime=runtime, force=force, no_latest=no_latest, pullspec=pullspec)
    await pipeline.run()
