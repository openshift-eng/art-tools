import asyncio
import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import click

from doozerlib import opm
from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.image import ImageMetadata
from doozerlib.runtime import Runtime
from doozerlib.source_resolver import SourceResolver

LOGGER = logging.getLogger(__name__)
yaml = opm.yaml

PRODUCTION_INDEX_PULLSPEC_FORMAT = "registry.redhat.io/redhat/redhat-operator-index:v{major}.{minor}"
BASE_IMAGE_RHEL9_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v{major}.{minor}"
BASE_IMAGE_RHEL8_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry:v{major}.{minor}"


class FbcImportCli:
    def __init__(self, runtime: Runtime, index_image: str | None, dest_dir: str):
        self.runtime = runtime
        self.index_image = index_image
        self.dest_dir = dest_dir

    async def run(self):
        """ Run the FBC import process
        This function implements the main logic of the FBC import process
        following https://github.com/konflux-ci/olm-operator-konflux-sample/blob/main/docs/konflux-onboarding.md#create-the-fbc-in-the-git-repository.
        """
        # Ensure opm is installed
        try:
            await opm.verify_opm()
        except (IOError, FileNotFoundError):
            LOGGER.error("Please install the latest opm cli binary from https://github.com/operator-framework/operator-registry/releases")
            raise

        # Initialize runtime
        runtime = self.runtime
        runtime.initialize(mode="images", clone_distgits=False)
        if "MAJOR" not in runtime.group_config.vars or "MINOR" not in runtime.group_config.vars:
            raise ValueError("MAJOR and MINOR vars must be set in group config")
        major = runtime.group_config.vars.MAJOR
        minor = runtime.group_config.vars.MINOR
        source_resolver = runtime.source_resolver
        assert source_resolver, "source_resolver is not set; Doozer bug?"
        if not self.index_image:
            self.index_image = PRODUCTION_INDEX_PULLSPEC_FORMAT.format(major=major, minor=minor)

        # Load package.yaml files and operator index image
        base_dir = Path(self.dest_dir)
        base_dir.mkdir(parents=True, exist_ok=True)
        operator_metas: List[ImageMetadata] = [operator_meta for operator_meta in runtime.ordered_image_metas() if operator_meta.is_olm_operator]

        LOGGER.info("Loading package.yaml files and remote operator index image...")
        package_names, org_catalog_blobs = await asyncio.gather(
            self.load_package_names(operator_metas, source_resolver),
            opm.render(self.index_image),
        )

        LOGGER.info("Filtering catalog blobs...")
        org_catalog_blobs = self.filter_catalog_blobs(org_catalog_blobs, set(package_names))

        # Write catalog_blobs to catalog-migrate/<package>/catalog.json
        LOGGER.info("Writing original catalog blobs to catalog-migrate")
        org_catalog_dir = base_dir.joinpath("catalog-migrate")
        org_catalog_dir.mkdir(parents=True, exist_ok=True)
        org_catalog_files: Dict[str, Path] = {}  # key is package name, value is catalog file path
        for package_name, blobs in org_catalog_blobs.items():
            org_catalog_subdir = org_catalog_dir.joinpath(package_name)
            org_catalog_subdir.mkdir(parents=True, exist_ok=True)
            org_catalog_file = org_catalog_subdir.joinpath("catalog.yaml")
            with org_catalog_file.open('w') as f:
                yaml.dump_all(blobs, f)
                # for blob in blobs:
                #     json.dump(blob, f, indent=2)
            org_catalog_files[package_name] = org_catalog_file

        # Generate basic fbc templates to catalog-templates/<package>.yaml
        LOGGER.info("Generating basic FBC templates")
        templates_dir = base_dir.joinpath("catalog-templates")
        templates_dir.mkdir(parents=True, exist_ok=True)
        template_files: Dict[str, Path] = {}  # key is package name, value is template file path
        tasks = []
        for package_name, org_catalog_file in org_catalog_files.items():
            template_file = templates_dir.joinpath(f"{package_name}.yaml")
            tasks.append(asyncio.create_task(opm.generate_basic_template(org_catalog_file, template_file)))
            template_files[package_name] = template_file
        await asyncio.gather(*tasks)

        LOGGER.info("Cleaning up catalog-migrate")
        shutil.rmtree(org_catalog_dir)

        # Render final FBC catalogs from templates at catalog/<package>/catalog.yaml
        LOGGER.info("Rendering final FBC catalogs")
        migrate_level = "none"
        if (major, minor) >= (4, 17):
            migrate_level = "bundle-object-to-csv-metadata"
        catalog_dir = base_dir.joinpath("catalog")
        catalog_dir.mkdir(parents=True, exist_ok=True)
        tasks = []
        for package_name, template_file in template_files.items():
            catalog_subdir = catalog_dir.joinpath(package_name)
            catalog_subdir.mkdir(parents=True, exist_ok=True)
            catalog_file = catalog_subdir.joinpath("catalog.yaml")
            LOGGER.info("Rendering catalog %s", catalog_file)
            tasks.append(asyncio.create_task(opm.render_catalog_from_template(template_file, catalog_file, migrate_level=migrate_level)))
        await asyncio.gather(*tasks)

        # Generate Dockerfile
        LOGGER.info("Generating Dockerfile")
        base_image_format = BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if (major, minor) > (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
        base_image = base_image_format.format(major=major, minor=minor)
        await opm.generate_dockerfile(base_dir, "catalog", base_image=base_image, builder_image=base_image)

    async def load_package_names(self, operator_metas: List[ImageMetadata], source_resolver: SourceResolver) -> List[str]:
        """ Load OLM package names for operator images.
        This function loads OLM package names for operator images by resolving sources and loading package.yaml files.

        :param operator_metas: List of operator image metadata.
        :param source_resolver: SourceResolver instance.
        :return: List of OLM package names.
        """
        package_names: List[str] = []
        sources = await asyncio.gather(*(asyncio.to_thread(source_resolver.resolve_source, operator_meta) for operator_meta in operator_metas))
        for operator_meta, source in zip(operator_metas, sources):
            source_dir = source_resolver.get_source_dir(source, operator_meta)
            csv_config = operator_meta.config.get('update-csv')
            if not csv_config:
                raise ValueError(f"update-csv config not found for {operator_meta.distgit_key}")
            source_path = source_dir.joinpath(csv_config['manifests-dir'])
            package_yaml_file = next(source_path.glob('**/*package.yaml'))
            with package_yaml_file.open() as f:
                package_yaml = yaml.load(f)
            package_name = str(package_yaml['packageName'])
            package_names.append(package_name)
        return package_names

    def filter_catalog_blobs(self, blobs: List[Dict], allowed_package_names: Set[str]):
        """ Filter catalog blobs by package names.

        :param blobs: List of catalog blobs.
        :param allowed_package_names: Set of allowed package names.
        :return: Dict of filtered catalog blobs.
        """
        filtered: Dict[str, List[Dict[str, Any]]] = {}  # key is package name, value is blobs
        for blob in blobs:
            schema = blob["schema"]
            package_name = None
            match schema:
                case "olm.package":
                    package_name = blob["name"]
                case "olm.channel" | "olm.bundle" | "olm.deprecations":
                    package_name = blob["package"]
            if not package_name:
                raise IOError(f"Couldn't determine package name for unknown schema: {schema}")
            if package_name not in allowed_package_names:
                continue  # filtered out; skipping
            if package_name not in filtered:
                filtered[package_name] = []
            filtered[package_name].append(blob)
        return filtered


@cli.command("beta:fbc:import", short_help="Create FBC by importing from the provided index image")
@click.option("--from-index", metavar='INDEX_IMAGE', help="The index image to import from")
@click.argument("dest_dir", metavar='DEST_DIR', default=".")
@pass_runtime
@click_coroutine
async def fbc_import(runtime: Runtime, from_index: Optional[str], dest_dir: str):
    """
    Create an FBC repository by importing from the provided index image

    Example usage:

    doozer --group=openshift-4.17 beta:fbc:import registry.redhat.io/redhat/redhat-operator-index:v4.17 ./fbc-4.17
    """
    cli = FbcImportCli(runtime=runtime, index_image=from_index, dest_dir=dest_dir)
    await cli.run()
