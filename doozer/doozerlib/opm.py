from io import StringIO
import logging
import re
from pathlib import Path

from ruamel.yaml import YAML
from artcommonlib import exectools
from semver import VersionInfo

LOGGER = logging.getLogger(__name__)
yaml = YAML(typ='safe')
yaml.default_flow_style = False


async def verify_opm():
    """ Verify that opm is installed and at least version 1.47.0 """
    try:
        _, out, _ = await exectools.cmd_gather_async(["opm", "version"])
    except FileNotFoundError:
        raise FileNotFoundError("opm binary not found.")
    m = re.search(r"OpmVersion:\"v([^\"]+)\"", out)
    if not m:
        raise IOError(f"Failed to parse opm version: {out}")
    version = VersionInfo.parse(m.group(1))
    if version < VersionInfo(1, 47, 0):
        raise IOError(f"opm version {version} is too old. Please upgrade to at least 1.47.0.")


async def render(catalog: str, output_format: str = "yaml"):
    """
    Run `opm render` on the given index and return the parsed file-based catalog blobs.

    :param catalog: The catalog to render.
    :param output_format: The output format to use when rendering the catalog. One of "yaml" or "json".
    """
    if output_format not in ["yaml", "json"]:
        raise ValueError(f"Invalid output format: {output_format}")
    LOGGER.debug(f"Rendering catalog {catalog}")
    _, out, _ = await exectools.cmd_gather_async(["opm", "render", "-o", "yaml", "--", catalog])
    blobs = yaml.load_all(StringIO(out))
    return blobs


async def generate_basic_template(catalog_file: Path, template_file: Path, output_format: str = "yaml"):
    """
    Generate a basic FBC template from a catalog file.

    :param catalog_file: The catalog file to generate the template from.
    :param template_file: The file to write the template to.
    :param output_format: The output format to use when generating the template. One of "yaml" or "json".
    """
    if output_format not in ["yaml", "json"]:
        raise ValueError(f"Invalid output format: {output_format}")
    LOGGER.debug(f"Generating basic template {template_file} from {catalog_file}")
    with open(template_file, 'w') as out:
        await exectools.cmd_assert_async([
            "opm", "alpha", "convert-template", "basic", "-o", output_format, "--", str(catalog_file),
        ], stdout=out)


async def render_catalog_from_template(template_file: Path, catalog_file: Path, migrate_level: str = "none", output_format: str = "yaml"):
    """
    Render a catalog from a template file.

    :param template_file: The template file to render.
    :param catalog_file: The file to write the rendered catalog to.
    :param migrate_level: The migration level to use when rendering the catalog.
    :param output_format: The output format to use when rendering the catalog. One of "yaml" or "json".
    """
    if migrate_level not in ["none", "bundle-object-to-csv-metadata"]:
        raise ValueError(f"Invalid migrate level: {migrate_level}")
    if output_format not in ["yaml", "json"]:
        raise ValueError(f"Invalid output format: {output_format}")
    LOGGER.debug(f"Rendering catalog {catalog_file} from template {template_file}")
    with open(catalog_file, 'w') as out:
        await exectools.cmd_assert_async([
            "opm", "alpha", "render-template", "basic", "--migrate-level", migrate_level, "-o", output_format, "--", str(template_file),
        ], stdout=out)


async def generate_dockerfile(dest_dir: Path, dc_dir_name: str, base_image: str, builder_image: str):
    """
    Generate a Dockerfile for for a file-based catalog.

    :param dest_dir: The directory to generate the Dockerfile in.
    :param dc_dir_name: The directory containing the file-based catalog.
    :param base_image: Image base to use to build catalog.
    """
    options = [
        "--builder-image", builder_image,
        "--base-image", base_image,
    ]
    LOGGER.debug(f"Generating FBC Dockerfile in {dest_dir}")
    await exectools.cmd_assert_async(
        ["opm", "generate", "dockerfile"] + options + ["--", dc_dir_name],
        cwd=dest_dir)
