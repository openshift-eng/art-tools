import base64
import json
import logging
import os
import re
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional

from artcommonlib import exectools
from ruamel.yaml import YAML
from semver import VersionInfo
from tenacity import retry, stop_after_attempt, wait_fixed

LOGGER = logging.getLogger(__name__)
yaml = YAML(typ="safe")
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.explicit_start = True
yaml.width = 1024 * 1024


@dataclass
class OpmRegistryAuth:
    """Dataclass for storing registry authentication information.

    :param path: The path to the registry credentials file. If provided, username, password, and registry_url are ignored.
    :param username: The username to use for the registry. If provided, password is required.
    :param password: The password to use for the registry. If username is not provided, this is used as the auth token.
    :param registry_url: The URL of the registry to authenticate. Default is "quay.io".
    """

    path: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    registry_url: Optional[str] = None


async def gather_opm(args: List[str], auth: Optional[OpmRegistryAuth] = None, **kwargs):
    """Run opm with the given arguments and return the result.

    :param args: The arguments to pass to opm.
    :param auth: The registry authentication information to use.
    :param kwargs: Additional keyword arguments to pass to exectools.cmd_gather_async.
    :return: The return code, stdout, and stderr of the opm command.
    """
    # Build the basic auth string
    auth_content = None
    if auth:
        if auth.path:
            auth_content = Path(auth.path).read_text()
        else:
            auth_token = None
            if auth.username:
                if not auth.password:
                    raise ValueError("password is required when using a username.")
                auth_token = base64.b64encode(f"{auth.username}:{auth.password}".encode()).decode()
            elif auth.password:  # Use the password as the auth token
                auth_token = auth.password
            if auth_token:
                auth_content = json.dumps(
                    {
                        "auths": {
                            auth.registry_url or "quay.io": {
                                "auth": auth_token,
                            },
                        },
                    }
                )

    # Use a temporary directory for OPM's internal temp files (opm-registry-*, bundle_tmp*, etc.)
    # Setting TMPDIR ensures these files are cleaned up when the context exits,
    # preventing leftover directories on build machines.
    with TemporaryDirectory(prefix="_doozer_opm_") as opm_tmpdir:
        env = kwargs.pop("env", None)
        env = env.copy() if env is not None else os.environ.copy()
        env["TMPDIR"] = opm_tmpdir

        if not auth_content:
            LOGGER.warning("No registry auth provided. Running opm without auth.")
            rc, out, err = await exectools.cmd_gather_async(["opm", *args], env=env, **kwargs)
            return rc, out, err

        auth_file = Path(opm_tmpdir, "config.json")
        auth_file.write_text(auth_content)
        env["DOCKER_CONFIG"] = opm_tmpdir
        rc, out, err = await exectools.cmd_gather_async(["opm", *args], env=env, **kwargs)
    return rc, out, err


async def verify_opm():
    """Verify that opm is installed and at least version 1.47.0"""
    try:
        _, out, _ = await gather_opm(["version"])
    except FileNotFoundError:
        raise FileNotFoundError("opm binary not found.")
    m = re.search(r"OpmVersion:\"v([^\"]+)\"", out)
    if not m:
        raise IOError(f"Failed to parse opm version: {out}")
    version = VersionInfo.parse(m.group(1))
    if version < VersionInfo(1, 47, 0):
        raise IOError(f"opm version {version} is too old. Please upgrade to at least 1.47.0.")


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
async def render(
    *input: str, output_format: str = "yaml", migrate_level: str = "none", auth: Optional[OpmRegistryAuth] = None
) -> List[dict]:
    """
        Run `opm render` on the given input and return the parsed file-based catalog blobs.

        :param input: The catalog images, file-based catalog directories, bundle images, and sqlite
    database files to render.
        :param output_format: The output format to use when rendering the catalog. One of "yaml" or "json".
        :param migrate_level: The migration level to use when rendering the catalog. One of "none" or "bundle-object-to-csv-metadata".
        :param auth: The registry authentication information to use.
        :return: The parsed file-based catalog blobs.
    """
    if not input:
        raise ValueError("input must not be empty.")
    if output_format not in ["yaml", "json"]:
        raise ValueError(f"Invalid output format: {output_format}")
    LOGGER.debug(f"Rendering FBC for {', '.join(input)}")
    _, out, _ = await gather_opm(
        ["render", "--migrate-level", migrate_level, "-o", output_format, "--", *input], auth=auth
    )
    blobs = yaml.load_all(StringIO(out))
    return list(blobs)


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
    with open(template_file, "w") as out:
        await gather_opm(
            ["alpha", "convert-template", "basic", "-o", output_format, "--", str(catalog_file)], stdout=out
        )


async def render_catalog_from_template(
    template_file: Path,
    catalog_file: Path,
    migrate_level: str = "none",
    output_format: str = "yaml",
    auth: Optional[OpmRegistryAuth] = None,
):
    """
    Render a catalog from a template file.

    :param template_file: The template file to render.
    :param catalog_file: The file to write the rendered catalog to.
    :param migrate_level: The migration level to use when rendering the catalog.
    :param output_format: The output format to use when rendering the catalog. One of "yaml" or "json".
    :param auth: The registry authentication information to use. If not provided, system-default authentication is used.
    """
    if migrate_level not in ["none", "bundle-object-to-csv-metadata"]:
        raise ValueError(f"Invalid migrate level: {migrate_level}")
    if output_format not in ["yaml", "json"]:
        raise ValueError(f"Invalid output format: {output_format}")
    LOGGER.debug(f"Rendering catalog {catalog_file} from template {template_file}")
    with open(catalog_file, "w") as out:
        await gather_opm(
            [
                "alpha",
                "render-template",
                "basic",
                "--migrate-level",
                migrate_level,
                "-o",
                output_format,
                "--",
                str(template_file),
            ],
            stdout=out,
            auth=auth,
        )


async def generate_dockerfile(dest_dir: Path, dc_dir_name: str, base_image: str, builder_image: str):
    """
    Generate a Dockerfile for for a file-based catalog.

    :param dest_dir: The directory to generate the Dockerfile in.
    :param dc_dir_name: The directory containing the file-based catalog.
    :param base_image: Image base to use to build catalog.
    """
    options = [
        "--builder-image",
        builder_image,
        "--base-image",
        base_image,
    ]
    LOGGER.debug(f"Generating FBC Dockerfile in {dest_dir}")
    await gather_opm(["generate", "dockerfile"] + options + ["--", dc_dir_name], cwd=dest_dir)


async def validate(catalog_dir: Path):
    """
    Validate the file-based catalog in a given directory

    :param catalog_dir: The directory containing the file-based catalog.
    """
    LOGGER.debug(f"Validating FBC in {catalog_dir}")
    await gather_opm(["validate", "--", str(catalog_dir)])
