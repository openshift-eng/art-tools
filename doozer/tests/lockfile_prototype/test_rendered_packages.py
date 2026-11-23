"""
Tests for package lists rendered during image builds.
"""

import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from dockerfile_parse import DockerfileParser
from doozerlib.lockfile_prototype.models import RepoEntry
from doozerlib.lockfile_prototype.rendered_packages import resolve_rendered_packages


@pytest.mark.parametrize("major,python", [("9", "3.12"), ("10", "3.14")])
def test_resolves_each_stage_and_preserves_constraints(tmp_path: Path, major: str, python: str):
    (tmp_path / "Dockerfile").write_text("""\
FROM example.com/base AS wheel-builder
COPY build-packages-list.ocp /tmp/
COPY python-config.sh /bin/
RUN render_package_list /tmp/build-packages-list.ocp /tmp/build-packages-list.ocp.rendered
FROM example.com/base
ENV PKGS_LIST=main-packages-list.ocp
COPY ${PKGS_LIST} /tmp/
COPY prepare-image.sh python-config.sh /bin/
RUN prepare-image.sh
""")
    (tmp_path / "python-config.sh").write_text("""\
case "${RHEL_MAJOR}" in
    9)
        PYTHON_VERSION=3.12
        ;;
    10)
        PYTHON_VERSION=3.14
        ;;
esac
""")
    (tmp_path / "prepare-image.sh").write_text('render_package_list "/tmp/${PKGS_LIST}" "output"\n')
    (tmp_path / "build-packages-list.ocp").write_text("# comment\ngcc\n@PYTHON_PACKAGE_PREFIX@-devel\n")
    (tmp_path / "main-packages-list.ocp").write_text("\nhttpd\n@PYTHON_PACKAGE_PREFIX@-flask >= 1:2.2.5\n")
    container = AsyncMock()
    container.read_file_from_image.return_value = f'VERSION_ID="{major}.6"\n'

    result = asyncio.run(
        resolve_rendered_packages(
            tmp_path, DockerfileParser(str(tmp_path / "Dockerfile")).structure, ["example.com/base"] * 2, container
        )
    )

    assert result == {
        0: ["gcc", f"python{python}-devel"],
        1: ["httpd", f"python{python}-flask >= 1:2.2.5"],
    }


def test_unavailable_os_release_fails_instead_of_dropping_packages(tmp_path: Path):
    (tmp_path / "Dockerfile").write_text(
        "FROM example.com/base\nCOPY packages /tmp/\nCOPY python-config.sh /bin/\n"
        "RUN render_package_list /tmp/packages /tmp/packages.rendered\n"
    )
    (tmp_path / "packages").write_text("@PYTHON_PACKAGE_PREFIX@-devel\n")
    (tmp_path / "python-config.sh").write_text("")
    container = AsyncMock()
    container.read_file_from_image.return_value = ""

    with pytest.raises(RuntimeError, match="Cannot determine Python package prefix"):
        asyncio.run(
            resolve_rendered_packages(
                tmp_path, DockerfileParser(str(tmp_path / "Dockerfile")).structure, ["example.com/base"], container
            )
        )


def test_reads_usr_lib_os_release_if_etc_is_unavailable(tmp_path: Path):
    (tmp_path / "Dockerfile").write_text(
        "FROM example.com/base\nCOPY packages /tmp/\nCOPY python-config.sh /bin/\n"
        "RUN render_package_list /tmp/packages /tmp/packages.rendered\n"
    )
    (tmp_path / "packages").write_text("@PYTHON_PACKAGE_PREFIX@-devel\n")
    (tmp_path / "python-config.sh").write_text("case $RHEL_MAJOR in\n10)\nPYTHON_VERSION=3.14\n;;\nesac\n")
    container = AsyncMock()
    container.read_file_from_image.side_effect = ["", 'VERSION_ID="10.1"\n']

    result = asyncio.run(
        resolve_rendered_packages(
            tmp_path, DockerfileParser(str(tmp_path / "Dockerfile")).structure, ["example.com/base"], container
        )
    )

    assert result == {0: ["python3.14-devel"]}
    assert container.read_file_from_image.await_count == 2


@pytest.mark.parametrize("majors,expected", [(["9"], "python3.12-devel"), (["9", "10"], None)])
def test_infers_os_version_only_from_unambiguous_repos(tmp_path: Path, majors: list[str], expected: str | None):
    (tmp_path / "Dockerfile").write_text(
        "FROM example.com/base\nCOPY packages /tmp/\nCOPY python-config.sh /bin/\n"
        "RUN render_package_list /tmp/packages /tmp/packages.rendered\n"
    )
    (tmp_path / "packages").write_text("@PYTHON_PACKAGE_PREFIX@-devel\n")
    (tmp_path / "python-config.sh").write_text("case $RHEL_MAJOR in\n9)\nPYTHON_VERSION=3.12\n;;\nesac\n")
    container = AsyncMock()
    container.read_file_from_image.return_value = ""
    repos = [RepoEntry(repoid=f"rhel-{major}-baseos-rpms", baseurl="https://example.com") for major in majors]

    async def resolve():
        return await resolve_rendered_packages(
            tmp_path,
            DockerfileParser(str(tmp_path / "Dockerfile")).structure,
            ["example.com/base"],
            container,
            repos,
            logging.getLogger(__name__),
        )

    if expected:
        assert asyncio.run(resolve()) == {0: [expected]}
    else:
        with pytest.raises(RuntimeError, match="Cannot determine Python package prefix"):
            asyncio.run(resolve())
