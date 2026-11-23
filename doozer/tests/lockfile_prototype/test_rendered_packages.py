"""
Tests for package lists rendered during image builds.
"""

import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from dockerfile_parse import DockerfileParser
from doozerlib.lockfile_prototype.dynamic_packages import DynamicPackageSet
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
        0: DynamicPackageSet(common=["gcc", f"python{python}-devel"], arch_specific={}),
        1: DynamicPackageSet(common=["httpd", f"python{python}-flask >= 1:2.2.5"], arch_specific={}),
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

    assert result == {0: DynamicPackageSet(common=["python3.14-devel"], arch_specific={})}
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
        assert asyncio.run(resolve()) == {0: DynamicPackageSet(common=[expected], arch_specific={})}
    else:
        with pytest.raises(RuntimeError, match="Cannot determine Python package prefix"):
            asyncio.run(resolve())


def test_wildcard_copy_includes_common_and_arch_specific_templates(tmp_path: Path):
    (tmp_path / "Dockerfile").write_text(
        "FROM example.com/base\nENV PKGS_LIST=packages-list.ocp\nARG EXTRA_PKGS_LIST\n"
        "COPY ${PKGS_LIST}* ${EXTRA_PKGS_LIST:-$PKGS_LIST} /tmp/\n"
        "COPY prepare-image.sh python-config.sh /bin/\nRUN prepare-image.sh\n"
    )
    (tmp_path / "prepare-image.sh").write_text(
        'package_list="/tmp/${PKGS_LIST}"\nrender_package_list "${package_list}" output\n'
        'arch_package_list="${package_list}-$(arch)"\nrender_package_list "${arch_package_list}" output\n'
    )
    (tmp_path / "python-config.sh").write_text("case $RHEL_MAJOR in\n9)\nPYTHON_VERSION=3.12\n;;\nesac\n")
    (tmp_path / "packages-list.ocp").write_text("/usr/sbin/udevadm\n@PYTHON_PACKAGE_PREFIX@-six\n")
    (tmp_path / "packages-list.ocp-x86_64").write_text("syslinux\n")
    (tmp_path / "packages-list.ocp-aarch64").write_text("aarch64-package\n")
    container = AsyncMock()
    container.read_file_from_image.return_value = 'VERSION_ID="9.8"\n'

    result = asyncio.run(
        resolve_rendered_packages(
            tmp_path,
            DockerfileParser(str(tmp_path / "Dockerfile")).structure,
            ["example.com/base"],
            container,
            arches=["x86_64", "aarch64"],
        )
    )

    assert result == {
        0: DynamicPackageSet(
            common=["/usr/sbin/udevadm", "python3.12-six"],
            arch_specific={"x86_64": ["syslinux"], "aarch64": ["aarch64-package"]},
        )
    }


def test_copies_are_resolved_at_each_run_and_local_copy_options_are_supported(tmp_path: Path):
    (tmp_path / "Dockerfile").write_text(
        "FROM example.com/base\nCOPY --chown=0:0 first /tmp/packages\n"
        "RUN render_package_list /tmp/packages /tmp/out\n"
        "COPY --chmod=0644 second /tmp/packages\n"
        "RUN render_package_list /tmp/packages /tmp/out\n"
    )
    for name in ("first", "second", "third"):
        (tmp_path / name).write_text(f"{name}-package\n")

    result = asyncio.run(
        resolve_rendered_packages(
            tmp_path, DockerfileParser(str(tmp_path / "Dockerfile")).structure, ["example.com/base"], AsyncMock()
        )
    )
    assert result == {0: DynamicPackageSet(common=["first-package", "second-package"], arch_specific={})}


def test_named_stage_inherits_json_copy_and_renamed_destination(tmp_path: Path):
    (tmp_path / "Dockerfile").write_text(
        'FROM example.com/base AS build\nENV PKGS_LIST=selected\nCOPY ["packages", "/tmp/selected"]\n'
        'FROM build\nRUN render_package_list "/tmp/${PKGS_LIST}" /tmp/out\n'
        'FROM example.com/other\nRUN render_package_list /tmp/selected /tmp/out\n'
    )
    (tmp_path / "packages").write_text("curl\n")

    result = asyncio.run(
        resolve_rendered_packages(
            tmp_path,
            DockerfileParser(str(tmp_path / "Dockerfile")).structure,
            ["example.com/base", "build", "other"],
            AsyncMock(),
            skipped_stages={0},
        )
    )
    assert result == {1: DynamicPackageSet(common=["curl"], arch_specific={})}


def test_copy_from_invalidates_overwritten_file(tmp_path: Path):
    (tmp_path / "Dockerfile").write_text(
        "FROM example.com/base\nCOPY packages /tmp/packages\nCOPY keep /tmp/keep\n"
        "COPY --from=other /packages /tmp/packages\nRUN render_package_list /tmp/keep /tmp/out\n"
    )
    (tmp_path / "packages").write_text("old\n")
    (tmp_path / "keep").write_text("curl\n")
    container = AsyncMock()
    entries = DockerfileParser(str(tmp_path / "Dockerfile")).structure
    assert asyncio.run(resolve_rendered_packages(tmp_path, entries, ["example.com/base"], container)) == {
        0: DynamicPackageSet(common=["curl"], arch_specific={})
    }

    (tmp_path / "Dockerfile").write_text(
        "FROM example.com/base\nCOPY packages /tmp/packages\n"
        "COPY --from=other /packages /tmp/packages\nRUN render_package_list /tmp/packages /tmp/out\n"
    )
    entries = DockerfileParser(str(tmp_path / "Dockerfile")).structure
    with pytest.raises(RuntimeError, match="copied from another stage"):
        asyncio.run(resolve_rendered_packages(tmp_path, entries, ["example.com/base"], container))


def test_skipped_stage_and_alias_repository_fallback(tmp_path: Path):
    (tmp_path / "Dockerfile").write_text(
        "FROM example.com/base AS build\nCOPY packages /tmp/\n"
        "RUN render_package_list /tmp/packages /tmp/out\n"
        "FROM build\nCOPY packages /tmp/\nCOPY python-config.sh /bin/\n"
        "RUN render_package_list /tmp/packages /tmp/out\n"
    )
    (tmp_path / "packages").write_text("@PYTHON_PACKAGE_PREFIX@-six\n")
    (tmp_path / "python-config.sh").write_text("case $RHEL_MAJOR in\n9)\nPYTHON_VERSION=3.12\n;;\nesac\n")
    container = AsyncMock()
    repos = [RepoEntry(repoid="rhel-9-baseos-rpms", baseurl="https://example.com")]

    result = asyncio.run(
        resolve_rendered_packages(
            tmp_path,
            DockerfileParser(str(tmp_path / "Dockerfile")).structure,
            ["example.com/base", "build"],
            container,
            repos,
            skipped_stages={0},
        )
    )
    assert result == {1: DynamicPackageSet(common=["python3.12-six"], arch_specific={})}
    container.read_file_from_image.assert_not_awaited()

    repos.append(RepoEntry(repoid="rhel-10-baseos-rpms", baseurl="https://example.com"))
    with pytest.raises(RuntimeError, match="Cannot determine Python package prefix"):
        asyncio.run(
            resolve_rendered_packages(
                tmp_path,
                DockerfileParser(str(tmp_path / "Dockerfile")).structure,
                ["example.com/base", "build"],
                container,
                repos,
                skipped_stages={0},
            )
        )
