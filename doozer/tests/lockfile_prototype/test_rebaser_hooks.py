import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.lockfile_prototype.generator import RpmLockfilePrototypeGenerator
from doozerlib.lockfile_prototype.models import LockfileData
from doozerlib.lockfile_prototype.rebaser_hooks import apply_dockerfile_transforms, generate_lockfile


class TestGenerateLockfile(unittest.IsolatedAsyncioTestCase):
    async def test_promised_member_preserves_rpms_without_pulling_a_parent(self):
        with TemporaryDirectory() as tmp:
            working = Path(tmp)
            parent_dir = working / "parent"
            parent_dir.mkdir()
            dest_dir = working / "child"
            dest_dir.mkdir()
            (dest_dir / "Dockerfile").write_text(
                "FROM quay.io/example/images:promised-build\nRUN dnf versionlock add '*' && dnf install -y target\n"
            )
            inherited = {"aarch64": {"libatomic": {"1.0-1"}}, "s390x": {}}
            parent = SimpleNamespace(
                distgit_key="parent",
                qualified_key="parent",
                lockfile_packages=[],
                lockfile_inherited_rpms=inherited,
            )
            metadata = MagicMock()
            metadata.config = {"from": {"member": "parent"}}
            metadata.get_enabled_repos.return_value = {"repo"}
            metadata.get_arches.return_value = list(inherited)
            metadata.distgit_key = "child"

            repo = MagicMock(cs_optional=False)
            repo.baseurl.side_effect = lambda repotype, arch: f"https://repo/{arch}/"
            repo.content_set.return_value = "repo"
            repo.get_repodata = AsyncMock(return_value=SimpleNamespace(primary_rpms=[]))
            with (
                patch("doozerlib.lockfile_prototype.rebaser_hooks.RpmResolver") as resolver_factory,
                patch.object(RpmLockfilePrototypeGenerator, "_write_lockfile"),
            ):
                resolver = resolver_factory.return_value
                resolver.resolve = AsyncMock(return_value=LockfileData())
                resolver.get_installed_evrs = AsyncMock()
                cache = await generate_lockfile(
                    metadata,
                    dest_dir,
                    {"repo": repo},
                    working,
                    working,
                    downstream_parents=["quay.io/example/images:promised-build"],
                    parent_members=[parent],
                )
                self.addCleanup(cache.cleanup)
            resolver.get_installed_evrs.assert_not_awaited()
            self.assertEqual(metadata.lockfile_inherited_rpms, inherited)
            self.assertIsNone(resolver.resolve.await_args.kwargs["image_pullspec"])
            self.assertEqual(set(resolver.resolve.await_args.kwargs["repos_by_arch"]), set(inherited))


class TestApplyDockerfileTransforms(unittest.TestCase):
    def test_makes_dynamic_package_reinstall_nonfatal(self):
        """
        A dynamic package script may reinstall packages that are not installed
        in the base image; only that optional reinstall pass is tolerated.
        """
        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            dockerfile = dest_dir / "Dockerfile"
            dockerfile.write_text("FROM base\nRUN extensions/build.sh\n")
            script = dest_dir / "extensions" / "build.sh"
            script.parent.mkdir()
            script.write_text(
                'extensions_yaml="extensions/${ID}-${VERSION_ID}.yaml"\n'
                "for subcommand in 'install' 'reinstall'; do\n"
                "    dnf --repo=\"${repo_list}\" \"${subcommand}\" \\\n"
                "        --downloadonly \\\n"
                "        \"${all_packages[@]}\"\n"
                "done\n"
            )

            apply_dockerfile_transforms(
                dest_dir,
                strip_updates=False,
            )

            result = script.read_text()

        self.assertIn(
            '"${all_packages[@]}" || [ "${subcommand}" = "reinstall" ]',
            result,
        )
        self.assertIn('"${subcommand}" \\\n', result)

    def test_adds_gpg_key_import_for_installroot(self):
        """
        An installroot command must import the Red Hat GPG key when its
        target directory is empty.
        """
        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            dockerfile = dest_dir / "Dockerfile"
            dockerfile.write_text(
                "FROM base\nRUN mkdir -p /mnt/rootfs && dnf --installroot=/mnt/rootfs install -y test-package\n"
            )

            apply_dockerfile_transforms(dest_dir, strip_updates=False)

            result = dockerfile.read_text()

        self.assertIn(
            "rpm --root /mnt/rootfs --import /etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release",
            result,
        )
        self.assertIn(
            "[ -z \"$(ls -A /mnt/rootfs)\" ]",
            result,
        )
        self.assertIn("dnf --installroot=/mnt/rootfs install -y test-package", result)
