import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from doozerlib.lockfile_prototype.rebaser_hooks import apply_dockerfile_transforms


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
