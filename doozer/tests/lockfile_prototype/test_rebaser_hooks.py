import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from doozerlib.lockfile_prototype.rebaser_hooks import apply_dockerfile_transforms


class TestApplyDockerfileTransforms(unittest.TestCase):
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
