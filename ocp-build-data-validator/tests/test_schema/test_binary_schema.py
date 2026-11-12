import os
import shutil
import tempfile
import unittest

from validator import support
from validator.schema import binary_schema, validate

BUILDERS = [{"member": "ose-installer"}, {"member": "ose-installer-artifacts"}]


class TestBinarySchema(unittest.TestCase):
    def setUp(self):
        self.obd = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.obd)
        os.makedirs(os.path.join(self.obd, "images"))
        os.makedirs(os.path.join(self.obd, "binaries", "openshift-install"))
        for name in ("ose-installer", "ose-installer-artifacts"):
            open(os.path.join(self.obd, "images", f"{name}.yml"), "w").close()
        open(os.path.join(self.obd, "binaries", "openshift-install", "Dockerfile"), "w").close()
        self.file = os.path.join(self.obd, "binaries", "openshift-install.yml")

    def _data(self, **overrides):
        data = {
            "name": "openshift/openshift-install-binaries-rhel9",
            "arches": ["x86_64"],
            "content": {
                "source": {
                    "dockerfile": "Dockerfile",
                    "path": "binaries/openshift-install",
                    "git": {
                        "branch": {"target": "openshift-{MAJOR}.{MINOR}"},
                        "url": "git@github.com:openshift-priv/ocp-build-data.git",
                        "web": "https://github.com/openshift-eng/ocp-build-data",
                    },
                }
            },
            "from": {"builder": BUILDERS, "stream": "rhel9"},
            "konflux": {"allow_cross_platform_images": True, "no_shell": True, "network_mode": "hermetic"},
            "owners": ["aos-team-art@redhat.com"],
        }
        data.update(overrides)
        return data

    def test_artifact_type_and_namespace(self):
        self.assertEqual(support.get_artifact_type(self.file), "binary")
        self.assertEqual(support.get_artifact_type("ocp-build-data/images/ose-installer.yml"), "image")
        self.assertEqual(support.get_namespace({}, self.file), "containers")

    def test_valid_binary(self):
        self.assertIsNone(binary_schema.validate(self.file, self._data()))
        self.assertIsNone(validate(self.file, self._data()))

    def test_image_only_keys_are_rejected(self):
        for key, value in (
            ("for_payload", False),
            ("for_release", False),
            ("delivery", {"delivery_repo_names": ["x"]}),
            ("dependents", ["x"]),
        ):
            with self.subTest(key=key):
                error = binary_schema.validate(self.file, self._data(**{key: value}))
                self.assertIn("Additional properties are not allowed", error)

    def test_required_keys(self):
        data = self._data()
        del data["from"]
        self.assertIn("'from' is a required property", binary_schema.validate(self.file, data))

    def test_unknown_member_is_rejected(self):
        data = self._data(**{"from": {"builder": [{"member": "ose-nope"}], "stream": "rhel9"}})
        self.assertIn("member 'ose-nope' not found", binary_schema.validate(self.file, data))

    def test_missing_build_context_is_rejected(self):
        os.remove(os.path.join(self.obd, "binaries", "openshift-install", "Dockerfile"))
        self.assertIn(
            "binaries/openshift-install/Dockerfile not found", binary_schema.validate(self.file, self._data())
        )

    def test_name_clash_with_image_is_rejected(self):
        open(os.path.join(self.obd, "images", "openshift-install.yml"), "w").close()
        self.assertIn("same name as images/openshift-install.yml", binary_schema.validate(self.file, self._data()))

    def test_image_schema_parts_still_apply(self):
        data = self._data(konflux={"no_shell": "yes"})
        self.assertIn("'yes' is not of type 'boolean'", binary_schema.validate(self.file, data))


if __name__ == "__main__":
    unittest.main()
