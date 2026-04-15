import base64
import json
import os
import tempfile
import unittest

from artcommonlib.registry_config import (
    COMMON_READ_ONLY_REGISTRIES,
    Registry,
    RegistryConfig,
)


class TestRegistry(unittest.TestCase):
    def test_requires_exactly_one_source(self):
        # No source
        with self.assertRaises(ValueError):
            Registry("quay.io")

        # Multiple sources
        with self.assertRaises(ValueError):
            Registry("quay.io", auth_file="/path/to/auth.json", username_password="user:pass")

    def test_valid_auth_file(self):
        reg = Registry("quay.io", auth_file="/path/to/auth.json")
        self.assertEqual(reg.auth_file, "/path/to/auth.json")
        self.assertIsNone(reg.username_password)
        self.assertIsNone(reg.base64_auth)

    def test_valid_username_password(self):
        reg = Registry("quay.io", username_password="user:pass")
        self.assertEqual(reg.username_password, "user:pass")
        self.assertIsNone(reg.auth_file)
        self.assertIsNone(reg.base64_auth)

    def test_valid_base64_auth(self):
        reg = Registry("quay.io", base64_auth="dXNlcjpwYXNz")
        self.assertEqual(reg.base64_auth, "dXNlcjpwYXNz")
        self.assertIsNone(reg.auth_file)
        self.assertIsNone(reg.username_password)

    def test_empty_path_raises(self):
        with self.assertRaises(ValueError):
            Registry("", auth_file="/path/to/auth.json")

    def test_empty_auth_file_raises(self):
        with self.assertRaises(ValueError):
            Registry("quay.io", auth_file="")

    def test_normalizes_path(self):
        reg = Registry("Quay.IO/OpenShift/CI", auth_file="/path/to/auth.json")
        self.assertEqual(reg.path, "quay.io/OpenShift/CI")  # Host lowercased, path preserved

        reg = Registry("QUAY.IO", auth_file="/path/to/auth.json")
        self.assertEqual(reg.path, "quay.io")

    def test_strips_schemes(self):
        reg = Registry("docker://quay.io/foo", auth_file="/path/to/auth.json")
        self.assertEqual(reg.path, "quay.io/foo")

        reg = Registry("https://registry.redhat.io", auth_file="/path/to/auth.json")
        self.assertEqual(reg.path, "registry.redhat.io")

    def test_strips_digest(self):
        reg = Registry("quay.io/foo@sha256:abc123", auth_file="/path/to/auth.json")
        self.assertEqual(reg.path, "quay.io/foo")

    def test_strips_trailing_slash(self):
        reg = Registry("quay.io/foo/", auth_file="/path/to/auth.json")
        self.assertEqual(reg.path, "quay.io/foo")


class TestRegistryConfig(unittest.TestCase):
    def _sample_b64(self) -> str:
        return base64.b64encode(b"user:pass").decode()

    def test_base64_auth(self):
        b64 = self._sample_b64()
        with RegistryConfig(
            registries=[
                Registry("quay.io", base64_auth=b64),
            ]
        ) as auth_file:
            self.assertTrue(os.path.isfile(auth_file))
            with open(auth_file, encoding="utf-8") as fh:
                data = json.load(fh)
            self.assertEqual(data["auths"]["quay.io"]["auth"], b64)

    def test_username_password(self):
        with RegistryConfig(
            registries=[
                Registry("quay.io", username_password="user:pass"),
            ]
        ) as auth_file:
            with open(auth_file, encoding="utf-8") as fh:
                data = json.load(fh)
            # Should be base64 encoded
            auth = data["auths"]["quay.io"]["auth"]
            decoded = base64.b64decode(auth).decode()
            self.assertEqual(decoded, "user:pass")

    def test_auth_file_exact_match(self):
        b64 = self._sample_b64()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
            json.dump({"auths": {"quay.io/foo": {"auth": b64}}}, tmp)
            auth_path = tmp.name

        try:
            with RegistryConfig(
                registries=[
                    Registry("quay.io/foo", auth_file=auth_path),
                ]
            ) as auth_file:
                with open(auth_file, encoding="utf-8") as fh:
                    data = json.load(fh)
                self.assertEqual(data["auths"]["quay.io/foo"]["auth"], b64)
        finally:
            os.unlink(auth_path)

    def test_auth_file_no_match_raises(self):
        b64 = self._sample_b64()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
            json.dump({"auths": {"quay.io": {"auth": b64}}}, tmp)
            auth_path = tmp.name

        try:
            # Requesting quay.io/foo but file only has quay.io - should fail
            with self.assertRaises(ValueError) as cm:
                with RegistryConfig(
                    registries=[
                        Registry("quay.io/foo", auth_file=auth_path),
                    ]
                ):
                    pass
            self.assertIn("not found in auth file", str(cm.exception))
        finally:
            os.unlink(auth_path)

    def test_first_source_wins(self):
        """Test that first source wins for duplicate registry paths"""
        b64_first = base64.b64encode(b"first:cred").decode()
        b64_second = base64.b64encode(b"second:cred").decode()

        with RegistryConfig(
            registries=[
                Registry("quay.io", base64_auth=b64_first),
                Registry("quay.io", base64_auth=b64_second),  # Ignored
            ]
        ) as auth_file:
            with open(auth_file, encoding="utf-8") as fh:
                data = json.load(fh)
            # Should use first
            self.assertEqual(data["auths"]["quay.io"]["auth"], b64_first)

    def test_multiple_registries(self):
        b64_quay = base64.b64encode(b"quay:cred").decode()
        b64_rh = base64.b64encode(b"rh:cred").decode()

        with RegistryConfig(
            registries=[
                Registry("quay.io", base64_auth=b64_quay),
                Registry("registry.redhat.io", base64_auth=b64_rh),
            ]
        ) as auth_file:
            with open(auth_file, encoding="utf-8") as fh:
                data = json.load(fh)
            self.assertEqual(data["auths"]["quay.io"]["auth"], b64_quay)
            self.assertEqual(data["auths"]["registry.redhat.io"]["auth"], b64_rh)

    def test_empty_registries_raises(self):
        with self.assertRaises(ValueError):
            with RegistryConfig(registries=[]):
                pass

    def test_cleanup_on_exit(self):
        b64 = self._sample_b64()
        path_holder = []

        with RegistryConfig(
            registries=[
                Registry("quay.io", base64_auth=b64),
            ]
        ) as auth_file:
            path_holder.append(auth_file)
            self.assertTrue(os.path.exists(auth_file))

        # File should be cleaned up
        self.assertFalse(os.path.exists(path_holder[0]))

    def test_cleanup_on_exception(self):
        b64 = self._sample_b64()
        path_seen = None

        try:
            with RegistryConfig(
                registries=[
                    Registry("quay.io", base64_auth=b64),
                ]
            ) as auth_file:
                path_seen = auth_file
                raise RuntimeError("boom")
        except RuntimeError:
            pass

        self.assertIsNotNone(path_seen)
        self.assertFalse(os.path.exists(path_seen))

    def test_get_registries_method(self):
        b64 = self._sample_b64()
        ctx = RegistryConfig(
            registries=[
                Registry("quay.io/foo", base64_auth=b64),
                Registry("registry.redhat.io", base64_auth=b64),
            ]
        )
        ctx.__enter__()
        try:
            registries = ctx.get_registries()
            self.assertIn("quay.io/foo", registries)
            self.assertIn("registry.redhat.io", registries)
            self.assertEqual(len(registries), 2)
        finally:
            ctx.__exit__(None, None, None)

    def test_has_credential_for_method(self):
        b64 = self._sample_b64()
        ctx = RegistryConfig(
            registries=[
                Registry("quay.io", base64_auth=b64),
            ]
        )
        ctx.__enter__()
        try:
            self.assertTrue(ctx.has_credential_for("quay.io"))
            # No prefix matching - exact only
            self.assertFalse(ctx.has_credential_for("quay.io/openshift"))
            self.assertFalse(ctx.has_credential_for("registry.redhat.io"))
        finally:
            ctx.__exit__(None, None, None)

    def test_auth_file_not_found_raises(self):
        with self.assertRaises(FileNotFoundError):
            with RegistryConfig(
                registries=[
                    Registry("quay.io", auth_file="/nonexistent/file.json"),
                ]
            ):
                pass

    def test_empty_base64_auth_raises(self):
        with self.assertRaises(ValueError):
            with RegistryConfig(
                registries=[
                    Registry("quay.io", base64_auth="   "),
                ]
            ):
                pass

    def test_empty_username_password_raises(self):
        with self.assertRaises(ValueError):
            with RegistryConfig(
                registries=[
                    Registry("quay.io", username_password="   "),
                ]
            ):
                pass


class TestCommonReadOnlyRegistries(unittest.TestCase):
    def test_returns_list_of_registries(self):
        # Set env vars
        os.environ["QUAY_AUTH_FILE"] = "/path/to/quay.json"
        os.environ["REGISTRY_REDHAT_IO_AUTH_FILE"] = "/path/to/rh.json"

        try:
            config = COMMON_READ_ONLY_REGISTRIES()
            self.assertIsInstance(config, list)
            self.assertTrue(all(isinstance(item, Registry) for item in config))

            # Should have entries for quay.io
            paths = [reg.path for reg in config]
            self.assertIn("quay.io/openshift-release-dev", paths)
            self.assertIn("quay.io", paths)
            self.assertIn("registry.redhat.io", paths)
        finally:
            os.environ.pop("QUAY_AUTH_FILE", None)
            os.environ.pop("REGISTRY_REDHAT_IO_AUTH_FILE", None)

    def test_handles_missing_env_vars(self):
        # Clear env vars
        os.environ.pop("QUAY_AUTH_FILE", None)
        os.environ.pop("REGISTRY_REDHAT_IO_AUTH_FILE", None)

        config = COMMON_READ_ONLY_REGISTRIES()
        # Should return empty list when env vars not set
        self.assertIsInstance(config, list)
        self.assertEqual(len(config), 0)

    def test_quay_only(self):
        os.environ["QUAY_AUTH_FILE"] = "/path/to/quay.json"
        os.environ.pop("REGISTRY_REDHAT_IO_AUTH_FILE", None)

        try:
            config = COMMON_READ_ONLY_REGISTRIES()
            paths = [reg.path for reg in config]
            self.assertIn("quay.io/openshift-release-dev", paths)
            self.assertIn("quay.io", paths)
            self.assertNotIn("registry.redhat.io", paths)
        finally:
            os.environ.pop("QUAY_AUTH_FILE", None)
