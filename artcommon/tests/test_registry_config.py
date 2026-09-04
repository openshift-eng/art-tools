import base64
import json
import os
import stat
import tempfile
import unittest

from artcommonlib.registry_config import (
    RegistryConfig,
    RegistryCredential,
    _normalize_registry_path,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_auth_file(auths: dict[str, dict]) -> str:
    """
    Write a temporary auth.json file and return its path.

    The caller is responsible for cleaning up the file.
    """
    fd, path = tempfile.mkstemp(suffix=".json", text=True)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump({"auths": auths}, f)
    return path


def _b64(username: str, password: str) -> str:
    """
    Return the base64-encoded "username:password" string.
    """
    return base64.b64encode(f"{username}:{password}".encode()).decode()


def _read_auth_file(path: str) -> dict:
    """
    Read and parse a Docker auth config.json file.
    """
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# _normalize_registry_path
# ---------------------------------------------------------------------------


class TestNormalizeRegistryPath(unittest.TestCase):
    def test_lowercase_host(self):
        self.assertEqual(_normalize_registry_path("QUAY.IO"), "quay.io")

    def test_preserves_path_case(self):
        self.assertEqual(
            _normalize_registry_path("Quay.IO/OpenShift/CI"),
            "quay.io/OpenShift/CI",
        )

    def test_strips_docker_scheme(self):
        self.assertEqual(
            _normalize_registry_path("docker://quay.io/foo"),
            "quay.io/foo",
        )

    def test_strips_https_scheme(self):
        self.assertEqual(
            _normalize_registry_path("https://registry.redhat.io"),
            "registry.redhat.io",
        )

    def test_strips_http_scheme(self):
        self.assertEqual(
            _normalize_registry_path("http://localhost:5000/repo"),
            "localhost:5000/repo",
        )

    def test_strips_oci_scheme(self):
        self.assertEqual(
            _normalize_registry_path("oci://quay.io/foo"),
            "quay.io/foo",
        )

    def test_strips_digest(self):
        self.assertEqual(
            _normalize_registry_path("quay.io/foo@sha256:abc123"),
            "quay.io/foo",
        )

    def test_strips_trailing_slash(self):
        self.assertEqual(
            _normalize_registry_path("quay.io/foo/"),
            "quay.io/foo",
        )

    def test_combined_normalization(self):
        """Scheme + uppercase host + trailing slash + digest all at once."""
        self.assertEqual(
            _normalize_registry_path("https://QUAY.IO/Foo/@sha256:abc"),
            "quay.io/Foo",
        )

    def test_host_only(self):
        self.assertEqual(_normalize_registry_path("quay.io"), "quay.io")

    def test_host_with_port(self):
        self.assertEqual(
            _normalize_registry_path("Localhost:5000"),
            "localhost:5000",
        )

    def test_empty_string_raises(self):
        with self.assertRaises(ValueError):
            _normalize_registry_path("")

    def test_whitespace_only_raises(self):
        with self.assertRaises(ValueError):
            _normalize_registry_path("   ")

    def test_scheme_only_raises(self):
        with self.assertRaises(ValueError):
            _normalize_registry_path("https://")

    def test_strips_leading_trailing_whitespace(self):
        self.assertEqual(
            _normalize_registry_path("  quay.io/foo  "),
            "quay.io/foo",
        )


# ---------------------------------------------------------------------------
# RegistryCredential
# ---------------------------------------------------------------------------


class TestRegistryCredential(unittest.TestCase):
    def test_valid_credential(self):
        cred = RegistryCredential("quay.io/qci", "user", "password")
        self.assertEqual(cred.registry, "quay.io/qci")
        self.assertEqual(cred.username, "user")
        self.assertEqual(cred.password, "password")

    def test_normalizes_registry(self):
        cred = RegistryCredential("QUAY.IO/Foo", "user", "pass")
        self.assertEqual(cred.registry, "quay.io/Foo")

    def test_strips_scheme_from_registry(self):
        cred = RegistryCredential("docker://quay.io/foo", "u", "p")
        self.assertEqual(cred.registry, "quay.io/foo")

    def test_empty_registry_raises(self):
        with self.assertRaises(ValueError):
            RegistryCredential("", "user", "pass")

    def test_whitespace_registry_raises(self):
        with self.assertRaises(ValueError):
            RegistryCredential("   ", "user", "pass")

    def test_empty_username_raises(self):
        with self.assertRaises(ValueError):
            RegistryCredential("quay.io", "", "pass")

    def test_whitespace_username_raises(self):
        with self.assertRaises(ValueError):
            RegistryCredential("quay.io", "   ", "pass")

    def test_empty_password_raises(self):
        with self.assertRaises(ValueError):
            RegistryCredential("quay.io", "user", "")

    def test_none_registry_raises(self):
        with self.assertRaises(ValueError):
            RegistryCredential(None, "user", "pass")

    def test_none_username_raises(self):
        with self.assertRaises(ValueError):
            RegistryCredential("quay.io", None, "pass")

    def test_none_password_raises(self):
        with self.assertRaises(ValueError):
            RegistryCredential("quay.io", "user", None)


# ---------------------------------------------------------------------------
# RegistryConfig — source files only
# ---------------------------------------------------------------------------


class TestRegistryConfigSourceFilesOnly(unittest.TestCase):
    def test_single_file_single_registry(self):
        auth_path = _write_auth_file(
            {
                "quay.io": {"auth": _b64("u", "p")},
                "registry.redhat.io": {"auth": _b64("r", "s")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[auth_path],
                registries=["quay.io"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertIn("quay.io", data["auths"])
                self.assertEqual(len(data["auths"]), 1)
        finally:
            os.unlink(auth_path)

    def test_single_file_multiple_registries(self):
        auth_path = _write_auth_file(
            {
                "quay.io": {"auth": _b64("u", "p")},
                "registry.redhat.io": {"auth": _b64("r", "s")},
                "gcr.io": {"auth": _b64("g", "h")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[auth_path],
                registries=["quay.io", "registry.redhat.io"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertEqual(len(data["auths"]), 2)
                self.assertIn("quay.io", data["auths"])
                self.assertIn("registry.redhat.io", data["auths"])
                # gcr.io should NOT be included
                self.assertNotIn("gcr.io", data["auths"])
        finally:
            os.unlink(auth_path)

    def test_cherry_pick_subset(self):
        """Only the requested registries are included, not the entire source."""
        auth_path = _write_auth_file(
            {
                "quay.io/openshift-release-dev": {"auth": _b64("a", "b")},
                "quay.io/redhat-user-workloads/ocp-art-tenant/art-images": {"auth": _b64("c", "d")},
                "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc": {"auth": _b64("e", "f")},
                "registry.redhat.io": {"auth": _b64("g", "h")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[auth_path],
                registries=[
                    "registry.redhat.io",
                    "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc",
                ],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertEqual(len(data["auths"]), 2)
                self.assertIn("registry.redhat.io", data["auths"])
                self.assertIn(
                    "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc",
                    data["auths"],
                )
        finally:
            os.unlink(auth_path)

    def test_multiple_files_registry_in_first(self):
        path1 = _write_auth_file({"quay.io": {"auth": _b64("first", "cred")}})
        path2 = _write_auth_file({"registry.redhat.io": {"auth": _b64("r", "s")}})
        try:
            with RegistryConfig(
                source_files=[path1, path2],
                registries=["quay.io"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                decoded = base64.b64decode(data["auths"]["quay.io"]["auth"]).decode()
                self.assertEqual(decoded, "first:cred")
        finally:
            os.unlink(path1)
            os.unlink(path2)

    def test_multiple_files_registry_in_second(self):
        path1 = _write_auth_file({"quay.io": {"auth": _b64("q", "p")}})
        path2 = _write_auth_file({"registry.redhat.io": {"auth": _b64("rh", "pw")}})
        try:
            with RegistryConfig(
                source_files=[path1, path2],
                registries=["registry.redhat.io"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                decoded = base64.b64decode(data["auths"]["registry.redhat.io"]["auth"]).decode()
                self.assertEqual(decoded, "rh:pw")
        finally:
            os.unlink(path1)
            os.unlink(path2)

    def test_multiple_files_first_file_wins(self):
        path1 = _write_auth_file({"quay.io": {"auth": _b64("first", "cred")}})
        path2 = _write_auth_file({"quay.io": {"auth": _b64("second", "cred")}})
        try:
            with RegistryConfig(
                source_files=[path1, path2],
                registries=["quay.io"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                decoded = base64.b64decode(data["auths"]["quay.io"]["auth"]).decode()
                self.assertEqual(decoded, "first:cred")
        finally:
            os.unlink(path1)
            os.unlink(path2)

    def test_registries_from_different_files(self):
        """Each requested registry can come from a different source file."""
        path1 = _write_auth_file({"quay.io": {"auth": _b64("q", "p")}})
        path2 = _write_auth_file({"registry.redhat.io": {"auth": _b64("r", "s")}})
        try:
            with RegistryConfig(
                source_files=[path1, path2],
                registries=["quay.io", "registry.redhat.io"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertEqual(len(data["auths"]), 2)
                self.assertIn("quay.io", data["auths"])
                self.assertIn("registry.redhat.io", data["auths"])
        finally:
            os.unlink(path1)
            os.unlink(path2)

    def test_duplicate_registries_in_list_deduplicated(self):
        auth_path = _write_auth_file({"quay.io": {"auth": _b64("u", "p")}})
        try:
            with RegistryConfig(
                source_files=[auth_path],
                registries=["quay.io", "quay.io"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                # Should only have one entry, not fail
                self.assertEqual(len(data["auths"]), 1)
        finally:
            os.unlink(auth_path)

    def test_registry_not_found_raises(self):
        auth_path = _write_auth_file({"quay.io": {"auth": _b64("u", "p")}})
        try:
            with self.assertRaises(ValueError) as cm:
                with RegistryConfig(
                    source_files=[auth_path],
                    registries=["registry.redhat.io"],
                ):
                    pass
            self.assertIn("not found in any source file", str(cm.exception))
            self.assertIn("quay.io", str(cm.exception))  # lists available keys
        finally:
            os.unlink(auth_path)

    def test_normalizes_source_file_keys_for_matching(self):
        """Source file keys are normalized before matching."""
        auth_path = _write_auth_file(
            {
                "QUAY.IO/Foo": {"auth": _b64("u", "p")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[auth_path],
                registries=["quay.io/Foo"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertIn("quay.io/Foo", data["auths"])
        finally:
            os.unlink(auth_path)

    def test_preserves_auth_value_from_source(self):
        """The auth value from the source file is preserved exactly."""
        original_auth = _b64("myuser", "mypass")
        auth_path = _write_auth_file({"quay.io": {"auth": original_auth}})
        try:
            with RegistryConfig(
                source_files=[auth_path],
                registries=["quay.io"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertEqual(data["auths"]["quay.io"]["auth"], original_auth)
        finally:
            os.unlink(auth_path)


# ---------------------------------------------------------------------------
# RegistryConfig — credentials only
# ---------------------------------------------------------------------------


class TestRegistryConfigCredentialsOnly(unittest.TestCase):
    def test_single_credential(self):
        with RegistryConfig(
            credentials=[RegistryCredential("quay.io/qci", "user", "pass")],
        ) as auth_file:
            data = _read_auth_file(auth_file)
            self.assertEqual(len(data["auths"]), 1)
            decoded = base64.b64decode(data["auths"]["quay.io/qci"]["auth"]).decode()
            self.assertEqual(decoded, "user:pass")

    def test_multiple_credentials(self):
        with RegistryConfig(
            credentials=[
                RegistryCredential("quay.io", "u1", "p1"),
                RegistryCredential("registry.redhat.io", "u2", "p2"),
            ],
        ) as auth_file:
            data = _read_auth_file(auth_file)
            self.assertEqual(len(data["auths"]), 2)
            self.assertIn("quay.io", data["auths"])
            self.assertIn("registry.redhat.io", data["auths"])

    def test_credential_is_base64_encoded(self):
        with RegistryConfig(
            credentials=[RegistryCredential("quay.io", "user", "pass")],
        ) as auth_file:
            data = _read_auth_file(auth_file)
            auth_value = data["auths"]["quay.io"]["auth"]
            decoded = base64.b64decode(auth_value).decode()
            self.assertEqual(decoded, "user:pass")

    def test_credential_with_special_characters(self):
        """Passwords with colons, spaces, and special chars are handled."""
        with RegistryConfig(
            credentials=[
                RegistryCredential("quay.io", "user", "p@ss:w0rd with spaces!"),
            ],
        ) as auth_file:
            data = _read_auth_file(auth_file)
            decoded = base64.b64decode(data["auths"]["quay.io"]["auth"]).decode()
            self.assertEqual(decoded, "user:p@ss:w0rd with spaces!")

    def test_duplicate_credentials_first_wins(self):
        with RegistryConfig(
            credentials=[
                RegistryCredential("quay.io", "first", "cred"),
                RegistryCredential("quay.io", "second", "cred"),
            ],
        ) as auth_file:
            data = _read_auth_file(auth_file)
            decoded = base64.b64decode(data["auths"]["quay.io"]["auth"]).decode()
            self.assertEqual(decoded, "first:cred")

    def test_no_source_files_needed(self):
        """Credentials-only mode does not require source_files."""
        with RegistryConfig(
            credentials=[RegistryCredential("quay.io", "u", "p")],
        ) as auth_file:
            self.assertTrue(os.path.isfile(auth_file))


# ---------------------------------------------------------------------------
# RegistryConfig — combined (source files + credentials)
# ---------------------------------------------------------------------------


class TestRegistryConfigCombined(unittest.TestCase):
    def test_different_registries_merged(self):
        auth_path = _write_auth_file(
            {
                "registry.redhat.io": {"auth": _b64("rh", "pw")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[auth_path],
                registries=["registry.redhat.io"],
                credentials=[RegistryCredential("quay.io/qci", "qci", "pass")],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertEqual(len(data["auths"]), 2)
                self.assertIn("registry.redhat.io", data["auths"])
                self.assertIn("quay.io/qci", data["auths"])
        finally:
            os.unlink(auth_path)

    def test_credential_overrides_source_entry(self):
        """When a registry appears in both source and credentials, credential wins."""
        auth_path = _write_auth_file(
            {
                "quay.io": {"auth": _b64("source-user", "source-pass")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[auth_path],
                registries=["quay.io"],
                credentials=[RegistryCredential("quay.io", "cred-user", "cred-pass")],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                decoded = base64.b64decode(data["auths"]["quay.io"]["auth"]).decode()
                # Credential should override the source file
                self.assertEqual(decoded, "cred-user:cred-pass")
        finally:
            os.unlink(auth_path)

    def test_mixed_sources_and_credentials(self):
        """Real-world scenario: multiple source files + explicit credentials."""
        path1 = _write_auth_file(
            {
                "quay.io/openshift-release-dev": {"auth": _b64("q1", "p1")},
                "quay.io": {"auth": _b64("q2", "p2")},
            }
        )
        path2 = _write_auth_file(
            {
                "registry.ci.openshift.org": {"auth": _b64("ci", "pw")},
                "quay.io/openshift": {"auth": _b64("qo", "pw")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[path1, path2],
                registries=[
                    "quay.io/openshift-release-dev",
                    "quay.io",
                    "registry.ci.openshift.org",
                    "quay.io/openshift",
                ],
                credentials=[
                    RegistryCredential("quay.io/special", "sp", "pw"),
                ],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertEqual(len(data["auths"]), 5)
                self.assertIn("quay.io/openshift-release-dev", data["auths"])
                self.assertIn("quay.io", data["auths"])
                self.assertIn("registry.ci.openshift.org", data["auths"])
                self.assertIn("quay.io/openshift", data["auths"])
                self.assertIn("quay.io/special", data["auths"])
        finally:
            os.unlink(path1)
            os.unlink(path2)

    def test_source_files_not_required_when_only_credentials(self):
        """source_files can be omitted when only credentials are used."""
        with RegistryConfig(
            credentials=[
                RegistryCredential("quay.io", "u", "p"),
                RegistryCredential("registry.redhat.io", "r", "s"),
            ],
        ) as auth_file:
            data = _read_auth_file(auth_file)
            self.assertEqual(len(data["auths"]), 2)

    def test_credential_for_registry_not_in_sources(self):
        """A credential can add a registry that does not appear in any source file."""
        auth_path = _write_auth_file(
            {
                "quay.io": {"auth": _b64("u", "p")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[auth_path],
                registries=["quay.io"],
                credentials=[
                    RegistryCredential("registry.redhat.io", "rh", "pw"),
                ],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertEqual(len(data["auths"]), 2)
                self.assertIn("quay.io", data["auths"])
                self.assertIn("registry.redhat.io", data["auths"])
        finally:
            os.unlink(auth_path)


# ---------------------------------------------------------------------------
# RegistryConfig — validation and error cases
# ---------------------------------------------------------------------------


class TestRegistryConfigValidation(unittest.TestCase):
    def test_no_registries_no_credentials_raises(self):
        with self.assertRaises(ValueError) as cm:
            RegistryConfig()
        self.assertIn("at least one", str(cm.exception))

    def test_empty_registries_no_credentials_raises(self):
        with self.assertRaises(ValueError):
            RegistryConfig(registries=[])

    def test_registries_without_source_files_raises(self):
        with self.assertRaises(ValueError) as cm:
            RegistryConfig(registries=["quay.io"])
        self.assertIn("source_files or kubeconfig must be provided", str(cm.exception))

    def test_registries_with_empty_source_files_raises(self):
        with self.assertRaises(ValueError):
            RegistryConfig(source_files=[], registries=["quay.io"])

    def test_source_file_not_found_raises(self):
        with self.assertRaises(FileNotFoundError):
            with RegistryConfig(
                source_files=["/nonexistent/auth.json"],
                registries=["quay.io"],
            ):
                pass

    def test_source_file_invalid_json_raises(self):
        fd, path = tempfile.mkstemp(suffix=".json", text=True)
        with os.fdopen(fd, "w") as f:
            f.write("not valid json{{{")
        try:
            with self.assertRaises(ValueError) as cm:
                with RegistryConfig(
                    source_files=[path],
                    registries=["quay.io"],
                ):
                    pass
            self.assertIn("Invalid JSON", str(cm.exception))
        finally:
            os.unlink(path)

    def test_source_file_missing_auths_key_raises(self):
        fd, path = tempfile.mkstemp(suffix=".json", text=True)
        with os.fdopen(fd, "w") as f:
            json.dump({"something_else": {}}, f)
        try:
            with self.assertRaises(ValueError) as cm:
                with RegistryConfig(
                    source_files=[path],
                    registries=["quay.io"],
                ):
                    pass
            self.assertIn("missing 'auths' key", str(cm.exception))
        finally:
            os.unlink(path)

    def test_source_file_is_array_raises(self):
        """A source file whose root is a JSON array is not valid."""
        fd, path = tempfile.mkstemp(suffix=".json", text=True)
        with os.fdopen(fd, "w") as f:
            json.dump([{"auths": {}}], f)
        try:
            with self.assertRaises(ValueError):
                with RegistryConfig(
                    source_files=[path],
                    registries=["quay.io"],
                ):
                    pass
        finally:
            os.unlink(path)

    def test_empty_registry_in_list_raises(self):
        auth_path = _write_auth_file({"quay.io": {"auth": _b64("u", "p")}})
        try:
            with self.assertRaises(ValueError):
                RegistryConfig(
                    source_files=[auth_path],
                    registries=[""],
                )
        finally:
            os.unlink(auth_path)

    def test_whitespace_registry_in_list_raises(self):
        auth_path = _write_auth_file({"quay.io": {"auth": _b64("u", "p")}})
        try:
            with self.assertRaises(ValueError):
                RegistryConfig(
                    source_files=[auth_path],
                    registries=["   "],
                )
        finally:
            os.unlink(auth_path)

    def test_source_file_with_empty_auth_value_skipped(self):
        """A source entry with an empty auth value is treated as not found."""
        auth_path = _write_auth_file({"quay.io": {"auth": ""}})
        try:
            with self.assertRaises(ValueError) as cm:
                with RegistryConfig(
                    source_files=[auth_path],
                    registries=["quay.io"],
                ):
                    pass
            self.assertIn("not found", str(cm.exception))
        finally:
            os.unlink(auth_path)

    def test_source_file_with_missing_auth_key_skipped(self):
        """A source entry without an 'auth' key is treated as not found."""
        auth_path = _write_auth_file({"quay.io": {"email": "foo@bar.com"}})
        try:
            with self.assertRaises(ValueError) as cm:
                with RegistryConfig(
                    source_files=[auth_path],
                    registries=["quay.io"],
                ):
                    pass
            self.assertIn("not found", str(cm.exception))
        finally:
            os.unlink(auth_path)


# ---------------------------------------------------------------------------
# RegistryConfig — context manager lifecycle
# ---------------------------------------------------------------------------


class TestRegistryConfigContextManager(unittest.TestCase):
    def test_file_exists_during_context(self):
        with RegistryConfig(
            credentials=[RegistryCredential("quay.io", "u", "p")],
        ) as auth_file:
            self.assertTrue(os.path.isfile(auth_file))

    def test_file_cleaned_up_after_exit(self):
        path_holder = []
        with RegistryConfig(
            credentials=[RegistryCredential("quay.io", "u", "p")],
        ) as auth_file:
            path_holder.append(auth_file)
            self.assertTrue(os.path.exists(auth_file))
        self.assertFalse(os.path.exists(path_holder[0]))

    def test_file_cleaned_up_after_exception(self):
        path_seen = None
        try:
            with RegistryConfig(
                credentials=[RegistryCredential("quay.io", "u", "p")],
            ) as auth_file:
                path_seen = auth_file
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        self.assertIsNotNone(path_seen)
        self.assertFalse(os.path.exists(path_seen))

    def test_output_json_structure(self):
        """Output file has the standard Docker config.json structure."""
        with RegistryConfig(
            credentials=[RegistryCredential("quay.io", "u", "p")],
        ) as auth_file:
            data = _read_auth_file(auth_file)
            self.assertIn("auths", data)
            self.assertIsInstance(data["auths"], dict)
            for key, value in data["auths"].items():
                self.assertIn("auth", value)

    def test_file_permissions_restrictive(self):
        """Temp file should be created with restrictive permissions (0600)."""
        with RegistryConfig(
            credentials=[RegistryCredential("quay.io", "u", "p")],
        ) as auth_file:
            mode = os.stat(auth_file).st_mode
            # Check that group and others have no permissions
            self.assertEqual(mode & stat.S_IRWXG, 0)
            self.assertEqual(mode & stat.S_IRWXO, 0)

    def test_get_registries_inside_context(self):
        ctx = RegistryConfig(
            credentials=[
                RegistryCredential("quay.io", "u1", "p1"),
                RegistryCredential("registry.redhat.io", "u2", "p2"),
            ],
        )
        ctx.__enter__()
        try:
            registries = ctx.get_registries()
            self.assertIn("quay.io", registries)
            self.assertIn("registry.redhat.io", registries)
            self.assertEqual(len(registries), 2)
        finally:
            ctx.__exit__(None, None, None)

    def test_get_registries_outside_context_empty(self):
        ctx = RegistryConfig(
            credentials=[RegistryCredential("quay.io", "u", "p")],
        )
        # Before entering context
        self.assertEqual(ctx.get_registries(), [])

    def test_has_credential_for_exact_match_only(self):
        with RegistryConfig(
            credentials=[RegistryCredential("quay.io/openshift", "u", "p")],
        ) as auth_file:
            ctx = RegistryConfig.__new__(RegistryConfig)
            ctx._merged_auths = _read_auth_file(auth_file)["auths"]
            # Re-enter properly for a clean test
        ctx2 = RegistryConfig(
            credentials=[RegistryCredential("quay.io/openshift", "u", "p")],
        )
        ctx2.__enter__()
        try:
            self.assertTrue(ctx2.has_credential_for("quay.io/openshift"))
            # No prefix/parent matching
            self.assertFalse(ctx2.has_credential_for("quay.io"))
            self.assertFalse(ctx2.has_credential_for("quay.io/openshift/ci"))
            self.assertFalse(ctx2.has_credential_for("registry.redhat.io"))
        finally:
            ctx2.__exit__(None, None, None)

    def test_get_registries_cleared_after_exit(self):
        ctx = RegistryConfig(
            credentials=[RegistryCredential("quay.io", "u", "p")],
        )
        ctx.__enter__()
        self.assertEqual(len(ctx.get_registries()), 1)
        ctx.__exit__(None, None, None)
        self.assertEqual(ctx.get_registries(), [])


# ---------------------------------------------------------------------------
# RegistryConfig — real-world scenarios
# ---------------------------------------------------------------------------


class TestRegistryConfigAliases(unittest.TestCase):
    """Test registry alias functionality for shared authentication realms."""

    def test_brew_registry_uses_redhat_registry_credentials(self):
        """Test that brew.registry.redhat.io automatically uses registry.redhat.io credentials."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            source_file = f.name
            json.dump(
                {
                    "auths": {
                        "registry.redhat.io": {"auth": "cmVkaGF0OnBhc3N3b3Jk"}  # redhat:password
                    }
                },
                f,
            )

        try:
            with RegistryConfig(
                source_files=[source_file],
                registries=['brew.registry.redhat.io'],  # Request brew, but only registry.redhat.io is in source
            ) as auth_file:
                with open(auth_file) as f:
                    result = json.load(f)

                # Should have brew.registry.redhat.io entry using registry.redhat.io credentials
                self.assertIn('brew.registry.redhat.io', result['auths'])
                self.assertEqual(result['auths']['brew.registry.redhat.io']['auth'], 'cmVkaGF0OnBhc3N3b3Jk')
        finally:
            os.unlink(source_file)

    def test_both_brew_and_redhat_registry_requested(self):
        """Test requesting both brew.registry.redhat.io and registry.redhat.io."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            source_file = f.name
            json.dump({"auths": {"registry.redhat.io": {"auth": "cmVkaGF0OnBhc3N3b3Jk"}}}, f)

        try:
            with RegistryConfig(
                source_files=[source_file],
                registries=['registry.redhat.io', 'brew.registry.redhat.io'],
            ) as auth_file:
                with open(auth_file) as f:
                    result = json.load(f)

                # Both should be present with the same credentials
                self.assertIn('registry.redhat.io', result['auths'])
                self.assertIn('brew.registry.redhat.io', result['auths'])
                self.assertEqual(
                    result['auths']['registry.redhat.io']['auth'], result['auths']['brew.registry.redhat.io']['auth']
                )
        finally:
            os.unlink(source_file)

    def test_brew_registry_not_found_without_fallback_raises(self):
        """Test that requesting brew.registry.redhat.io fails if registry.redhat.io is also missing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            source_file = f.name
            json.dump({"auths": {"quay.io": {"auth": "cXVheTpwYXNzd29yZA=="}}}, f)

        try:
            with self.assertRaises(ValueError) as ctx:
                with RegistryConfig(
                    source_files=[source_file],
                    registries=['brew.registry.redhat.io'],
                ):
                    pass

            self.assertIn('brew.registry.redhat.io', str(ctx.exception))
            self.assertIn('not found', str(ctx.exception))
        finally:
            os.unlink(source_file)


class TestRegistryConfigRealWorldScenarios(unittest.TestCase):
    def test_art_pipeline_scenario(self):
        """
        Simulates the ocp4-konflux mirror_streams_to_ci use case:
        - Source files: QUAY_AUTH_FILE + oc-login temp file
        - Registries: quay.io/openshift-release-dev, quay.io, registry.ci, quay.io/openshift
        """
        quay_auth = _write_auth_file(
            {
                "quay.io/openshift-release-dev": {"auth": _b64("quay-dev", "pw")},
                "quay.io": {"auth": _b64("quay-general", "pw")},
                "quay.io/redhat-user-workloads/ocp-art-tenant/art-images": {"auth": _b64("art", "pw")},
            }
        )
        ci_auth = _write_auth_file(
            {
                "registry.ci.openshift.org": {"auth": _b64("ci-user", "ci-pw")},
                "quay.io/openshift": {"auth": _b64("qci-user", "qci-pw")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[quay_auth, ci_auth],
                registries=[
                    "quay.io/openshift-release-dev",
                    "quay.io",
                    "registry.ci.openshift.org",
                    "quay.io/openshift",
                ],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertEqual(len(data["auths"]), 4)

                # Verify each credential came from the correct file
                self.assertEqual(
                    base64.b64decode(data["auths"]["quay.io/openshift-release-dev"]["auth"]).decode(),
                    "quay-dev:pw",
                )
                self.assertEqual(
                    base64.b64decode(data["auths"]["quay.io"]["auth"]).decode(),
                    "quay-general:pw",
                )
                self.assertEqual(
                    base64.b64decode(data["auths"]["registry.ci.openshift.org"]["auth"]).decode(),
                    "ci-user:ci-pw",
                )
                self.assertEqual(
                    base64.b64decode(data["auths"]["quay.io/openshift"]["auth"]).decode(),
                    "qci-user:qci-pw",
                )

                # art-images should NOT be in the output
                self.assertNotIn(
                    "quay.io/redhat-user-workloads/ocp-art-tenant/art-images",
                    data["auths"],
                )
        finally:
            os.unlink(quay_auth)
            os.unlink(ci_auth)

    def test_credential_plus_source_files_scenario(self):
        """
        Simulates: extract from auth file + add QCI creds explicitly.
        """
        quay_auth = _write_auth_file(
            {
                "quay.io/openshift-release-dev": {"auth": _b64("dev", "pw")},
                "quay.io": {"auth": _b64("quay", "pw")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[quay_auth],
                registries=["quay.io/openshift-release-dev", "quay.io"],
                credentials=[
                    RegistryCredential("quay.io/openshift", "qci-user", "qci-pw"),
                    RegistryCredential("registry.ci.openshift.org", "ci", "pw"),
                ],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertEqual(len(data["auths"]), 4)

                # From source
                self.assertEqual(
                    base64.b64decode(data["auths"]["quay.io/openshift-release-dev"]["auth"]).decode(),
                    "dev:pw",
                )
                # From credential
                self.assertEqual(
                    base64.b64decode(data["auths"]["quay.io/openshift"]["auth"]).decode(),
                    "qci-user:qci-pw",
                )
        finally:
            os.unlink(quay_auth)

    def test_override_source_with_credential_scenario(self):
        """
        Source file has general quay.io creds, but we override with
        specific push credentials.
        """
        quay_auth = _write_auth_file(
            {
                "quay.io": {"auth": _b64("readonly", "pw")},
            }
        )
        try:
            with RegistryConfig(
                source_files=[quay_auth],
                registries=["quay.io"],
                credentials=[
                    RegistryCredential("quay.io", "push-user", "push-pw"),
                ],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                decoded = base64.b64decode(data["auths"]["quay.io"]["auth"]).decode()
                # Credential should override the source
                self.assertEqual(decoded, "push-user:push-pw")
        finally:
            os.unlink(quay_auth)

    def test_empty_source_file_with_credentials_only(self):
        """
        source_files can be omitted entirely when only credentials are used.
        """
        with RegistryConfig(
            credentials=[
                RegistryCredential("quay.io/openshift-release-dev", "dev", "pw"),
                RegistryCredential("registry.redhat.io", "rh", "pw"),
                RegistryCredential("quay.io/qci", "qci", "pw"),
            ],
        ) as auth_file:
            data = _read_auth_file(auth_file)
            self.assertEqual(len(data["auths"]), 3)


# ---------------------------------------------------------------------------
# RegistryConfig — kubeconfig support
# ---------------------------------------------------------------------------


def _mock_cmd_gather_success(auths_to_write: dict):
    """
    Return a mock for exectools.cmd_gather that simulates a successful
    ``oc registry login --to=<path>`` by writing credentials to the --to path.
    """

    def _mock(cmd, **kwargs):
        if isinstance(cmd, str):
            parts = cmd.split()
        else:
            parts = cmd
        to_path = None
        for part in parts:
            if part.startswith("--to="):
                to_path = part[5:]
                break
        if to_path:
            with open(to_path, "w") as f:
                json.dump({"auths": auths_to_write}, f)
        return (0, "", "")

    return _mock


class TestRegistryConfigKubeconfig(unittest.TestCase):
    """
    Tests for the kubeconfig parameter.
    """

    def test_kubeconfig_only_registries_no_source_files(self):
        """
        kubeconfig alone satisfies the source requirement for registries.
        """
        ci_registry_auth = _b64("ci-user", "ci-pass")

        from unittest.mock import patch

        mock = _mock_cmd_gather_success({"registry.ci.openshift.org": {"auth": ci_registry_auth}})
        with patch("artcommonlib.registry_config.exectools.cmd_gather", side_effect=mock):
            with RegistryConfig(
                kubeconfig="/fake/kubeconfig",
                registries=["registry.ci.openshift.org"],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                self.assertIn("registry.ci.openshift.org", data["auths"])
                self.assertEqual(data["auths"]["registry.ci.openshift.org"]["auth"], ci_registry_auth)

    def test_kubeconfig_combined_with_source_files(self):
        """
        kubeconfig credentials are merged with existing source files.
        """
        quay_auth_path = _write_auth_file(
            {
                "quay.io/openshift-release-dev": {"auth": _b64("quay-user", "quay-pass")},
            }
        )
        ci_registry_auth = _b64("ci-user", "ci-pass")

        from unittest.mock import patch

        mock = _mock_cmd_gather_success({"registry.ci.openshift.org": {"auth": ci_registry_auth}})
        try:
            with patch("artcommonlib.registry_config.exectools.cmd_gather", side_effect=mock):
                with RegistryConfig(
                    kubeconfig="/fake/kubeconfig",
                    source_files=[quay_auth_path],
                    registries=[
                        "quay.io/openshift-release-dev",
                        "registry.ci.openshift.org",
                    ],
                ) as auth_file:
                    data = _read_auth_file(auth_file)
                    self.assertEqual(len(data["auths"]), 2)
                    self.assertIn("quay.io/openshift-release-dev", data["auths"])
                    self.assertIn("registry.ci.openshift.org", data["auths"])
                    decoded_quay = base64.b64decode(data["auths"]["quay.io/openshift-release-dev"]["auth"]).decode()
                    self.assertEqual(decoded_quay, "quay-user:quay-pass")
        finally:
            os.unlink(quay_auth_path)

    def test_kubeconfig_combined_with_credentials(self):
        """
        Explicit credentials override kubeconfig-sourced entries.
        """
        ci_registry_auth = _b64("ci-from-kubeconfig", "kube-pass")

        from unittest.mock import patch

        mock = _mock_cmd_gather_success({"registry.ci.openshift.org": {"auth": ci_registry_auth}})
        with patch("artcommonlib.registry_config.exectools.cmd_gather", side_effect=mock):
            with RegistryConfig(
                kubeconfig="/fake/kubeconfig",
                registries=["registry.ci.openshift.org"],
                credentials=[
                    RegistryCredential("registry.ci.openshift.org", "override-user", "override-pass"),
                ],
            ) as auth_file:
                data = _read_auth_file(auth_file)
                decoded = base64.b64decode(data["auths"]["registry.ci.openshift.org"]["auth"]).decode()
                self.assertEqual(decoded, "override-user:override-pass")

    def test_kubeconfig_passes_correct_command(self):
        """
        Verify the oc command includes the kubeconfig path.
        """
        captured_cmd = []

        def mock_cmd_gather(cmd, **kwargs):
            captured_cmd.append(cmd)
            if isinstance(cmd, str):
                parts = cmd.split()
            else:
                parts = cmd
            to_path = None
            for part in parts:
                if part.startswith("--to="):
                    to_path = part[5:]
                    break
            if to_path:
                with open(to_path, "w") as f:
                    json.dump({"auths": {"registry.ci.openshift.org": {"auth": _b64("u", "p")}}}, f)
            return (0, "", "")

        from unittest.mock import patch

        with patch("artcommonlib.registry_config.exectools.cmd_gather", side_effect=mock_cmd_gather):
            with RegistryConfig(
                kubeconfig="/my/custom/kubeconfig",
                registries=["registry.ci.openshift.org"],
            ):
                pass

        self.assertEqual(len(captured_cmd), 1)
        self.assertIn("/my/custom/kubeconfig", captured_cmd[0])
        self.assertIn("registry login", captured_cmd[0])

    def test_kubeconfig_failure_raises_runtime_error(self):
        """
        If oc registry login fails, a RuntimeError is raised.
        """

        def mock_cmd_gather(cmd, **kwargs):
            return (1, "", "error: dial tcp: connection refused")

        from unittest.mock import patch

        with patch("artcommonlib.registry_config.exectools.cmd_gather", side_effect=mock_cmd_gather):
            with self.assertRaises(RuntimeError) as cm:
                with RegistryConfig(
                    kubeconfig="/bad/kubeconfig",
                    registries=["registry.ci.openshift.org"],
                ):
                    pass
            self.assertIn("Failed to run", str(cm.exception))
            self.assertIn("/bad/kubeconfig", str(cm.exception))

    def test_kubeconfig_temp_files_cleaned_up(self):
        """
        Both the merged auth file and the kubeconfig temp file are cleaned up on exit.
        """
        ci_registry_auth = _b64("ci-user", "ci-pass")

        from unittest.mock import patch

        mock = _mock_cmd_gather_success({"registry.ci.openshift.org": {"auth": ci_registry_auth}})
        auth_file_path = None
        with patch("artcommonlib.registry_config.exectools.cmd_gather", side_effect=mock):
            with RegistryConfig(
                kubeconfig="/fake/kubeconfig",
                registries=["registry.ci.openshift.org"],
            ) as auth_file:
                auth_file_path = auth_file
                self.assertTrue(os.path.exists(auth_file))

        self.assertFalse(os.path.exists(auth_file_path))

    def test_kubeconfig_temp_files_cleaned_up_on_exception(self):
        """
        Temp files are cleaned up even if an exception occurs inside the context.
        """
        ci_registry_auth = _b64("ci-user", "ci-pass")

        from unittest.mock import patch

        mock = _mock_cmd_gather_success({"registry.ci.openshift.org": {"auth": ci_registry_auth}})
        auth_file_path = None
        try:
            with patch("artcommonlib.registry_config.exectools.cmd_gather", side_effect=mock):
                with RegistryConfig(
                    kubeconfig="/fake/kubeconfig",
                    registries=["registry.ci.openshift.org"],
                ) as auth_file:
                    auth_file_path = auth_file
                    raise RuntimeError("boom")
        except RuntimeError:
            pass

        self.assertIsNotNone(auth_file_path)
        self.assertFalse(os.path.exists(auth_file_path))

    def test_kubeconfig_validation_registries_without_sources_or_kubeconfig(self):
        """
        Registries without source_files or kubeconfig should still raise.
        """
        with self.assertRaises(ValueError) as cm:
            RegistryConfig(registries=["quay.io"])
        self.assertIn("source_files or kubeconfig", str(cm.exception))

    def test_kubeconfig_with_registries_allows_init(self):
        """
        kubeconfig + registries (no source_files) should pass init validation.
        """
        config = RegistryConfig(
            kubeconfig="/some/kubeconfig",
            registries=["registry.ci.openshift.org"],
        )
        self.assertIsNotNone(config)


if __name__ == "__main__":
    unittest.main()
