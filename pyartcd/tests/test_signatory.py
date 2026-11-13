import asyncio
import base64
import json
import os
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID
from pyartcd.exceptions import SignatoryServerError
from pyartcd.signatory import AsyncSignatory, DirectSignatory, SigstoreSignatory, create_signatory


class TestAsyncSignatory(IsolatedAsyncioTestCase):
    @patch("aiofiles.open", autospec=True)
    async def test_get_certificate_account_name(self, open: AsyncMock):
        # Well, this is the content of "Red Hat IT Root CA"
        expected = "Red Hat IT Root CA"
        one_day = timedelta(1, 0, 0)
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()

        builder = x509.CertificateBuilder()
        builder = builder.subject_name(
            x509.Name(
                [
                    x509.NameAttribute(NameOID.USER_ID, expected),
                ]
            )
        )
        builder = builder.issuer_name(
            x509.Name(
                [
                    x509.NameAttribute(NameOID.COMMON_NAME, 'cryptography.io'),
                ]
            )
        )
        builder = builder.not_valid_before(datetime.today() - one_day)
        builder = builder.not_valid_after(datetime.today() + (one_day * 30))
        builder = builder.serial_number(x509.random_serial_number())
        builder = builder.public_key(public_key)
        certificate = builder.sign(
            private_key=private_key,
            algorithm=hashes.SHA256(),
        )
        open.return_value.__aenter__.return_value.read.return_value = certificate.public_bytes(Encoding.PEM)
        actual = await AsyncSignatory._get_certificate_account_name("/path/to/client.crt")
        self.assertEqual(actual, expected)

    @patch("pyartcd.signatory.AsyncSignatory._get_certificate_account_name", autospec=True)
    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    async def test_start(self, AsyncUMBClient: AsyncMock, _get_certificate_account_name: AsyncMock):
        uri = "failover:(stomp+ssl://stomp1.example.com:12345,stomp://stomp2.example.com:23456)"
        cert_file = "/path/to/client.crt"
        key_file = "/path/to/client.key"
        _get_certificate_account_name.return_value = "fake-service-account"
        umb = AsyncUMBClient.return_value
        receiver = umb.subscribe.return_value
        receiver.iter_messages = MagicMock()
        fake_messages = [
            MagicMock(
                headers={
                    "message-id": "fake-message-id",
                    "timestamp": datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000,
                },
                body="",
            ),
        ]
        receiver.iter_messages.return_value.__aiter__.return_value = fake_messages
        signatory = AsyncSignatory(
            uri,
            cert_file,
            key_file,
            sig_keyname="test",
            requestor="fake-requestor",
            subscription_name="fake-subscription",
        )
        await signatory.start()
        umb.subscribe.assert_awaited_once_with(
            "/queue/Consumer.fake-service-account.fake-subscription.VirtualTopic.eng.robosignatory.art.sign",
            "fake-subscription",
        )

    @patch("pyartcd.signatory.datetime", wraps=datetime)
    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    async def test_handle_messages_with_stale_message(self, AsyncUMBClient: AsyncMock, datetime: MagicMock):
        uri = "failover:(stomp+ssl://stomp1.example.com:12345,stomp://stomp2.example.com:23456)"
        cert_file = "/path/to/client.crt"
        key_file = "/path/to/client.key"
        signatory = AsyncSignatory(
            uri,
            cert_file,
            key_file,
            sig_keyname="test",
            requestor="fake-requestor",
            subscription_name="fake-subscription",
        )
        receiver = signatory._receiver = MagicMock(id="fake-subscription")
        datetime.utcnow.return_value = datetime(2023, 1, 2, 0, 0, 0)
        receiver.iter_messages = MagicMock()
        fake_messages = [
            MagicMock(
                headers={
                    "message-id": "fake-message-id",
                    "timestamp": datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000,
                },
                body="",
            ),
        ]
        receiver.iter_messages.return_value.__aiter__.return_value = fake_messages
        umb = AsyncUMBClient.return_value

        await signatory._handle_messages()

        umb.ack.assert_awaited_once_with("fake-message-id", "fake-subscription")

    @patch("pyartcd.signatory.datetime", wraps=datetime)
    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    async def test_handle_messages_with_invalid_message(self, AsyncUMBClient: AsyncMock, datetime: MagicMock):
        uri = "failover:(stomp+ssl://stomp1.example.com:12345,stomp://stomp2.example.com:23456)"
        cert_file = "/path/to/client.crt"
        key_file = "/path/to/client.key"
        signatory = AsyncSignatory(
            uri,
            cert_file,
            key_file,
            sig_keyname="test",
            requestor="fake-requestor",
            subscription_name="fake-subscription",
        )
        receiver = signatory._receiver = MagicMock(id="fake-subscription")
        datetime.utcnow.return_value = datetime(2023, 1, 1, 0, 1, 0)
        receiver.iter_messages = MagicMock()
        fake_messages = [
            MagicMock(
                headers={
                    "message-id": "fake-message-id",
                    "timestamp": datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000,
                },
                body=json.dumps({"msg": {"request_id": "invalid-request-id"}}),
            ),
        ]
        receiver.iter_messages.return_value.__aiter__.return_value = fake_messages
        umb = AsyncUMBClient.return_value

        await signatory._handle_messages()

        umb.ack.assert_not_called()

    @patch("pyartcd.signatory.datetime", wraps=datetime)
    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    async def test_handle_messages_with_valid_message(self, AsyncUMBClient: AsyncMock, datetime: MagicMock):
        uri = "failover:(stomp+ssl://stomp1.example.com:12345,stomp://stomp2.example.com:23456)"
        cert_file = "/path/to/client.crt"
        key_file = "/path/to/client.key"
        signatory = AsyncSignatory(
            uri,
            cert_file,
            key_file,
            sig_keyname="test",
            requestor="fake-requestor",
            subscription_name="fake-subscription",
        )
        receiver = signatory._receiver = MagicMock(id="fake-subscription")
        datetime.utcnow.return_value = datetime(2023, 1, 1, 0, 1, 0)
        signatory._requests["fake-request-id"] = asyncio.get_event_loop().create_future()
        receiver.iter_messages = MagicMock()
        fake_messages = [
            MagicMock(
                headers={
                    "message-id": "fake-message-id",
                    "timestamp": datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000,
                },
                body=json.dumps({"msg": {"request_id": "fake-request-id"}}),
            ),
        ]
        receiver.iter_messages.return_value.__aiter__.return_value = fake_messages
        umb = AsyncUMBClient.return_value

        await signatory._handle_messages()

        umb.ack.assert_awaited_once_with("fake-message-id", "fake-subscription")
        message_headers, message_body = await signatory._requests["fake-request-id"]
        self.assertEqual(message_headers["message-id"], "fake-message-id")
        self.assertEqual(message_body["msg"]["request_id"], "fake-request-id")

    @patch("pyartcd.signatory.datetime", wraps=datetime)
    @patch("uuid.uuid4", autospec=True)
    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    async def test_sign_artifact(self, AsyncUMBClient: AsyncMock, uuid4: MagicMock, datetime: MagicMock):
        uri = "failover:(stomp+ssl://stomp1.example.com:12345,stomp://stomp2.example.com:23456)"
        cert_file = "/path/to/client.crt"
        key_file = "/path/to/client.key"
        signatory = AsyncSignatory(
            uri,
            cert_file,
            key_file,
            sig_keyname="test",
            requestor="fake-requestor",
            subscription_name="fake-subscription",
        )
        artifact = BytesIO(b"fake_artifact")
        sig_file = BytesIO()
        uuid4.return_value = "fake-uuid"
        datetime.utcnow.return_value = datetime(2023, 1, 2, 12, 30, 40)
        umb = AsyncUMBClient.return_value
        response_headers = {}
        response_body = {
            "msg": {
                "artifact_meta": {
                    "name": "sha256sum.txt.gpg",
                    "product": "openshift",
                    "release_name": "4.0.1",
                    "type": "message-digest",
                },
                "signing_status": "success",
                "errors": [],
                "signed_artifact": base64.b64encode(b'fake-signature').decode(),
            },
        }
        expected_requested_id = 'openshift-message-digest-20230102123040-fake-uuid'
        asyncio.get_event_loop().call_soon(
            lambda: signatory._requests[expected_requested_id].set_result((response_headers, response_body))
        )

        await signatory._sign_artifact("message-digest", "openshift", "4.0.1", "sha256sum.txt.gpg", artifact, sig_file)
        umb.send.assert_awaited_once_with(signatory.SEND_DESTINATION, ANY)
        self.assertEqual(sig_file.getvalue(), b'fake-signature')

    @patch("pyartcd.signatory.AsyncSignatory._sign_artifact")
    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    async def test_sign_message_digest(self, AsyncUMBClient: AsyncMock, _sign_artifact: AsyncMock):
        uri = "failover:(stomp+ssl://stomp1.example.com:12345,stomp://stomp2.example.com:23456)"
        cert_file = "/path/to/client.crt"
        key_file = "/path/to/client.key"
        signatory = AsyncSignatory(
            uri,
            cert_file,
            key_file,
            sig_keyname="test",
            requestor="fake-requestor",
            subscription_name="fake-subscription",
        )
        artifact = BytesIO(b"fake_artifact")
        sig_file = BytesIO()
        _sign_artifact.side_effect = lambda *args, **kwargs: sig_file.write(b"fake-signature")

        await signatory.sign_message_digest("openshift", "4.0.1", artifact, sig_file)
        _sign_artifact.assert_awaited_once_with(
            typ='message-digest',
            product='openshift',
            release_name='4.0.1',
            name='sha256sum.txt.gpg',
            artifact=artifact,
            sig_file=sig_file,
        )
        self.assertEqual(sig_file.getvalue(), b'fake-signature')

    @patch("pyartcd.signatory.AsyncSignatory._sign_artifact")
    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    async def test_sign_json_digest(self, AsyncUMBClient: AsyncMock, _sign_artifact: AsyncMock):
        uri = "failover:(stomp+ssl://stomp1.example.com:12345,stomp://stomp2.example.com:23456)"
        cert_file = "/path/to/client.crt"
        key_file = "/path/to/client.key"
        signatory = AsyncSignatory(
            uri,
            cert_file,
            key_file,
            sig_keyname="test",
            requestor="fake-requestor",
            subscription_name="fake-subscription",
        )
        sig_file = BytesIO()
        _sign_artifact.side_effect = lambda *args, **kwargs: sig_file.write(b"fake-signature")
        pullspec = "example.com/fake/repo@sha256:dead-beef"

        await signatory.sign_json_digest("openshift", "4.0.1", pullspec, "sha256:dead-beef", sig_file)
        _sign_artifact.assert_awaited_once_with(
            typ='json-digest',
            product='openshift',
            release_name='4.0.1',
            name='sha256=dead-beef',
            artifact=ANY,
            sig_file=sig_file,
        )
        self.assertEqual(sig_file.getvalue(), b'fake-signature')


class TestDirectSignatory(IsolatedAsyncioTestCase):
    async def test_sign_message_digest_writes_signature_from_client_output(self):
        command_runner = AsyncMock(return_value=(0, "", ""))

        async def write_signature(command, **kwargs):
            if command[0] == "gpg2":
                return 0, "", ""
            output_path = Path(command[command.index("--output") + 1])
            output_path.write_bytes(b"fake-direct-signature")
            return 0, "", ""

        command_runner.side_effect = write_signature
        signatory = DirectSignatory(
            client_command=("rh-signing-client",),
            sig_keyname="redhatrelease2",
            command_runner=command_runner,
        )
        sig_file = BytesIO()

        await signatory.sign_message_digest("openshift", "4.19.0", BytesIO(b"sha256 data"), sig_file)

        self.assertEqual(sig_file.getvalue(), b"fake-direct-signature")
        command = command_runner.await_args_list[0].args[0]
        self.assertEqual(command[:3], ["rh-signing-client", "--key", "redhatrelease2"])
        self.assertEqual(command[4], "--gpgsign")
        self.assertEqual(command[5], "--output")
        self.assertEqual(command[7:], ["--onbehalfof", "jupierce@redhat.com"])
        verification_command = command_runner.await_args_list[1].args[0]
        self.assertEqual(verification_command[:2], ["gpg2", "-d"])

    async def test_sign_json_digest_writes_signature_from_client_output(self):
        command_runner = AsyncMock()

        async def write_signature(command, **kwargs):
            if command[0] == "gpg2":
                return 0, "", ""
            input_path = Path(command[3])
            output_path = Path(command[command.index("--output") + 1])
            claim = json.loads(input_path.read_text())
            self.assertEqual(claim["critical"]["image"]["docker-manifest-digest"], "sha256:dead-beef")
            output_path.write_bytes(b"fake-json-signature")
            return 0, "", ""

        command_runner.side_effect = write_signature
        signatory = DirectSignatory(
            client_command=("rh-signing-client",),
            sig_keyname="redhatrelease2",
            command_runner=command_runner,
        )
        sig_file = BytesIO()

        await signatory.sign_json_digest(
            "openshift", "4.19.0", "quay.io/example/release:4.19.0", "sha256:dead-beef", sig_file
        )

        self.assertEqual(sig_file.getvalue(), b"fake-json-signature")

    async def test_context_manager_uses_isolated_kerberos_cache_and_leaves_keytab_untouched(self):
        command_runner = AsyncMock(return_value=(0, "", ""))
        with tempfile.TemporaryDirectory() as temp_dir:
            keytab = Path(temp_dir) / "signing.keytab"
            ccache = Path(temp_dir) / "krb5cc"
            keytab.write_bytes(b"temporary-keytab")
            signatory = DirectSignatory(
                client_command=("rh-signing-client",),
                sig_keyname="redhatrelease2",
                keytab_file=str(keytab),
                principal="ocp-art-signing-prod@IPA.REDHAT.COM",
                ccache_path=str(ccache),
                command_runner=command_runner,
            )

            async with signatory:
                pass
            self.assertTrue(keytab.exists())

        kinit_command = command_runner.await_args_list[0].args[0]
        kdestroy_command = command_runner.await_args_list[1].args[0]
        self.assertEqual(kinit_command[:2], ["kinit", "-k"])
        self.assertIn(f"FILE:{ccache}", kinit_command)
        self.assertEqual(kdestroy_command, ["kdestroy", "-c", f"FILE:{ccache}"])
        self.assertEqual(command_runner.await_args_list[0].kwargs["env"]["KRB5CCNAME"], f"FILE:{ccache}")

    async def test_sign_artifact_raises_when_client_fails(self):
        command_runner = AsyncMock(return_value=(1, "", "signing server rejected request"))
        signatory = DirectSignatory(client_command=("rh-signing-client",), command_runner=command_runner)

        with self.assertRaisesRegex(SignatoryServerError, "signing server rejected request"):
            await signatory.sign_message_digest("openshift", "4.19.0", BytesIO(b"sha256 data"), BytesIO())

    async def test_sign_artifact_raises_when_client_does_not_write_signature(self):
        command_runner = AsyncMock(return_value=(0, "", ""))
        signatory = DirectSignatory(client_command=("rh-signing-client",), command_runner=command_runner)

        with self.assertRaisesRegex(SignatoryServerError, "did not produce a signature"):
            await signatory.sign_message_digest("openshift", "4.19.0", BytesIO(b"sha256 data"), BytesIO())

    async def test_from_environment_requires_direct_signing_credentials(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "DIRECT_SIGNING_KEYTAB"):
                DirectSignatory.from_environment("redhatrelease2")

    async def test_from_environment_ignores_environment_specific_credentials(self):
        with patch.dict(
            os.environ,
            {
                "DIRECT_SIGNING_PROD_KEYTAB": "/path/to/prod-keytab",
                "DIRECT_SIGNING_PROD_PRINCIPAL": "ocp-art-signing-prod@IPA.REDHAT.COM",
                "DIRECT_SIGNING_STAGE_KEYTAB": "/path/to/stage-keytab",
                "DIRECT_SIGNING_STAGE_PRINCIPAL": "ocp-art-signing-prod@IPA.REDHAT.COM",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "DIRECT_SIGNING_KEYTAB"):
                DirectSignatory.from_environment("redhatrelease2")

    async def test_from_environment_uses_default_client_configuration(self):
        with patch.dict(
            os.environ,
            {
                "DIRECT_SIGNING_KEYTAB": "/path/to/signing-keytab",
                "DIRECT_SIGNING_PRINCIPAL": "ocp-art-signing-prod@IPA.REDHAT.COM",
                "DIRECT_SIGNING_CLIENT_COMMAND": "rh-signing-client --config /path/to/config",
                "DIRECT_SIGNING_TIMEOUT": "42",
            },
            clear=True,
        ):
            signatory = DirectSignatory.from_environment("beta2")

        self.assertEqual(signatory.client_command, DirectSignatory.DEFAULT_CLIENT_COMMAND)
        self.assertEqual(signatory.keytab_file, "/path/to/signing-keytab")
        self.assertEqual(signatory.principal, "ocp-art-signing-prod@IPA.REDHAT.COM")
        self.assertEqual(signatory.sig_keyname, "beta2")
        self.assertEqual(signatory.ON_BEHALF_OF, "jupierce@redhat.com")
        self.assertEqual(signatory.timeout, DirectSignatory.DEFAULT_TIMEOUT)


class TestCreateSignatory(IsolatedAsyncioTestCase):
    @patch("pyartcd.signatory.AsyncSignatory")
    async def test_create_signatory_uses_umb_transport(self, async_signatory):
        create_signatory(
            "umb",
            signing_env="prod",
            sig_keyname="redhatrelease2",
            cert_file="/path/cert",
            key_file="/path/key",
        )

        async_signatory.assert_called_once()

    @patch("pyartcd.signatory.DirectSignatory.from_environment")
    async def test_create_signatory_uses_direct_transport(self, direct_signatory):
        create_signatory("direct", signing_env="prod", sig_keyname="redhatrelease2")

        direct_signatory.assert_called_once_with(sig_keyname="redhatrelease2")


class TestSigstoreSignatory(IsolatedAsyncioTestCase):
    def _create_signatory(self, dry_run=True, **kwargs) -> SigstoreSignatory:
        """Create a SigstoreSignatory for testing"""
        logger = MagicMock()
        defaults = dict(
            logger=logger,
            dry_run=dry_run,
            signing_creds="/path/to/creds",
            signing_key_ids=["test-key-id"],
            rekor_url="",
            concurrency_limit=10,
        )
        defaults.update(kwargs)
        return SigstoreSignatory(**defaults)

    def test_redigest_pullspec_with_digest(self):
        """Test redigest_pullspec with an existing digest reference"""
        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:olddigest"
        new_digest = "sha256:newdigest"
        result = SigstoreSignatory.redigest_pullspec(pullspec, new_digest)
        self.assertEqual(result, "quay.io/openshift-release-dev/ocp-release@sha256:newdigest")

    def test_redigest_pullspec_with_tag(self):
        """Test redigest_pullspec with a tag reference"""
        pullspec = "quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64"
        new_digest = "sha256:abc123"
        result = SigstoreSignatory.redigest_pullspec(pullspec, new_digest)
        self.assertEqual(result, "quay.io/openshift-release-dev/ocp-release@sha256:abc123")

    def test_redigest_pullspec_bare_repo(self):
        """Test redigest_pullspec with a bare repo (no tag or digest)"""
        pullspec = "quay.io/openshift-release-dev/ocp-release"
        new_digest = "sha256:abc123"
        result = SigstoreSignatory.redigest_pullspec(pullspec, new_digest)
        self.assertEqual(result, "quay.io/openshift-release-dev/ocp-release@sha256:abc123")

    async def test_sign_manifest_with_canonical_tag(self):
        """Test signing a manifest with canonical tag signs with both digest and tag identities"""
        signatory = self._create_signatory()
        # A signature object exists, so the precise per-identity verify path is exercised.
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=False)

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        canonical_tag = "4.16.1-x86_64"
        result = await signatory._sign_manifest(pullspec, canonical_tag)

        self.assertEqual(result, {})
        # Should have logged dry run messages twice (digest + tag identities)
        dry_run_calls = [call for call in signatory._logger.info.call_args_list if "[DRY RUN]" in str(call)]
        self.assertEqual(len(dry_run_calls), 2)

    async def test_sign_manifest_without_canonical_tag(self):
        """Test signing a manifest without canonical tag signs with digest identity only"""
        signatory = self._create_signatory()
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=False)

        pullspec = "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123"
        result = await signatory._sign_manifest(pullspec)

        self.assertEqual(result, {})
        # Should have logged dry run message once (digest identity only)
        dry_run_calls = [call for call in signatory._logger.info.call_args_list if "[DRY RUN]" in str(call)]
        self.assertEqual(len(dry_run_calls), 1)

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_uses_legacy_cosign_signing(self, mock_cmd, mock_sleep):
        """Keep custom Rekor services and legacy image signatures with cosign 3."""
        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        canonical_tag = "4.16.1-x86_64"
        identities = [pullspec, f"quay.io/openshift-release-dev/ocp-release:{canonical_tag}"]
        mock_cmd.return_value = (0, "ok", "")

        for rekor_url in ("https://rekor.example.com", ""):
            with self.subTest(rekor_url=rekor_url):
                mock_cmd.reset_mock()
                signatory = self._create_signatory(dry_run=False, rekor_url=rekor_url)
                signatory._any_signature_exists = AsyncMock(return_value=True)
                signatory._signature_exists = AsyncMock(return_value=False)

                result = await signatory._sign_manifest(pullspec, canonical_tag)

                self.assertEqual(result, {})
                self.assertEqual(mock_cmd.await_count, len(identities))
                for call, identity in zip(mock_cmd.await_args_list, identities):
                    cmd = call.args[0]
                    env = call.kwargs["env"]
                    self.assertEqual(env["COSIGN_USE_SIGNING_CONFIG"], "false")
                    self.assertEqual(env["COSIGN_NEW_BUNDLE_FORMAT"], "false")
                    self.assertNotIn("--use-signing-config=false", cmd)
                    self.assertNotIn("--new-bundle-format=false", cmd)
                    self.assertIn(f"--sign-container-identity={identity}", cmd)
                    self.assertEqual(cmd[cmd.index("--key") + 1], "awskms:///test-key-id")
                    self.assertIn(f"--rekor-url={rekor_url}" if rekor_url else "--tlog-upload=false", cmd)
                    self.assertEqual(cmd[-1], pullspec)

    async def test_sign_release_images_with_multiple_manifests(self):
        """Test signing release images where a manifest list has multiple arch manifests.

        When a user pulls "ocp-release:4.16.1-multi", the registry returns an arch-specific
        manifest. The signature for that manifest must have identity "ocp-release:4.16.1-multi"
        (the tag the user requested), not an arch-specific tag.
        """
        from pyartcd.signatory import ReleaseImageInfo

        signatory = self._create_signatory()
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=False)

        # Simulate a manifest list where multiple arch manifests are discovered
        release_info = ReleaseImageInfo(
            original_pullspec="quay.io/openshift-release-dev/ocp-release@sha256:manifest-list",
            canonical_tag="4.16.1-multi",
            manifests_to_sign=[
                "quay.io/openshift-release-dev/ocp-release@sha256:x86-manifest",
                "quay.io/openshift-release-dev/ocp-release@sha256:arm-manifest",
            ],
        )

        result = await signatory.sign_release_images([release_info])

        self.assertEqual(result, {})
        # Should have logged for 4 signings: 2 manifests x 2 identities (digest + tag) each
        dry_run_calls = [call for call in signatory._logger.info.call_args_list if "[DRY RUN]" in str(call)]
        self.assertEqual(len(dry_run_calls), 4)

    async def test_sign_component_images(self):
        """Test signing component images signs with digest identity only"""
        signatory = self._create_signatory()
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=False)

        component_images = [
            "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:comp1",
            "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:comp2",
        ]

        result = await signatory.sign_component_images(component_images)

        self.assertEqual(result, {})
        # Should have logged 2 dry run messages (one per component, digest only)
        dry_run_calls = [call for call in signatory._logger.info.call_args_list if "[DRY RUN]" in str(call)]
        self.assertEqual(len(dry_run_calls), 2)

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_batch_retry_retries_only_failed_release_images(self, mock_cmd, mock_sleep):
        """After a batch failure, only the failed pullspecs are retried"""
        from pyartcd.signatory import ReleaseImageInfo

        signatory = self._create_signatory(dry_run=False, batch_retries=2, batch_retry_delay=0)
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=False)

        ps_ok = "quay.io/ocp-release@sha256:aaa"
        ps_fail = "quay.io/ocp-release@sha256:bbb"

        call_count = {}

        async def fake_cmd(cmd, check=False, env=None):
            pullspec = cmd[-1]
            call_count.setdefault(pullspec, 0)
            call_count[pullspec] += 1
            if pullspec == ps_fail and call_count[pullspec] <= 5:
                return (1, "", "cosign error")
            return (0, "ok", "")

        mock_cmd.side_effect = fake_cmd

        release_info = ReleaseImageInfo(
            original_pullspec="quay.io/ocp-release:4.16.1-x86_64",
            canonical_tag="4.16.1-x86_64",
            manifests_to_sign=[ps_ok, ps_fail],
        )

        errors = await signatory.sign_release_images([release_info])

        # ps_ok should have been called only in the first batch (2 identities x 1 key = 2 calls)
        self.assertNotIn(ps_ok, errors)
        self.assertEqual(call_count[ps_ok], 2)
        # ps_fail should have been retried in batch 2 (5 per-invocation retries * 2 batches * 2 identities)
        self.assertGreater(call_count[ps_fail], 5)

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_batch_retry_succeeds_on_second_attempt(self, mock_cmd, mock_sleep):
        """A transient failure that succeeds on the second batch attempt results in overall success"""
        signatory = self._create_signatory(dry_run=False, batch_retries=3, batch_retry_delay=0)
        signatory._any_signature_exists = AsyncMock(return_value=True)

        batch_attempt = {"count": 0}
        ps = "quay.io/ocp-release@sha256:abc"

        async def fake_cmd(cmd, check=False, env=None):
            if batch_attempt["count"] == 0:
                return (1, "", "transient error")
            return (0, "ok", "")

        mock_cmd.side_effect = fake_cmd

        original_sign = signatory._sign_manifest
        call_number = {"n": 0}

        async def counting_sign(pullspec, canonical_tag=None, tag_only=False):
            call_number["n"] += 1
            if call_number["n"] > 1:
                batch_attempt["count"] = 1
            return await original_sign(pullspec, canonical_tag, tag_only)

        signatory._sign_manifest = counting_sign

        result = await signatory.sign_component_images([ps])

        self.assertEqual(result, {})

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_batch_retry_returns_errors_after_exhaustion(self, mock_cmd, mock_sleep):
        """After all batch retries exhausted, remaining errors are returned"""
        signatory = self._create_signatory(dry_run=False, batch_retries=2, batch_retry_delay=0)
        signatory._any_signature_exists = AsyncMock(return_value=True)

        mock_cmd.return_value = (1, "", "permanent error")

        ps = "quay.io/ocp-v4.0-art-dev@sha256:fail"
        result = await signatory.sign_component_images([ps])

        self.assertIn(ps, result)
        self.assertIsInstance(result[ps], RuntimeError)

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_error_detail_includes_identity_and_key(self, mock_cmd, mock_sleep):
        """Error dict from _sign_manifest includes identity and key_id in the error message"""
        signatory = self._create_signatory(dry_run=False, signing_key_ids=["my-key-123"])
        signatory._any_signature_exists = AsyncMock(return_value=True)

        mock_cmd.return_value = (1, "", "auth failure")

        ps = "quay.io/ocp-release@sha256:xyz"
        result = await signatory._sign_manifest(ps, canonical_tag="4.16.1-x86_64")

        self.assertIn(ps, result)
        err_msg = str(result[ps])
        self.assertIn("identity=", err_msg)
        self.assertIn("key=my-key-123", err_msg)

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_signature_exists_true_when_identity_matches(self, mock_cmd):
        """_signature_exists returns True when verify succeeds and a payload matches the identity"""
        signatory = self._create_signatory()

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        identity = "quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64"
        payloads = [
            {"critical": {"identity": {"docker-reference": "quay.io/other:tag"}}},
            {"critical": {"identity": {"docker-reference": identity}}},
        ]
        mock_cmd.return_value = (0, json.dumps(payloads), "")

        result = await signatory._signature_exists(pullspec, identity, "test-key-id")

        self.assertTrue(result)
        cmd = mock_cmd.await_args.args[0]
        self.assertEqual(cmd[:2], ["cosign", "verify"])
        self.assertEqual(cmd[cmd.index("--key") + 1], "awskms:///test-key-id")
        self.assertIn("--output", cmd)
        self.assertEqual(cmd[-1], pullspec)
        # The existence check always ignores the tlog and never uses --rekor-url.
        self.assertIn("--insecure-ignore-tlog=true", cmd)
        self.assertFalse(any(arg.startswith("--rekor-url") for arg in cmd))

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_signature_exists_always_ignores_tlog(self, mock_cmd):
        """_signature_exists ignores the tlog and never passes --rekor-url, even when rekor_url is set."""
        signatory = self._create_signatory(rekor_url="https://rekor.example.com")
        mock_cmd.return_value = (0, json.dumps([]), "")

        await signatory._signature_exists("quay.io/x@sha256:a", "quay.io/x:tag", "test-key-id")

        cmd = mock_cmd.await_args.args[0]
        self.assertIn("--insecure-ignore-tlog=true", cmd)
        self.assertFalse(any(arg.startswith("--rekor-url") for arg in cmd))

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_signature_exists_false_when_identity_does_not_match(self, mock_cmd):
        """_signature_exists returns False when no payload matches the requested identity"""
        signatory = self._create_signatory()

        payloads = [{"critical": {"identity": {"docker-reference": "quay.io/other:tag"}}}]
        mock_cmd.return_value = (0, json.dumps(payloads), "")

        result = await signatory._signature_exists(
            "quay.io/openshift-release-dev/ocp-release@sha256:abc123",
            "quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64",
            "test-key-id",
        )

        self.assertFalse(result)

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_signature_exists_false_when_verify_fails(self, mock_cmd):
        """_signature_exists returns False (fail open) when cosign verify exits non-zero"""
        signatory = self._create_signatory()

        mock_cmd.return_value = (1, "", "no matching signatures")

        result = await signatory._signature_exists("quay.io/x@sha256:a", "quay.io/x:tag", "test-key-id")

        self.assertFalse(result)

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_signature_exists_false_on_unparseable_json(self, mock_cmd):
        """_signature_exists returns False (does not raise) when verify output is not valid JSON"""
        signatory = self._create_signatory()

        mock_cmd.return_value = (0, "not json at all", "")

        result = await signatory._signature_exists("quay.io/x@sha256:a", "quay.io/x:tag", "test-key-id")

        self.assertFalse(result)

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_skips_when_signature_exists(self, mock_cmd, mock_sleep):
        """_sign_manifest skips the cosign sign call when our signature already exists"""
        signatory = self._create_signatory(dry_run=False)
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=True)
        mock_cmd.return_value = (0, "ok", "")

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        self.assertEqual(mock_cmd.await_count, 0)
        self.assertEqual(signatory._signature_exists.await_count, 2)

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_signs_when_signature_absent(self, mock_cmd, mock_sleep):
        """_sign_manifest signs when our signature does not yet exist"""
        signatory = self._create_signatory(dry_run=False)
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=False)
        mock_cmd.return_value = (0, "ok", "")

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        self.assertEqual(mock_cmd.await_count, 2)
        for call in mock_cmd.await_args_list:
            self.assertEqual(call.args[0][:2], ["cosign", "sign"])

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_signs_only_missing_identity(self, mock_cmd, mock_sleep):
        """Only the unsigned identity is signed when its sibling identity is already signed (ART-23308)."""
        signatory = self._create_signatory(dry_run=False)
        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        digest_identity = pullspec
        tag_identity = "quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64"

        # digest is signed, tag is not.
        signed_by_identity = {digest_identity: True, tag_identity: False}

        async def fake_exists(ps, identity, key_id):
            return signed_by_identity[identity]

        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(side_effect=fake_exists)
        mock_cmd.return_value = (0, "ok", "")

        result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        self.assertEqual(signatory._signature_exists.await_count, 2)
        checked_identities = {call.args[1] for call in signatory._signature_exists.await_args_list}
        self.assertEqual(checked_identities, {digest_identity, tag_identity})

        self.assertEqual(mock_cmd.await_count, 1)
        sign_cmd = mock_cmd.await_args_list[0].args[0]
        self.assertEqual(sign_cmd[:2], ["cosign", "sign"])
        self.assertIn(f"--sign-container-identity={tag_identity}", sign_cmd)
        self.assertNotIn(f"--sign-container-identity={digest_identity}", sign_cmd)

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_force_resign_bypasses_existence_check(self, mock_cmd, mock_sleep):
        """force_resign=True signs unconditionally and never consults _signature_exists"""
        signatory = self._create_signatory(dry_run=False, force_resign=True)
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=True)
        mock_cmd.return_value = (0, "ok", "")

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        signatory._any_signature_exists.assert_not_awaited()
        signatory._signature_exists.assert_not_awaited()
        self.assertEqual(mock_cmd.await_count, 2)

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_cosign_repository_bypasses_skip_checks(self, mock_cmd, mock_sleep):
        """COSIGN_REPOSITORY redirects signature storage, so the skip checks are bypassed and we sign."""
        signatory = self._create_signatory(dry_run=False)
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=True)
        mock_cmd.return_value = (0, "ok", "")

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        with patch.dict(os.environ, {"COSIGN_REPOSITORY": "quay.io/other/signature-mirror"}):
            result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        signatory._any_signature_exists.assert_not_awaited()
        signatory._signature_exists.assert_not_awaited()
        self.assertEqual(mock_cmd.await_count, 2)

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_any_signature_exists_true_on_success(self, mock_cmd):
        """_any_signature_exists returns True when skopeo inspect finds the .sig object (rc 0)."""
        signatory = self._create_signatory()
        mock_cmd.return_value = (0, "{}", "")

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        with patch.dict(os.environ, {"QUAY_AUTH_FILE": "/path/to/auth.json"}, clear=False):
            result = await signatory._any_signature_exists(pullspec)

        self.assertTrue(result)
        cmd = mock_cmd.await_args.args[0]
        self.assertEqual(cmd[:3], ["skopeo", "inspect", "--raw"])
        self.assertIn("--authfile", cmd)
        self.assertEqual(cmd[cmd.index("--authfile") + 1], "/path/to/auth.json")
        self.assertEqual(cmd[-1], "docker://quay.io/openshift-release-dev/ocp-release:sha256-abc123.sig")

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_any_signature_exists_false_on_manifest_unknown(self, mock_cmd):
        """_any_signature_exists returns False on a tight registry not-found signal."""
        signatory = self._create_signatory()
        mock_cmd.return_value = (1, "", "Error: reading manifest sha256-abc123.sig: manifest unknown")

        result = await signatory._any_signature_exists("quay.io/x/y@sha256:abc123")

        self.assertFalse(result)

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_any_signature_exists_false_on_404(self, mock_cmd):
        """_any_signature_exists returns False when the registry responds HTTP 404."""
        signatory = self._create_signatory()
        mock_cmd.return_value = (1, "", "unexpected HTTP 404 (Not Found) while fetching manifest")

        result = await signatory._any_signature_exists("quay.io/x/y@sha256:abc123")

        self.assertFalse(result)

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_any_signature_exists_true_on_ambiguous_error(self, mock_cmd):
        """_any_signature_exists fails OPEN (returns True) on an auth/other error or a bare 'not found'."""
        signatory = self._create_signatory()
        mock_cmd.return_value = (1, "", "unauthorized: authentication required")

        result = await signatory._any_signature_exists("quay.io/x/y@sha256:abc123")
        self.assertTrue(result)

        mock_cmd.return_value = (127, "", "bash: skopeo: command not found")
        result = await signatory._any_signature_exists("quay.io/x/y@sha256:abc123")
        self.assertTrue(result)

    async def test_any_signature_exists_true_when_no_digest(self):
        """_any_signature_exists returns True (can't derive .sig tag) for a non-digest pullspec."""
        signatory = self._create_signatory()

        with patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock) as mock_cmd:
            result = await signatory._any_signature_exists("quay.io/x/y:4.16.1-x86_64")

        self.assertTrue(result)
        mock_cmd.assert_not_awaited()

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_skips_verify_when_no_sig_object(self, mock_cmd, mock_sleep):
        """When no signature object exists, _signature_exists is never called and all identities sign."""
        signatory = self._create_signatory(dry_run=False)
        signatory._any_signature_exists = AsyncMock(return_value=False)
        signatory._signature_exists = AsyncMock(return_value=True)
        mock_cmd.return_value = (0, "ok", "")

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        self.assertEqual(signatory._any_signature_exists.await_count, 1)
        self.assertEqual(signatory._signature_exists.await_count, 0)
        self.assertEqual(mock_cmd.await_count, 2)
        for call in mock_cmd.await_args_list:
            self.assertEqual(call.args[0][:2], ["cosign", "sign"])

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_signature_exists_false_on_unexpected_json_shapes(self, mock_cmd):
        """_signature_exists fails OPEN (returns False, no raise) on valid JSON with unexpected shapes."""
        signatory = self._create_signatory()

        for stdout in ("null", "{}", "[null]", '["x"]', '[{"critical": null}]', '[{"critical": {"identity": null}}]'):
            with self.subTest(stdout=stdout):
                mock_cmd.return_value = (0, stdout, "")
                result = await signatory._signature_exists("quay.io/x@sha256:a", "quay.io/x:tag", "test-key-id")
                self.assertFalse(result)

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_signature_exists_false_when_cosign_cannot_launch(self, mock_cmd):
        """_signature_exists fails OPEN (returns False, no raise) when cosign cannot be launched."""
        signatory = self._create_signatory()
        mock_cmd.side_effect = FileNotFoundError("cosign")

        result = await signatory._signature_exists("quay.io/x@sha256:a", "quay.io/x:tag", "test-key-id")

        self.assertFalse(result)

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_any_signature_exists_true_when_skopeo_cannot_launch(self, mock_cmd):
        """_any_signature_exists fails OPEN (returns True, no raise) when skopeo cannot be launched."""
        signatory = self._create_signatory()
        mock_cmd.side_effect = FileNotFoundError("skopeo")

        result = await signatory._any_signature_exists("quay.io/x/y@sha256:abc123")

        self.assertTrue(result)

    # --- repo-aware (longest-prefix) credential selection ----------------------------------------

    # Computed at runtime so no credential literal lives in source (throwaway fixtures only).
    RELEASE_CRED = {"auth": base64.b64encode(b"release-user:release-pass").decode()}
    ART_CRED = {"auth": base64.b64encode(b"art-user:art-pass").decode()}

    def _make_auth_file(self, auths: dict) -> str:
        """Write a QUAY_AUTH_FILE-style config.json with the given auths and return its path."""
        tmpdir = tempfile.mkdtemp(prefix="test-quay-auth-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        path = os.path.join(tmpdir, "auth.json")
        with open(path, "w") as f:
            json.dump({"auths": auths}, f)
        return path

    def _read_config_auths(self, config_dir: str) -> dict:
        with open(os.path.join(config_dir, "config.json")) as f:
            return json.load(f)["auths"]

    def test_repo_aware_selects_longest_prefix_credential(self):
        """With multiple quay.io path-scoped robots, the hostname-only entry handed to cosign is the
        longest-prefix match for each image's repository."""
        auths = {
            "quay.io/openshift-release-dev": self.RELEASE_CRED,
            "quay.io/redhat-user-workloads/ocp-art-tenant/art-images": self.ART_CRED,
        }
        path = self._make_auth_file(auths)
        with patch.dict(os.environ, {"QUAY_AUTH_FILE": path}):
            signatory = self._create_signatory()
            self.addCleanup(signatory.close)

            # An ocp-v4.0-art-dev image under openshift-release-dev must resolve to the release cred.
            release_ps = "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:" + "a" * 64
            release_dir = signatory._docker_config_dir_for_pullspec(release_ps)
            release_auths = self._read_config_auths(release_dir)
            self.assertEqual(release_auths["quay.io"], self.RELEASE_CRED)
            # The chosen key is preserved; the unrelated robot's credential is absent from this config.
            self.assertEqual(release_auths["quay.io/openshift-release-dev"], self.RELEASE_CRED)
            self.assertNotIn("quay.io/redhat-user-workloads/ocp-art-tenant/art-images", release_auths)

            # An art-images image must resolve to the art-images cred instead.
            art_ps = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:" + "b" * 64
            art_dir = signatory._docker_config_dir_for_pullspec(art_ps)
            art_auths = self._read_config_auths(art_dir)
            self.assertEqual(art_auths["quay.io"], self.ART_CRED)
            self.assertEqual(art_auths["quay.io/redhat-user-workloads/ocp-art-tenant/art-images"], self.ART_CRED)
            self.assertNotIn("quay.io/openshift-release-dev", art_auths)

            # Different repos resolve to different cached config dirs.
            self.assertNotEqual(release_dir, art_dir)
            # Same repo reuses the cached config dir.
            self.assertEqual(release_dir, signatory._docker_config_dir_for_pullspec(release_ps))

    def test_repo_aware_resolves_tagged_pullspec(self):
        """Repo derivation strips a trailing tag as well as a digest."""
        auths = {
            "quay.io/openshift-release-dev": self.RELEASE_CRED,
            "quay.io/redhat-user-workloads/ocp-art-tenant/art-images": self.ART_CRED,
        }
        path = self._make_auth_file(auths)
        with patch.dict(os.environ, {"QUAY_AUTH_FILE": path}):
            signatory = self._create_signatory()
            self.addCleanup(signatory.close)
            tagged_ps = "quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64"
            auths_out = self._read_config_auths(signatory._docker_config_dir_for_pullspec(tagged_ps))
            self.assertEqual(auths_out["quay.io"], self.RELEASE_CRED)

    def test_single_entry_per_host_unchanged(self):
        """With a single path-scoped credential per host, behavior is unchanged: the global fallback
        config aliases the hostname to that single credential."""
        auths = {"quay.io/openshift-release-dev": self.RELEASE_CRED}
        path = self._make_auth_file(auths)
        with patch.dict(os.environ, {"QUAY_AUTH_FILE": path}):
            signatory = self._create_signatory()
            self.addCleanup(signatory.close)

            # The global/fallback config collapses the lone path-scoped key to a hostname alias.
            global_auths = self._read_config_auths(signatory._global_docker_config_dir)
            self.assertEqual(global_auths["quay.io"], self.RELEASE_CRED)

            # Per-pullspec resolution also yields that single credential.
            ps = "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:" + "c" * 64
            resolved = self._read_config_auths(signatory._docker_config_dir_for_pullspec(ps))
            self.assertEqual(resolved["quay.io"], self.RELEASE_CRED)

    def test_no_matching_entry_falls_back_without_crash(self):
        """A pullspec whose repo matches no stored path-scoped entry falls back to the global config
        dir rather than crashing."""
        auths = {"quay.io/openshift-release-dev": self.RELEASE_CRED}
        path = self._make_auth_file(auths)
        with patch.dict(os.environ, {"QUAY_AUTH_FILE": path}):
            signatory = self._create_signatory()
            self.addCleanup(signatory.close)
            other_host_ps = "registry.redhat.io/some/other-image@sha256:" + "d" * 64
            self.assertEqual(
                signatory._docker_config_dir_for_pullspec(other_host_ps),
                signatory._global_docker_config_dir,
            )

    def test_no_auth_file_returns_none(self):
        """Without QUAY_AUTH_FILE there is no config dir to hand cosign (env stays unchanged)."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("QUAY_AUTH_FILE", None)
            signatory = self._create_signatory()
            self.addCleanup(signatory.close)
            self.assertIsNone(signatory._global_docker_config_dir)
            self.assertIsNone(
                signatory._docker_config_dir_for_pullspec("quay.io/openshift-release-dev/x@sha256:" + "e" * 64)
            )

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_passes_repo_aware_docker_config(self, mock_cmd, mock_sleep):
        """_sign_manifest hands cosign the repo-aware DOCKER_CONFIG for the signed pullspec."""
        auths = {
            "quay.io/openshift-release-dev": self.RELEASE_CRED,
            "quay.io/redhat-user-workloads/ocp-art-tenant/art-images": self.ART_CRED,
        }
        path = self._make_auth_file(auths)
        mock_cmd.return_value = (0, "ok", "")
        with patch.dict(os.environ, {"QUAY_AUTH_FILE": path}):
            signatory = self._create_signatory(dry_run=False)
            self.addCleanup(signatory.close)
            art_ps = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:" + "f" * 64
            expected_dir = signatory._docker_config_dir_for_pullspec(art_ps)

            result = await signatory._sign_manifest(art_ps)

            self.assertEqual(result, {})
            self.assertTrue(mock_cmd.await_count >= 1)
            # Every cosign/skopeo invocation for this pullspec uses the repo-aware DOCKER_CONFIG.
            for call in mock_cmd.await_args_list:
                env = call.kwargs["env"]
                self.assertEqual(env["DOCKER_CONFIG"], expected_dir)
                self.assertEqual(self._read_config_auths(env["DOCKER_CONFIG"])["quay.io"], self.ART_CRED)

    def test_env_is_instance_local_and_does_not_leak_docker_config(self):
        """Each SigstoreSignatory uses its own ENV copy, so DOCKER_CONFIG and its temp dir cannot leak
        onto another instance or onto the class-level ENV default."""
        class_env_before = dict(SigstoreSignatory.ENV)
        auths = {"quay.io/openshift-release-dev": self.RELEASE_CRED}
        path = self._make_auth_file(auths)
        with patch.dict(os.environ, {"QUAY_AUTH_FILE": path}):
            sig1 = self._create_signatory()
            sig2 = self._create_signatory()

        # Each instance has its own ENV dict (not the class dict, and not each other's).
        self.assertIsNot(sig1.ENV, SigstoreSignatory.ENV)
        self.assertIsNot(sig2.ENV, SigstoreSignatory.ENV)
        self.assertIsNot(sig1.ENV, sig2.ENV)

        # Each instance points DOCKER_CONFIG at its OWN temp dir.
        self.assertIn("DOCKER_CONFIG", sig1.ENV)
        self.assertIn("DOCKER_CONFIG", sig2.ENV)
        self.assertNotEqual(sig1.ENV["DOCKER_CONFIG"], sig2.ENV["DOCKER_CONFIG"])

        # The class-level default was NOT mutated: no DOCKER_CONFIG or AWS creds leaked onto it.
        self.assertEqual(SigstoreSignatory.ENV, class_env_before)

        # Closing sig1 removes ITS docker config dir but must not disturb sig2's env or temp dir.
        sig1_dir = sig1.ENV["DOCKER_CONFIG"]
        sig2_dir = sig2.ENV["DOCKER_CONFIG"]
        sig1.close()
        self.assertFalse(os.path.exists(sig1_dir))
        self.assertEqual(sig2.ENV["DOCKER_CONFIG"], sig2_dir)
        self.assertTrue(os.path.exists(sig2_dir))
        sig2.close()
