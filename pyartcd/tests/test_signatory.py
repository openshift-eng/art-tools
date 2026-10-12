import asyncio
import base64
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID
from pyartcd.exceptions import SignatoryServerError
from pyartcd.signatory import (
    PROD_SIGNING_KEY_NAME,
    STAGE_SIGNING_KEY_NAME,
    AsyncSignatory,
    KonfluxSignatory,
    SigstoreSignatory,
    create_signatory,
)


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

        mock_cmd.return_value = (1, "", "auth failure")

        ps = "quay.io/ocp-release@sha256:xyz"
        result = await signatory._sign_manifest(ps, canonical_tag="4.16.1-x86_64")

        self.assertIn(ps, result)
        err_msg = str(result[ps])
        self.assertIn("identity=", err_msg)
        self.assertIn("key=my-key-123", err_msg)


class TestKonfluxSignatory(IsolatedAsyncioTestCase):
    """Tests for the KonfluxSignatory class."""

    def _create_signatory(self, **kwargs) -> KonfluxSignatory:
        defaults = dict(
            sig_keyname="redhatrelease2",
            namespace="test-ns",
            ta_registry="quay.io/test/ta",
            on_behalf_of="test-user",
            poll_interval=0.01,
            timeout=5.0,
        )
        defaults.update(kwargs)
        return KonfluxSignatory(**defaults)

    def test_constructor_defaults(self):
        """Constructor sets defaults from constants or hardcoded values."""
        sig = KonfluxSignatory(sig_keyname="beta2")
        self.assertEqual(sig.sig_keyname, "beta2")
        self.assertEqual(sig.namespace, "art-release--signing")
        self.assertEqual(sig.on_behalf_of, "jupierce@redhat.com")
        self.assertEqual(sig.task_git_url, KonfluxSignatory.DEFAULT_TASK_GIT_URL)
        self.assertEqual(sig.task_git_revision, KonfluxSignatory.DEFAULT_TASK_GIT_REVISION)

    def test_constructor_env_overrides(self):
        """Constructor reads from environment variables when provided."""
        env = {
            "KONFLUX_SIGNING_NAMESPACE": "custom-ns",
            "KONFLUX_TA_REGISTRY": "registry.example.com/ta",
        }
        with patch.dict("os.environ", env):
            sig = KonfluxSignatory(sig_keyname="redhatrelease2")
        self.assertEqual(sig.namespace, "custom-ns")
        self.assertEqual(sig.ta_registry, "registry.example.com/ta")
        # on_behalf_of uses the module constant, not an env var
        self.assertEqual(sig.on_behalf_of, "jupierce@redhat.com")

    def test_rate_limit_labels(self):
        """Rate-limiting labels match the signing infrastructure requirements."""
        self.assertEqual(
            KonfluxSignatory.RATE_LIMIT_LABELS,
            {
                "internal-services.appstudio.openshift.io/rate-limited": "true",
                "internal-services.appstudio.openshift.io/rate-limiting-group": "signing-server",
            },
        )

    @patch("pyartcd.signatory.KonfluxSignatory._sign_content", new_callable=AsyncMock)
    async def test_sign_json_digest_constructs_claim(self, mock_sign_content):
        """sign_json_digest builds the correct JSON claim and calls _sign_content."""
        sig = self._create_signatory()
        sig_file = BytesIO()

        await sig.sign_json_digest(
            product="openshift",
            release_name="4.16.1",
            pullspec="quay.io/ocp-release:4.16.1",
            digest="sha256:abc123",
            sig_file=sig_file,
        )

        mock_sign_content.assert_awaited_once()
        call_args = mock_sign_content.call_args
        content = call_args[0][0]
        filename = call_args[0][1]

        # Verify JSON claim structure
        claim = json.loads(content)
        self.assertEqual(claim["critical"]["image"]["docker-manifest-digest"], "sha256:abc123")
        self.assertEqual(claim["critical"]["type"], "atomic container signature")
        self.assertEqual(claim["critical"]["identity"]["docker-reference"], "quay.io/ocp-release:4.16.1")
        self.assertEqual(claim["optional"]["creator"], "Red Hat OpenShift Signing Authority 0.0.1")

        # Verify filename format
        self.assertEqual(filename, "sha256=abc123.json")

    @patch("pyartcd.signatory.KonfluxSignatory._sign_content", new_callable=AsyncMock)
    async def test_sign_message_digest_reads_artifact(self, mock_sign_content):
        """sign_message_digest reads artifact content and calls _sign_content."""
        sig = self._create_signatory()
        artifact = BytesIO(b"fake sha256sum content")
        sig_file = BytesIO()

        await sig.sign_message_digest(
            product="openshift",
            release_name="4.16.1",
            artifact=artifact,
            sig_file=sig_file,
        )

        mock_sign_content.assert_awaited_once()
        call_args = mock_sign_content.call_args
        content = call_args[0][0]
        filename = call_args[0][1]

        self.assertEqual(content, b"fake sha256sum content")
        self.assertEqual(filename, "sha256sum.txt")

    @patch("pyartcd.signatory.KonfluxSignatory._extract_signature", new_callable=AsyncMock)
    @patch("pyartcd.signatory.KonfluxSignatory._pull_trusted_artifact", new_callable=AsyncMock)
    @patch("pyartcd.signatory.KonfluxSignatory._wait_for_completion", new_callable=AsyncMock)
    @patch("pyartcd.signatory.KonfluxSignatory._create_internal_request", new_callable=AsyncMock)
    @patch("pyartcd.signatory.KonfluxSignatory._push_trusted_artifact", new_callable=AsyncMock)
    async def test_sign_content_orchestrates_pipeline(
        self, mock_push, mock_create_ir, mock_wait, mock_pull, mock_extract
    ):
        """_sign_content orchestrates the full push→IR→wait→pull→extract flow."""
        sig = self._create_signatory()
        sig._api = MagicMock()  # Skip k8s init

        mock_push.return_value = "oci:quay.io/test/ta:art-input-test"
        mock_create_ir.return_value = "art-sign-test123"
        mock_wait.return_value = "oci:quay.io/test/ta:art-output-test"

        sig_file = BytesIO()
        await sig._sign_content(b"test content", "test.txt", sig_file)

        # Verify the full orchestration sequence
        mock_push.assert_awaited_once()
        mock_create_ir.assert_awaited_once()
        mock_wait.assert_awaited_once_with("art-sign-test123")
        mock_pull.assert_awaited_once()
        mock_extract.assert_awaited_once()

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_push_trusted_artifact_calls_oras(self, mock_cmd):
        """_push_trusted_artifact invokes oras push with the correct arguments."""
        mock_cmd.return_value = (0, "Pushed", "")
        sig = self._create_signatory(ta_registry="quay.io/test/ta")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test file
            test_file = os.path.join(tmpdir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test content")

            result = await sig._push_trusted_artifact(tmpdir, "req-123")

        self.assertTrue(result.startswith("oci:"))
        mock_cmd.assert_awaited_once()
        cmd = mock_cmd.call_args[0][0]
        self.assertEqual(cmd[0], "oras")
        self.assertEqual(cmd[1], "push")
        self.assertIn("quay.io/test/ta:art-input-req-123", cmd[2])

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_push_trusted_artifact_raises_on_failure(self, mock_cmd):
        """_push_trusted_artifact raises SignatoryServerError on oras failure."""
        mock_cmd.return_value = (1, "", "auth error")
        sig = self._create_signatory()

        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test")

            with self.assertRaises(SignatoryServerError):
                await sig._push_trusted_artifact(tmpdir, "req-fail")

    async def test_create_internal_request_body(self):
        """_create_internal_request builds correct CR with rate-limiting labels."""
        sig = self._create_signatory(
            sig_keyname="redhatrelease2",
            namespace="test-ns",
            on_behalf_of="test-user",
            task_git_url="https://gitlab.example.com/signing/signing.git",
            task_git_revision="v1.0",
        )
        mock_api = MagicMock()
        sig._api = mock_api

        # Run in executor calls the lambda, so mock run_in_executor to call it directly
        loop = asyncio.get_event_loop()

        async def fake_run_in_executor(executor, fn):
            return fn()

        with patch.object(loop, "run_in_executor", side_effect=fake_run_in_executor):
            await sig._create_internal_request("art-sign-test", "oci:quay.io/test:input")

        mock_api.create_namespaced_custom_object.assert_called_once()
        call_kwargs = mock_api.create_namespaced_custom_object.call_args[1]

        self.assertEqual(call_kwargs["group"], "appstudio.redhat.com")
        self.assertEqual(call_kwargs["version"], "v1alpha1")
        self.assertEqual(call_kwargs["namespace"], "test-ns")
        self.assertEqual(call_kwargs["plural"], "internalrequests")

        body = call_kwargs["body"]
        self.assertEqual(body["metadata"]["name"], "art-sign-test")
        self.assertEqual(body["metadata"]["namespace"], "test-ns")

        # Verify rate-limiting labels
        labels = body["metadata"]["labels"]
        self.assertEqual(labels["internal-services.appstudio.openshift.io/rate-limited"], "true")
        self.assertEqual(labels["internal-services.appstudio.openshift.io/rate-limiting-group"], "signing-server")
        self.assertEqual(labels["art.openshift.io/signing-request"], "true")

        # Verify params
        params = body["spec"]["params"]
        self.assertEqual(params["sourceDataArtifact"], "oci:quay.io/test:input")
        self.assertEqual(params["keyname"], "redhatrelease2")
        self.assertEqual(params["signMethod"], "gpgsign")
        self.assertEqual(params["onbehalfof"], "test-user")
        self.assertEqual(params["taskGitUrl"], "https://gitlab.example.com/signing/signing.git")
        self.assertEqual(params["taskGitRevision"], "v1.0")

    async def test_wait_for_completion_success(self):
        """_wait_for_completion returns result URI on success."""
        sig = self._create_signatory()
        mock_api = MagicMock()
        sig._api = mock_api

        mock_api.get_namespaced_custom_object.return_value = {
            "status": {
                "conditions": [{"type": "Succeeded", "status": "True"}],
                "results": {"sourceDataArtifact": "oci:quay.io/test:output"},
            }
        }

        loop = asyncio.get_event_loop()

        async def fake_run_in_executor(executor, fn):
            return fn()

        with patch.object(loop, "run_in_executor", side_effect=fake_run_in_executor):
            result = await sig._wait_for_completion("test-ir")

        self.assertEqual(result, "oci:quay.io/test:output")

    async def test_wait_for_completion_failure(self):
        """_wait_for_completion raises on failed InternalRequest."""
        sig = self._create_signatory()
        mock_api = MagicMock()
        sig._api = mock_api

        mock_api.get_namespaced_custom_object.return_value = {
            "status": {
                "conditions": [
                    {
                        "type": "Succeeded",
                        "status": "False",
                        "reason": "PipelineFailed",
                        "message": "signing error details",
                    }
                ],
            }
        }

        loop = asyncio.get_event_loop()

        async def fake_run_in_executor(executor, fn):
            return fn()

        with patch.object(loop, "run_in_executor", side_effect=fake_run_in_executor):
            with self.assertRaises(SignatoryServerError) as ctx:
                await sig._wait_for_completion("test-ir")
        self.assertIn("PipelineFailed", str(ctx.exception))
        self.assertIn("signing error details", str(ctx.exception))

    async def test_wait_for_completion_timeout(self):
        """_wait_for_completion raises on timeout."""
        sig = self._create_signatory(timeout=0.05, poll_interval=0.01)
        mock_api = MagicMock()
        sig._api = mock_api

        # Always return "still running"
        mock_api.get_namespaced_custom_object.return_value = {
            "status": {
                "conditions": [{"type": "Succeeded", "status": "Unknown"}],
            }
        }

        loop = asyncio.get_event_loop()

        async def fake_run_in_executor(executor, fn):
            return fn()

        with patch.object(loop, "run_in_executor", side_effect=fake_run_in_executor):
            with self.assertRaises(SignatoryServerError) as ctx:
                await sig._wait_for_completion("test-ir")
        self.assertIn("timed out", str(ctx.exception))

    async def test_extract_signature_success(self):
        """_extract_signature reads results.json and writes the signature."""
        sig = self._create_signatory()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create signatures directory and file
            sig_dir = os.path.join(tmpdir, "signatures")
            os.makedirs(sig_dir)
            sig_content = b"-----BEGIN PGP SIGNATURE-----\nfake-signature\n-----END PGP SIGNATURE-----"
            with open(os.path.join(sig_dir, "0_abc123.sig"), "wb") as f:
                f.write(sig_content)

            # Create results.json
            results = {
                "results": [
                    {
                        "file": "sha256sum.txt",
                        "signature_file": "signatures/0_abc123.sig",
                        "key": "redhatrelease2",
                    }
                ]
            }
            with open(os.path.join(tmpdir, "results.json"), "w") as f:
                json.dump(results, f)

            sig_file = BytesIO()
            await sig._extract_signature(tmpdir, "sha256sum.txt", sig_file)

        self.assertEqual(sig_file.getvalue(), sig_content)

    async def test_extract_signature_missing_results_json(self):
        """_extract_signature raises when results.json is missing."""
        sig = self._create_signatory()

        with tempfile.TemporaryDirectory() as tmpdir:
            sig_file = BytesIO()
            with self.assertRaises(SignatoryServerError) as ctx:
                await sig._extract_signature(tmpdir, "sha256sum.txt", sig_file)
            self.assertIn("results.json not found", str(ctx.exception))

    async def test_extract_signature_no_matching_file(self):
        """_extract_signature raises when no matching entry exists in results.json."""
        sig = self._create_signatory()

        with tempfile.TemporaryDirectory() as tmpdir:
            results = {"results": [{"file": "other.txt", "signature_file": "signatures/0.sig", "key": "test"}]}
            with open(os.path.join(tmpdir, "results.json"), "w") as f:
                json.dump(results, f)

            sig_file = BytesIO()
            with self.assertRaises(SignatoryServerError) as ctx:
                await sig._extract_signature(tmpdir, "sha256sum.txt", sig_file)
            self.assertIn("No signature found", str(ctx.exception))

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_pull_trusted_artifact_strips_oci_prefix(self, mock_cmd):
        """_pull_trusted_artifact strips oci: prefix and calls oras pull."""
        mock_cmd.return_value = (0, "Pulled", "")
        sig = self._create_signatory()

        with tempfile.TemporaryDirectory() as tmpdir:
            await sig._pull_trusted_artifact("oci:quay.io/test:output", tmpdir)

        mock_cmd.assert_awaited_once()
        cmd = mock_cmd.call_args[0][0]
        self.assertEqual(cmd[0], "oras")
        self.assertEqual(cmd[1], "pull")
        self.assertEqual(cmd[2], "quay.io/test:output")

    async def test_context_manager(self):
        """KonfluxSignatory supports async context manager."""
        sig = self._create_signatory()
        with patch("pyartcd.signatory.KonfluxSignatory._ensure_k8s_client"):
            async with sig as s:
                self.assertIs(s, sig)


class TestCreateSignatory(IsolatedAsyncioTestCase):
    """Tests for the create_signatory factory function."""

    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    def test_create_umb_signatory_prod(self, _):
        """create_signatory('umb', 'prod') creates AsyncSignatory with prod key."""
        sig = create_signatory(
            transport="umb",
            signing_env="prod",
            uri="stomp+ssl://umb.example.com:61612",
            cert_file="/path/to/cert",
            key_file="/path/to/key",
        )
        self.assertIsInstance(sig, AsyncSignatory)
        self.assertEqual(sig.sig_keyname, PROD_SIGNING_KEY_NAME)

    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    def test_create_umb_signatory_stage(self, _):
        """create_signatory('umb', 'stage') creates AsyncSignatory with stage key."""
        sig = create_signatory(
            transport="umb",
            signing_env="stage",
            uri="stomp+ssl://umb.stage.example.com:61612",
            cert_file="/path/to/cert",
            key_file="/path/to/key",
        )
        self.assertIsInstance(sig, AsyncSignatory)
        self.assertEqual(sig.sig_keyname, STAGE_SIGNING_KEY_NAME)

    def test_create_konflux_signatory_prod(self):
        """create_signatory('konflux', 'prod') creates KonfluxSignatory with prod key."""
        sig = create_signatory(
            transport="konflux",
            signing_env="prod",
            namespace="test-ns",
        )
        self.assertIsInstance(sig, KonfluxSignatory)
        self.assertEqual(sig.sig_keyname, PROD_SIGNING_KEY_NAME)

    def test_create_konflux_signatory_stage(self):
        """create_signatory('konflux', 'stage') creates KonfluxSignatory with stage key."""
        sig = create_signatory(
            transport="konflux",
            signing_env="stage",
            namespace="test-ns",
        )
        self.assertIsInstance(sig, KonfluxSignatory)
        self.assertEqual(sig.sig_keyname, STAGE_SIGNING_KEY_NAME)

    def test_create_signatory_invalid_transport(self):
        """create_signatory raises ValueError for unknown transport."""
        with self.assertRaises(ValueError) as ctx:
            create_signatory(transport="invalid", signing_env="prod")
        self.assertIn("Unknown signing transport", str(ctx.exception))

    @patch("pyartcd.signatory.AsyncUMBClient", autospec=True)
    @patch.dict("os.environ", {"SIGNING_CERT": "/env/cert.pem", "SIGNING_KEY": "/env/key.pem"})
    def test_create_umb_signatory_uses_env_vars(self, _):
        """create_signatory('umb') falls back to SIGNING_CERT/KEY env vars."""
        sig = create_signatory(transport="umb", signing_env="prod")
        self.assertIsInstance(sig, AsyncSignatory)
