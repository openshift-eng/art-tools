import asyncio
import base64
import json
import os
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID
from pyartcd.signatory import AsyncSignatory, SigstoreSignatory


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
        # No rekor_url configured -> ignore tlog, mirroring sign-time --tlog-upload=false
        self.assertIn("--insecure-ignore-tlog=true", cmd)

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_signature_exists_appends_rekor_url(self, mock_cmd):
        """_signature_exists uses --rekor-url when rekor_url is configured (mirrors sign-time flags)"""
        signatory = self._create_signatory(rekor_url="https://rekor.example.com")
        mock_cmd.return_value = (0, json.dumps([]), "")

        await signatory._signature_exists("quay.io/x@sha256:a", "quay.io/x:tag", "test-key-id")

        cmd = mock_cmd.await_args.args[0]
        self.assertIn("--rekor-url=https://rekor.example.com", cmd)
        self.assertNotIn("--insecure-ignore-tlog=true", cmd)

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
        # A signature object exists, so the precise per-identity verify path is exercised.
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=True)
        mock_cmd.return_value = (0, "ok", "")

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        # Both identities already signed -> no sign commands issued
        self.assertEqual(mock_cmd.await_count, 0)
        self.assertEqual(signatory._signature_exists.await_count, 2)

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_signs_when_signature_absent(self, mock_cmd, mock_sleep):
        """_sign_manifest signs when our signature does not yet exist"""
        signatory = self._create_signatory(dry_run=False)
        # A signature object exists, so the precise per-identity verify path is exercised.
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(return_value=False)
        mock_cmd.return_value = (0, "ok", "")

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        # Two identities (digest + tag), one key each -> two sign commands
        self.assertEqual(mock_cmd.await_count, 2)
        for call in mock_cmd.await_args_list:
            self.assertEqual(call.args[0][:2], ["cosign", "sign"])

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_signs_only_missing_identity(self, mock_cmd, mock_sleep):
        """
        Per-identity skipping contract: when the digest identity is already signed but the tag
        identity is not, both identities are checked and ONLY the missing tag identity gets a
        `cosign sign` command. This is the RHCOS/kubevirt case (ART-23308) that motivated the
        per-identity check -- the tag identity must still be signed even though the digest is done.
        """
        signatory = self._create_signatory(dry_run=False)
        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        digest_identity = pullspec
        tag_identity = "quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64"

        # Key the existence check by identity: digest is signed, tag is not.
        signed_by_identity = {digest_identity: True, tag_identity: False}

        async def fake_exists(ps, identity, key_id):
            return signed_by_identity[identity]

        # A signature object exists, so the precise per-identity verify path is exercised.
        signatory._any_signature_exists = AsyncMock(return_value=True)
        signatory._signature_exists = AsyncMock(side_effect=fake_exists)
        mock_cmd.return_value = (0, "ok", "")

        result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        # Both identities were evaluated independently.
        self.assertEqual(signatory._signature_exists.await_count, 2)
        checked_identities = {call.args[1] for call in signatory._signature_exists.await_args_list}
        self.assertEqual(checked_identities, {digest_identity, tag_identity})

        # Exactly one sign command, and it is for the missing tag identity only.
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
        # Both existence checks are bypassed entirely, and both identities are signed
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
        # QUAY_AUTH_FILE is passed through as --authfile (skopeo understands path-scoped keys).
        self.assertIn("--authfile", cmd)
        self.assertEqual(cmd[cmd.index("--authfile") + 1], "/path/to/auth.json")
        # The derived cosign signature tag is <repo>:sha256-<hex>.sig.
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
        """_any_signature_exists fails OPEN (returns True) on an auth/other error.

        A bare 'not found' (e.g. 'skopeo: command not found') is NOT a registry not-found
        signal, so it is treated as ambiguous rather than 'no signature'.
        """
        signatory = self._create_signatory()
        mock_cmd.return_value = (1, "", "unauthorized: authentication required")

        result = await signatory._any_signature_exists("quay.io/x/y@sha256:abc123")
        self.assertTrue(result)

        # A bare "not found" must not be mistaken for a registry not-found signal.
        mock_cmd.return_value = (127, "", "bash: skopeo: command not found")
        result = await signatory._any_signature_exists("quay.io/x/y@sha256:abc123")
        self.assertTrue(result)

    async def test_any_signature_exists_true_when_no_digest(self):
        """_any_signature_exists returns True (can't derive .sig tag) for a non-digest pullspec."""
        signatory = self._create_signatory()

        with patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock) as mock_cmd:
            result = await signatory._any_signature_exists("quay.io/x/y:4.16.1-x86_64")

        self.assertTrue(result)
        # No registry call is made when we cannot derive the signature tag.
        mock_cmd.assert_not_awaited()

    @patch("pyartcd.signatory.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_sign_manifest_skips_verify_when_no_sig_object(self, mock_cmd, mock_sleep):
        """When no signature object exists, _signature_exists is never called and all identities sign."""
        signatory = self._create_signatory(dry_run=False)
        # No signature object at all -> skip the per-identity verify entirely.
        signatory._any_signature_exists = AsyncMock(return_value=False)
        signatory._signature_exists = AsyncMock(return_value=True)
        mock_cmd.return_value = (0, "ok", "")

        pullspec = "quay.io/openshift-release-dev/ocp-release@sha256:abc123"
        result = await signatory._sign_manifest(pullspec, canonical_tag="4.16.1-x86_64")

        self.assertEqual(result, {})
        # The pre-check short-circuits the per-identity verify: zero cosign verify calls.
        self.assertEqual(signatory._any_signature_exists.await_count, 1)
        self.assertEqual(signatory._signature_exists.await_count, 0)
        # Both identities (digest + tag), one key each -> two cosign sign commands.
        self.assertEqual(mock_cmd.await_count, 2)
        for call in mock_cmd.await_args_list:
            self.assertEqual(call.args[0][:2], ["cosign", "sign"])

    @patch("pyartcd.signatory.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_signature_exists_false_on_unexpected_json_shapes(self, mock_cmd):
        """_signature_exists fails OPEN (returns False, no raise) on valid JSON with unexpected shapes.

        cosign may exit 0 yet emit something other than a list of well-formed payload dicts. Each of
        these must be treated as 'signature not found' so the caller re-signs rather than raising.
        """
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
