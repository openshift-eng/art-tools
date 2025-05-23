import asyncio
import base64
import json
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID

from pyartcd.signatory import AsyncSignatory


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
