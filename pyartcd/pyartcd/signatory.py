
import asyncio
import base64
import io
import itertools
import json
import logging
import os
import aiohttp
from random import uniform
import uuid
from datetime import datetime, timedelta
from tenacity import retry, wait_random_exponential, stop_after_attempt
from typing import Set, Iterable, List, BinaryIO, Dict, cast

import aiofiles
from cryptography import x509
from cryptography.x509.oid import NameOID

from artcommonlib import exectools
from pyartcd.exceptions import SignatoryServerError
from pyartcd.umb_client import AsyncUMBClient
from pyartcd.oc import get_release_image_info, get_image_info
from artcommonlib.util import run_limited_unordered

_LOGGER = logging.getLogger(__name__)


class AsyncSignatory:
    """
    AsyncSignatory can sign OCP artifacts by sending a signing request to RADAS over UMB.

    Example usage:
    ```
    uri = "stomp+ssl://umb.stage.api.redhat.com:61612"
    cert_file = "ssl/nonprod-openshift-art-bot.crt"
    key_file = "ssl/nonprod-openshift-art-bot.key"
    async with AsyncSignatory(uri, cert_file, key_file, sig_keyname="beta2", requestor="yuxzhu") as signatory:
        pullspec = "quay.io/openshift-release-dev/ocp-release:4.11.31-x86_64"
        digest = "sha256:cc10900ad98b44ba432bc0d99e7d4fffb5498fd6844fc3b6a0a3552ee6d64059"
        # sign a release payload
        with open("signature-1", "wb") as sig_file:
            await signatory.sign_json_digest("openshift", "4.11.31", pullspec, digest, sig_file)
        # sign a message digest
        with open("sha256sum.txt", "rb") as in_file, open("sha256sum.txt.gpg", "wb") as sig_file:
            await signatory.sign_message_digest("openshift", "4.11.31", in_file, sig_file)
    ```
    """

    SEND_DESTINATION = '/topic/VirtualTopic.eng.art.artifact.sign'
    CONSUMER_QUEUE_TEMPLATE = "/queue/Consumer.{service_account}.{subscription}.VirtualTopic.eng.robosignatory.art.sign"

    def __init__(
        self,
        uri: str,
        cert_file: str,
        key_file: str,
        sig_keyname="test",
        requestor="timer",
        subscription_name="artcd",
    ):
        self.cert_file = cert_file
        self.sig_keyname = sig_keyname
        self.requestor = requestor
        self.subscription_name = subscription_name
        self._umb = AsyncUMBClient(uri, cert_file, key_file)
        self._receiver = None
        self._receiver_task = None
        self._requests: Dict[str, asyncio.Future] = {}
        self._loop = asyncio.get_event_loop()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @staticmethod
    async def _get_certificate_account_name(cert_file: str):
        """ Get service account name embedded in the certificate file
        """
        async with aiofiles.open(cert_file, "rb") as f:
            cert = x509.load_pem_x509_certificate(await f.read())
        return cert.subject.get_attributes_for_oid(NameOID.USER_ID)[0].value

    async def start(self):
        # Get service account name embedded in the client certificate
        service_account = await self._get_certificate_account_name(self.cert_file)
        _LOGGER.info("Using UMB service account: %s", service_account)
        # Connect to UMB
        await self._umb.connect()
        # Subscribe to the consumer queue
        # e.g. /queue/Consumer.openshift-art-bot.artcd.VirtualTopic.eng.robosignatory.art.sign
        consumer_queue = self.CONSUMER_QUEUE_TEMPLATE.format_map({
            "service_account": service_account,
            "subscription": self.subscription_name
        })
        self._receiver = await self._umb.subscribe(consumer_queue, self.subscription_name)
        # Start a task to handle messages received from the consumer queue
        self._receiver_task = asyncio.create_task(self._handle_messages())

    async def close(self):
        """ Closes connection to UMB
        """
        await self._umb.close()
        # self._receiver_task will stop until receives EOF or it was garbage collected
        self._receiver_task = None
        self._receiver = None

    async def _handle_messages(self):
        """ Handles messages received from the consumer queue
        """
        receiver = self._receiver
        assert receiver, "start() was not called"
        async for message in receiver.iter_messages():
            message_id = str(message.headers["message-id"])
            timestamp = int(message.headers["timestamp"]) / 1000
            try:
                age = datetime.utcnow() - datetime.utcfromtimestamp(timestamp)
                if age >= timedelta(hours=1):
                    _LOGGER.warning("Discarding stale message {}".format(message_id))
                    await self._umb.ack(message_id, receiver.id)  # discard the message
                    continue
                body = json.loads(str(message.body))
                request_id = body["msg"]["request_id"]
                fut = self._requests.get(request_id)
                if not fut:
                    _LOGGER.warning("Unknown request_id %s in message %s", request_id, message_id)
                    continue
                fut.set_result((message.headers, body))
                await self._umb.ack(message_id, receiver.id)  # consume the message
            except Exception:
                _LOGGER.exception("Error handling message %s", message_id)
        _LOGGER.info("_handle_message: exited")

    async def _sign_artifact(
        self,
        typ: str,
        product: str,
        release_name: str,
        name: str,
        artifact: BinaryIO,
        sig_file: BinaryIO,
    ):
        """ Signs an artifact
        """
        # Create a signing request
        # Example request: https://datagrepper.stage.engineering.redhat.com/id?id=ID:umb-stage-3.umb-001.preprod.us-east-1.aws.redhat.com-38533-1689629292398-10:23520:-1:1:1&is_raw=true&size=extra-large
        artifact_base64 = io.BytesIO()
        base64.encode(artifact, artifact_base64)
        request_id = (
            f'{product}-{typ}-{datetime.utcnow().strftime("%Y%m%d%H%M%S")}-{uuid.uuid4()}'
        )
        message = {
            "artifact": artifact_base64.getvalue().decode(),
            "artifact_meta": {
                "product": product,
                "release_name": release_name,
                "name": name,
                "type": typ,
            },
            "request_id": request_id,
            "requestor": self.requestor,
            "sig_keyname": self.sig_keyname,
        }
        request_body = json.dumps(message)

        # Send the signing request via UMB
        fut = self._loop.create_future()
        self._requests[request_id] = fut
        try:
            await self._umb.send(self.SEND_DESTINATION, request_body)
            _, response_body = await fut
        finally:
            del self._requests[request_id]

        # example response: https://datagrepper.stage.engineering.redhat.com/id?id=2019-0304004b-d1e6-4e03-b28d-cfa1e5f59948&is_raw=true&size=extra-large
        if response_body["msg"]["signing_status"] != "success":
            err = ", ".join(response_body["msg"]["errors"])
            raise SignatoryServerError(f"Robo Signatory declined: {err}")
        input = io.BytesIO(response_body["msg"]["signed_artifact"].encode())
        base64.decode(input, sig_file)
        artifact_meta = cast(Dict[str, str], response_body["msg"]["artifact_meta"])
        return artifact_meta

    async def sign_json_digest(
        self, product: str, release_name: str, pullspec: str, digest: str, sig_file: BinaryIO
    ):
        """ Sign a JSON digest claim
        """
        json_claim = {
            "critical": {
                "image": {"docker-manifest-digest": digest},
                "type": "atomic container signature",
                "identity": {
                    "docker-reference": pullspec,
                },
            },
            "optional": {
                "creator": "Red Hat OpenShift Signing Authority 0.0.1",
            },
        }
        artifact = io.BytesIO(json.dumps(json_claim).encode())
        name = digest.replace(":", "=")
        signature_meta = await self._sign_artifact(
            typ="json-digest",
            product=product,
            release_name=release_name,
            name=name,
            artifact=artifact,
            sig_file=sig_file,
        )
        return signature_meta

    async def sign_message_digest(
        self, product: str, release_name: str, artifact: BinaryIO, sig_file: BinaryIO
    ):
        """ Sign a message digest
        """
        name = "sha256sum.txt.gpg"
        signature_meta = await self._sign_artifact(
            typ="message-digest",
            product=product,
            release_name=release_name,
            name=name,
            artifact=artifact,
            sig_file=sig_file,
        )
        return signature_meta


class SigstoreSignatory:
    """
    SigstoreSignatory uses sigstore's cosign to sign container image manifests keylessly and publish
    the signatures in the registry next to the images. This is a class for finding manifests to sign
    from a release and signing them.
    """

    # there are a number of ways in which we might run into rate limits when examining and signing
    # as fast as possible. to prevent this, we will introduce a small amount of jitter to each
    # concurrent attempt. if we find we are still hitting rate limits, we can increase this delay.
    THROTTLE_DELAY = 1.0  # jittered delay for each examining or signing attempt

    # strip out any AWS_ environment variables that might interfere with KMS
    ENV = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    # it's easier to set AWS_REGION for now than to create a whole AWS_CONFIG_FILE
    ENV["AWS_REGION"] = "us-east-1"

    def __init__(self, logger, dry_run: bool, signing_creds: str,
                 signing_key_ids: List[str], rekor_url: str,
                 concurrency_limit: int, sign_release: bool, sign_components: bool,
                 verify_release: bool) -> None:
        self._logger = logger
        self.dry_run = dry_run  # if true, run discovery but do not sign anything
        self.signing_key_ids = signing_key_ids  # key ids for signing
        self.rekor_url = rekor_url  # rekor server for cosign tlog storage
        self.ENV["AWS_SHARED_CREDENTIALS_FILE"] = signing_creds  # filename for KMS credentials
        self.concurrency_limit = concurrency_limit  # limit on concurrent lookups or signings
        self.sign_release = sign_release  # whether to sign release images that we examine
        self.sign_components = sign_components  # whether to sign component images that we examine
        self.verify_release = verify_release  # require a legacy signature on release images

    @staticmethod
    def redigest_pullspec(pullspec, digest):
        """ form the pullspec for a digest in the same repo as an existing pullspec """
        if len(halves := pullspec.split("@sha256:")) == 2:  # assume that was a digest at the end
            return f"{halves[0]}@{digest}"
        elif len(halves := pullspec.rsplit(":", 1)) == 2:
            # assume that was a tag at the end, while allowing for ":" in the registry spec
            return f"{halves[0]}@{digest}"
        return f"{pullspec}@{digest}"  # assume it was a bare registry/repo

    async def discover_pullspecs(
            self, pullspecs: Iterable[str], release_name: str
    ) -> (Set[str], Dict[str, Exception]):
        """
        Recursively discover pullspecs that need signatures. Given manifest lists, examine the
        digests of each platform. Given a release image, examine the digests of all payload
        components. Come up with a list of the individual manifests we will actually sign.

        :param pullspecs: List of pullspecs to begin discovery
        :param release_name: Require any release images to have this release name
        :return: a set of discovered pullspecs to sign, and a dict of any discovery errors
        """
        seen: Set[str] = set(pullspecs)  # prevent re-examination and multiple signings
        need_signing: Set[str] = set()   # pullspecs for manifests to be signed
        errors: Dict[str, Exception] = {}  # pullspec -> error when examining it

        need_examining: List[str] = list(pullspecs)
        while need_examining:
            args = [(ps, release_name) for ps in need_examining]
            results = await run_limited_unordered(self._examine_pullspec, args, self.concurrency_limit)

            need_examining = []
            for next_signing, next_examining, next_errors in results:
                need_signing.update(next_signing)
                errors.update(next_errors)
                for ps in next_examining:
                    if ps not in seen:
                        seen.add(ps)
                        need_examining.append(ps)

        return need_signing, errors

    async def _examine_pullspec(
            self, pullspec: str, release_name: str
    ) -> (Set[str], Set[str], Dict[str, Exception]):
        """
        Determine what a pullspec is (single manifest, manifest list, release image) and
        recursively add it or its references. limit concurrency or we can run out of processes.
        :param pullspec: Pullspec to be signed
        :param release_name: Require any release images to have this release name
        :return: pullspecs needing signing, pullspecs needing examining, and any discovery errors
        """
        need_signing: Set[str] = set()
        need_examining: Set[str] = set()
        errors: Dict[str, Exception] = {}

        await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))  # introduce jitter to avoid rate limits
        img_info = await get_image_info(pullspec, True)

        if isinstance(img_info, list):  # pullspec is for a manifest list
            self._logger.info("%s is a manifest list", pullspec)
            # [lmeyer] AFAICS there is no signing for manifest lists, only manifests; cosign given a
            # manifest list signs the manifests, and podman etc do not even look for a signature for
            # the list, only the final image to be downloaded. we do however need to examine each
            # manifest to see if that might be a release image.
            for manifest in img_info:
                need_examining.add(self.redigest_pullspec(manifest["name"], manifest["digest"]))
        elif (this_rn := img_info["config"]["config"]["Labels"].get("io.openshift.release")):
            # release image; get references and examine those
            self._logger.info("%s is a release image with name %s", pullspec, this_rn)
            if release_name != this_rn:
                errors[pullspec] = RuntimeError(
                    f"release image at {pullspec} has release name {this_rn}, not the expected {release_name}"
                )
            elif self.verify_release and not await self.verify_legacy_signature(img_info):
                errors[pullspec] = RuntimeError(
                    f"release image at {pullspec} does not have a required legacy signature"
                )
            else:
                if self.sign_components:
                    # look up the components referenced by this release image
                    try:
                        for child_spec in await self.get_release_image_references(pullspec):
                            need_examining.add(child_spec)
                            # [lmeyer] it might seem unnecessary to examine component images. we _could_
                            # just give them to cosign to sign (recursively, to cover multiarch
                            # components). however, with multiarch releases, this would lead to signing
                            # most manifests at least five times (once for the single-arch release
                            # image, and once for each arch in the multi-arch release), and "pod"
                            # fillers even more; if we only ever sign at the level of manifests, we can
                            # ensure we sign only once per release.
                    except RuntimeError as exc:
                        errors[pullspec] = exc
                # also plan to sign the release image itself
                if self.sign_release:
                    need_signing.add(pullspec)
        else:  # pullspec is for a normal image manifest
            self._logger.info("%s is a single manifest", pullspec)
            if self.sign_components:
                need_signing.add(pullspec)

        return need_signing, need_examining, errors

    @staticmethod
    async def get_release_image_references(pullspec: str) -> Set[str]:
        """ Retrieve the pullspecs referenced by a release image """
        return set(
            tag["from"]["name"]
            for tag in (await get_release_image_info(pullspec))["references"]["spec"]["tags"]
        )

    async def sign_pullspecs(self, need_signing: Iterable[str]) -> Dict[str, Exception]:
        """
        Sign the given pullspecs via cosign with our KMS.
        :param need_signing: Pullspecs to be signed
        :return: dict with any signing errors per pullspec
        """
        args = [(ps, ) for ps in need_signing]
        results = await run_limited_unordered(self._sign_single_manifest, args, self.concurrency_limit)
        return {pullspec: err for result in results for pullspec, err in result.items()}

    async def verify_legacy_signature(self, img_info: Dict) -> bool:
        """
        Verify the signature from mirror.openshift.com matches the release image and RH public key
        :param img_info: the oc image info structure from the image
        :return: True if valid signature found, False otherwise
        """
        sha = img_info["digest"].removeprefix("sha256:")
        async with aiohttp.ClientSession() as session:
            for sig in itertools.count(1):
                # there can be more than one signature, look until we run out
                url = "https://mirror.openshift.com/pub/openshift-v4/signatures/openshift-release-dev/"
                url += f"ocp-release/sha256={sha}/signature-{sig}"
                async with session.get(url) as response:
                    if response.status != 200:
                        return False  # no more signatures found, verification failed

                    # [lmeyer] at this point ideally we would verify the signature is signed by the
                    # right key and matches the image. however this turns out to be unreasonably
                    # complicated with existing tools. instead, we will take the existence of the
                    # signature file at the right shasum on our mirror as sufficient evidence that
                    # we signed the image before. i do not see a plausible risk resulting.
                    self._logger.info(f"found sig file at {url}")
                    return True

    async def _sign_single_manifest(self, pullspec: str) -> Dict[str, Exception]:
        """
        use sigstore to sign a single image manifest, with one or more signing keys, and upload the signature
        :param pullspec: Pullspec to be signed
        :return: dict with any signing errors for pullspec
        """
        log = self._logger
        for signing_key_id in self.signing_key_ids:
            cmd = ["cosign", "sign",
                   "--yes",
                   # https://issues.redhat.com/browse/ART-10052
                   f"--sign-container-identity={pullspec}",
                   "--key", f"awskms:///{signing_key_id}",
                   ]

            if self.rekor_url:
                cmd.append(f"--rekor-url={self.rekor_url}")
            else:
                cmd.append("--tlog-upload=false")

            cmd.append(pullspec)

            if self.dry_run:
                log.info("[DRY RUN] Would have signed image: %s", cmd)
                continue

            log.info("Signing %s with %s...", pullspec, signing_key_id)
            try:
                stdout = await self._retrying_sign_single_manifest(cmd)
                log.debug("Successfully signed %s with %s:\n%s", pullspec, signing_key_id, stdout)
                await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))  # introduce jitter to avoid rate limits
            except Exception as exc:
                log.error("Failure signing %s with %s:\n%s", pullspec, signing_key_id, exc)
                return {pullspec: exc}

        return {}

    @retry(wait=wait_random_exponential(), stop=stop_after_attempt(5), reraise=True)
    async def _retrying_sign_single_manifest(self, cmd: List[str]) -> str:
        await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))  # introduce jitter to avoid rate limits
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False, env=self.ENV)
        if rc:
            raise RuntimeError(stderr)
        return stdout
