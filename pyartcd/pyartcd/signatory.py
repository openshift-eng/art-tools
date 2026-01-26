import asyncio
import base64
import io
import itertools
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from random import uniform
from typing import BinaryIO, Dict, Iterable, List, Optional, Set, Tuple, cast

import aiofiles
import aiohttp
from artcommonlib import exectools
from artcommonlib.util import run_limited_unordered
from cryptography import x509
from cryptography.x509.oid import NameOID
from tenacity import retry, stop_after_attempt, wait_random_exponential

from pyartcd.exceptions import SignatoryServerError
from pyartcd.oc import get_image_info, get_release_image_info
from pyartcd.umb_client import AsyncUMBClient

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
        """Get service account name embedded in the certificate file"""
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
        consumer_queue = self.CONSUMER_QUEUE_TEMPLATE.format_map(
            {
                "service_account": service_account,
                "subscription": self.subscription_name,
            }
        )
        self._receiver = await self._umb.subscribe(consumer_queue, self.subscription_name)
        # Start a task to handle messages received from the consumer queue
        self._receiver_task = asyncio.create_task(self._handle_messages())

    async def close(self):
        """Closes connection to UMB"""
        await self._umb.close()
        # self._receiver_task will stop until receives EOF or it was garbage collected
        self._receiver_task = None
        self._receiver = None

    async def _handle_messages(self):
        """Handles messages received from the consumer queue"""
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
        """Signs an artifact"""
        # Create a signing request
        # Example request: https://datagrepper.stage.engineering.redhat.com/id?id=ID:umb-stage-3.umb-001.preprod.us-east-1.aws.redhat.com-38533-1689629292398-10:23520:-1:1:1&is_raw=true&size=extra-large
        artifact_base64 = io.BytesIO()
        base64.encode(artifact, artifact_base64)
        request_id = f'{product}-{typ}-{datetime.utcnow().strftime("%Y%m%d%H%M%S")}-{uuid.uuid4()}'
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

    async def sign_json_digest(self, product: str, release_name: str, pullspec: str, digest: str, sig_file: BinaryIO):
        """Sign a JSON digest claim"""
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

    async def sign_message_digest(self, product: str, release_name: str, artifact: BinaryIO, sig_file: BinaryIO):
        """Sign a message digest"""
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


@dataclass
class ReleaseImageInfo:
    """Information about a release image for signing purposes.

    Attributes:
        original_pullspec: The original input pullspec (may be manifest list or single manifest)
        canonical_tag: The canonical tag for this release image (e.g., "4.16.1-x86_64" or "4.16.1-multi")
        manifests_to_sign: List of individual manifest pullspecs discovered from this release image.
            For manifest lists, these are the arch-specific manifests.
            For single manifests, this is just [original_pullspec].
            All manifests will be signed with the same canonical_tag.
    """

    original_pullspec: str
    canonical_tag: str
    manifests_to_sign: List[str] = field(default_factory=list)


@dataclass
class DiscoveryResult:
    """Results from discovering pullspecs to sign.

    Provides clear separation between release images (which need canonical tag signing)
    and component images (which only need digest-based signing).
    """

    # Release images with their canonical tags and discovered manifests
    release_images: List[ReleaseImageInfo] = field(default_factory=list)

    # Component images to sign (no canonical tags needed)
    component_images: Set[str] = field(default_factory=set)

    # Any errors encountered during discovery
    errors: Dict[str, Exception] = field(default_factory=dict)


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

    def __init__(
        self,
        logger,
        dry_run: bool,
        signing_creds: str,
        signing_key_ids: List[str],
        rekor_url: str,
        concurrency_limit: int,
    ) -> None:
        self._logger = logger
        self.dry_run = dry_run  # if true, run discovery but do not sign anything
        self.signing_key_ids = signing_key_ids  # key ids for signing
        self.rekor_url = rekor_url  # rekor server for cosign tlog storage
        self.ENV["AWS_SHARED_CREDENTIALS_FILE"] = signing_creds  # filename for KMS credentials
        self.concurrency_limit = concurrency_limit  # limit on concurrent lookups or signings

    @staticmethod
    def redigest_pullspec(pullspec: str, digest: str) -> str:
        """Form the pullspec for a digest in the same repo as an existing pullspec."""
        if len(halves := pullspec.split("@sha256:")) == 2:  # assume that was a digest at the end
            return f"{halves[0]}@{digest}"
        elif len(halves := pullspec.rsplit(":", 1)) == 2:
            # assume that was a tag at the end, while allowing for ":" in the registry spec
            return f"{halves[0]}@{digest}"
        return f"{pullspec}@{digest}"  # assume it was a bare registry/repo

    async def discover_release_image(
        self,
        pullspec: str,
        canonical_tag: str,
        release_name: str,
        verify_legacy_sig: bool = False,
    ) -> Tuple[ReleaseImageInfo, Dict[str, Exception]]:
        """
        Discover all manifests that need signing for a single release image.

        For manifest lists, this discovers all arch-specific manifests. All discovered manifests
        will be signed with the same canonical_tag, enabling users to verify images pulled via
        the manifest list tag without signedIdentity overrides.

        :param pullspec: The release image pullspec (manifest list or single manifest)
        :param canonical_tag: The canonical tag for this release (e.g., "4.16.1-multi")
        :param release_name: Expected release name label value
        :param verify_legacy_sig: If True, verify legacy signature exists before proceeding
        :return: ReleaseImageInfo with discovered manifests, and any errors
        """
        info = ReleaseImageInfo(
            original_pullspec=pullspec,
            canonical_tag=canonical_tag,
        )
        errors: Dict[str, Exception] = {}
        seen: Set[str] = {pullspec}

        await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))
        img_info = await get_image_info(pullspec, True)

        if isinstance(img_info, list):
            # Manifest list: discover each arch manifest
            self._logger.info("%s is a manifest list with %d manifests", pullspec, len(img_info))
            for manifest in img_info:
                manifest_pullspec = self.redigest_pullspec(manifest["name"], manifest["digest"])
                if manifest_pullspec not in seen:
                    seen.add(manifest_pullspec)
                    info.manifests_to_sign.append(manifest_pullspec)
        else:
            # Single manifest release image
            this_rn = img_info["config"]["config"]["Labels"].get("io.openshift.release")
            if not this_rn:
                errors[pullspec] = RuntimeError(f"{pullspec} is not a release image (missing label)")
            elif release_name and release_name != this_rn:
                # Only validate release name if one was provided
                errors[pullspec] = RuntimeError(
                    f"release image at {pullspec} has release name {this_rn}, not expected {release_name}"
                )
            elif verify_legacy_sig and not await self.verify_legacy_signature(img_info):
                errors[pullspec] = RuntimeError(
                    f"release image at {pullspec} does not have a required legacy signature"
                )
            else:
                # Always use digest-based pullspec for signing (immutable reference)
                digest = img_info.get("digest")
                if digest:
                    digest_pullspec = self.redigest_pullspec(pullspec, digest)
                    info.manifests_to_sign.append(digest_pullspec)
                else:
                    # Fallback to original if no digest available
                    info.manifests_to_sign.append(pullspec)

        return info, errors

    async def discover_component_images(
        self,
        release_pullspec: str,
        release_name: str,
    ) -> Tuple[Set[str], Dict[str, Exception]]:
        """
        Discover all component images referenced by a release image.

        For multiarch components, this recursively discovers the individual arch manifests.
        Component images don't need canonical tag signing - only digest-based signing.

        :param release_pullspec: The release image pullspec
        :param release_name: Expected release name for validation
        :return: Set of component image pullspecs to sign, and any errors
        """
        component_images: Set[str] = set()
        errors: Dict[str, Exception] = {}
        seen: Set[str] = set()

        # Get component references from the release image
        try:
            references = await self.get_release_image_references(release_pullspec)
        except RuntimeError as exc:
            errors[release_pullspec] = exc
            return component_images, errors

        # Examine each component to get individual manifests
        need_examining = list(references)
        seen.update(references)

        while need_examining:
            args = [(ps,) for ps in need_examining]
            results = await run_limited_unordered(self._examine_component, args, self.concurrency_limit)

            next_to_examine = []
            for idx, (to_sign, to_examine, errs) in enumerate(results):
                component_images.update(to_sign)
                errors.update(errs)

                for ps in to_examine:
                    if ps not in seen:
                        seen.add(ps)
                        next_to_examine.append(ps)

            need_examining = next_to_examine

        return component_images, errors

    async def _examine_component(self, pullspec: str) -> Tuple[Set[str], Set[str], Dict[str, Exception]]:
        """
        Examine a component image and return what needs signing/examining.

        :param pullspec: Component image pullspec
        :return: pullspecs to sign, pullspecs to examine further, any errors
        """
        need_signing: Set[str] = set()
        need_examining: Set[str] = set()
        errors: Dict[str, Exception] = {}

        await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))

        try:
            img_info = await get_image_info(pullspec, True)
        except Exception as exc:
            errors[pullspec] = exc
            return need_signing, need_examining, errors

        if isinstance(img_info, list):
            # Manifest list: examine each arch manifest
            self._logger.debug("%s is a manifest list", pullspec)
            for manifest in img_info:
                need_examining.add(self.redigest_pullspec(manifest["name"], manifest["digest"]))
        else:
            # Single manifest: sign it
            self._logger.debug("%s is a single manifest", pullspec)
            need_signing.add(pullspec)

        return need_signing, need_examining, errors

    @staticmethod
    async def get_release_image_references(pullspec: str) -> Set[str]:
        """Retrieve the pullspecs referenced by a release image."""
        return set(
            tag["from"]["name"] for tag in (await get_release_image_info(pullspec))["references"]["spec"]["tags"]
        )

    async def sign_release_images(
        self,
        release_images: List[ReleaseImageInfo],
        tag_only: bool = False,
    ) -> Dict[str, Exception]:
        """
        Sign release image manifests with digest and/or canonical tag identities.

        By default, each manifest is signed twice:
        1. With digest-based identity (e.g., quay.io/.../ocp-release@sha256:...)
        2. With tag-based identity (e.g., quay.io/.../ocp-release:4.16.1-multi)

        The tag-based signature enables customers to verify images referenced by tag
        without needing signedIdentity overrides in their policy.

        :param release_images: List of ReleaseImageInfo with canonical tags and manifests to sign
        :param tag_only: If True, only sign with tag identity (skip digest identity).
            Useful for retroactive signing where digest signatures already exist.
        :return: Dict of any signing errors per pullspec
        """
        # Build args: (pullspec, canonical_tag, tag_only)
        args: List[Tuple[str, str, bool]] = []
        for info in release_images:
            for pullspec in info.manifests_to_sign:
                args.append((pullspec, info.canonical_tag, tag_only))

        self._logger.info(
            "Signing %d release image manifests (from %d release images)%s",
            len(args),
            len(release_images),
            " [TAG ONLY]" if tag_only else "",
        )

        results = await run_limited_unordered(self._sign_manifest, args, self.concurrency_limit)
        return {pullspec: err for result in results for pullspec, err in result.items()}

    async def sign_component_images(
        self,
        component_images: Iterable[str],
    ) -> Dict[str, Exception]:
        """
        Sign component images with digest-based identity only.

        Component images don't need canonical tag signing since users reference them
        via the release image, not directly by tag.

        :param component_images: Pullspecs of component images to sign
        :return: Dict of any signing errors per pullspec
        """
        pullspecs = list(component_images)
        self._logger.info("Signing %d component images", len(pullspecs))

        args = [(ps, None) for ps in pullspecs]
        results = await run_limited_unordered(self._sign_manifest, args, self.concurrency_limit)
        return {pullspec: err for result in results for pullspec, err in result.items()}

    async def _sign_manifest(
        self,
        pullspec: str,
        canonical_tag: Optional[str] = None,
        tag_only: bool = False,
    ) -> Dict[str, Exception]:
        """
        Sign a manifest with cosign.

        When canonical_tag is provided, the manifest is signed with both:
        1. Digest-based identity (e.g., quay.io/.../ocp-release@sha256:...)
        2. Tag-based identity (e.g., quay.io/.../ocp-release:4.16.1-multi)

        When canonical_tag is None, only the digest-based identity is used.
        When tag_only is True, only the tag-based identity is used (skips digest signing).

        :param pullspec: Digest-based pullspec to sign
        :param canonical_tag: Optional canonical tag for additional tag-based identity
        :param tag_only: If True, only sign with tag identity (skip digest identity)
        :return: Dict with any signing error
        """
        log = self._logger

        # Build list of identities to sign with
        identities = []
        if not tag_only:
            identities.append(pullspec)
        if canonical_tag:
            repo = pullspec.split("@sha256:")[0] if "@sha256:" in pullspec else pullspec.rsplit(":", 1)[0]
            tag_identity = f"{repo}:{canonical_tag}"
            identities.append(tag_identity)

        if not identities:
            log.warning("No identities to sign for %s (tag_only=True but no canonical_tag)", pullspec)
            return {}

        log.info("Signing manifest %s with identities: %s", pullspec, identities)

        for identity in identities:
            for signing_key_id in self.signing_key_ids:
                cmd = [
                    "cosign",
                    "sign",
                    "--yes",
                    f"--sign-container-identity={identity}",
                    "--key",
                    f"awskms:///{signing_key_id}",
                ]

                if self.rekor_url:
                    cmd.append(f"--rekor-url={self.rekor_url}")
                else:
                    cmd.append("--tlog-upload=false")

                cmd.append(pullspec)

                if self.dry_run:
                    log.info("[DRY RUN] Would sign: %s", cmd)
                    continue

                log.debug("Running: %s", cmd)
                try:
                    stdout = await self._retrying_cosign(cmd)
                    log.debug("Signed %s (identity=%s): %s", pullspec, identity, stdout)
                    await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))
                except Exception as exc:
                    log.error("Failed signing %s (identity=%s): %s", pullspec, identity, exc)
                    return {pullspec: exc}

        return {}

    async def verify_legacy_signature(self, img_info: Dict) -> bool:
        """
        Verify the signature from mirror.openshift.com matches the release image and RH public key.

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

    @retry(wait=wait_random_exponential(), stop=stop_after_attempt(5), reraise=True)
    async def _retrying_cosign(self, cmd: List[str]) -> str:
        """Execute cosign with retry on failure."""
        await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False, env=self.ENV)
        if rc:
            raise RuntimeError(stderr)
        return stdout
