import asyncio
import base64
import io
import itertools
import json
import logging
import os
import shutil
import tarfile
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from random import uniform
from typing import BinaryIO, Dict, Iterable, List, Optional, Protocol, Set, Tuple, cast, runtime_checkable

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

# Signing key names used for production vs staging environments
PROD_SIGNING_KEY_NAME = "redhatrelease2"
STAGE_SIGNING_KEY_NAME = "beta2"

# Human identity attached to signing requests for audit purposes
ON_BEHALF_OF = "jupierce@redhat.com"


@runtime_checkable
class Signatory(Protocol):
    """Protocol defining the interface for artifact signing implementations.

    Any class implementing this protocol can sign OCP artifacts either via
    UMB (AsyncSignatory) or via the Konflux generic-signing pipeline (KonfluxSignatory).
    """

    async def sign_json_digest(
        self, product: str, release_name: str, pullspec: str, digest: str, sig_file: BinaryIO
    ) -> None:
        """Sign a JSON digest claim for a container image.

        :param product: Product name (e.g. "openshift")
        :param release_name: Release version (e.g. "4.16.1")
        :param pullspec: Image pullspec
        :param digest: Image digest (e.g. "sha256:...")
        :param sig_file: Writable binary file to receive the signature
        """
        ...

    async def sign_message_digest(
        self, product: str, release_name: str, artifact: BinaryIO, sig_file: BinaryIO
    ) -> None:
        """Sign a message digest (e.g. sha256sum.txt).

        :param product: Product name (e.g. "openshift")
        :param release_name: Release version (e.g. "4.16.1")
        :param artifact: Readable binary file containing the content to sign
        :param sig_file: Writable binary file to receive the signature
        """
        ...


def create_signatory(
    transport: str,
    signing_env: str,
    **kwargs,
) -> "AsyncSignatory | KonfluxSignatory":
    """Factory function to create the appropriate Signatory implementation.

    :param transport: Signing transport to use: "umb" for legacy UMB/RADAS,
        "konflux" for the Konflux generic-signing pipeline via InternalRequest.
    :param signing_env: Signing environment ("prod" or "stage"). Determines the
        signing key (redhatrelease2 for prod, beta2 for stage).
    :param kwargs: Additional keyword arguments passed to the signatory constructor.
        For "umb": uri, cert_file, key_file, requestor, subscription_name.
        For "konflux": namespace, kubeconfig, ta_registry, on_behalf_of,
            task_git_url, task_git_revision.
    :returns: A signatory instance implementing the Signatory protocol.
    :raises ValueError: If transport is not "umb" or "konflux".
    """
    sig_keyname = PROD_SIGNING_KEY_NAME if signing_env == "prod" else STAGE_SIGNING_KEY_NAME

    if transport == "umb":
        from pyartcd import constants

        uri = kwargs.pop("uri", None) or constants.UMB_BROKERS[signing_env]
        cert_file = kwargs.pop("cert_file", None) or os.environ.get("SIGNING_CERT", "")
        key_file = kwargs.pop("key_file", None) or os.environ.get("SIGNING_KEY", "")
        requestor = kwargs.pop("requestor", "artcd")
        subscription_name = kwargs.pop("subscription_name", "artcd")
        return AsyncSignatory(
            uri=uri,
            cert_file=cert_file,
            key_file=key_file,
            sig_keyname=sig_keyname,
            requestor=requestor,
            subscription_name=subscription_name,
        )

    if transport == "konflux":
        return KonfluxSignatory(
            sig_keyname=sig_keyname,
            **kwargs,
        )

    raise ValueError(f"Unknown signing transport: {transport!r}. Supported: 'umb', 'konflux'")


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
    # Preserve legacy signatures and explicit Rekor URLs with cosign 3.
    # Environment overrides also work with older versions that lack these flags.
    ENV["COSIGN_USE_SIGNING_CONFIG"] = "false"
    ENV["COSIGN_NEW_BUNDLE_FORMAT"] = "false"

    def __init__(
        self,
        logger,
        dry_run: bool,
        signing_creds: str,
        signing_key_ids: List[str],
        rekor_url: str,
        concurrency_limit: int,
        batch_retries: int = 3,
        batch_retry_delay: float = 30.0,
    ) -> None:
        self._logger = logger
        self.dry_run = dry_run  # if true, run discovery but do not sign anything
        self.signing_key_ids = signing_key_ids  # key ids for signing
        self.rekor_url = rekor_url  # rekor server for cosign tlog storage
        self.ENV["AWS_SHARED_CREDENTIALS_FILE"] = signing_creds  # filename for KMS credentials
        # cosign uses go-containerregistry which does hostname-only auth lookup.
        # QUAY_AUTH_FILE has path-scoped keys (e.g. "quay.io/openshift-release-dev")
        # so we must add hostname-only entries (e.g. "quay.io") for cosign to find them.
        quay_auth_file = os.environ.get("QUAY_AUTH_FILE")
        if quay_auth_file:
            docker_config_dir = tempfile.mkdtemp(prefix="cosign-docker-config-")
            config_json_path = os.path.join(docker_config_dir, "config.json")
            try:
                with open(quay_auth_file) as f:
                    auth_data = json.load(f)
                auths = auth_data.get("auths", {})
                # Add hostname-only entries for any path-scoped keys
                hostname_auths = {}
                for key, value in auths.items():
                    hostname = key.split("/")[0]
                    if hostname != key and hostname not in auths:
                        hostname_auths[hostname] = value
                if hostname_auths:
                    auths.update(hostname_auths)
                    auth_data["auths"] = auths
                    logger.info("Added hostname-only auth entries for cosign: %s", list(hostname_auths.keys()))
                with open(config_json_path, "w") as f:
                    json.dump(auth_data, f)
            except Exception as e:
                logger.warning("Could not rewrite auth file for cosign, falling back to copy: %s", e)
                shutil.copy2(quay_auth_file, config_json_path)
            self.ENV["DOCKER_CONFIG"] = docker_config_dir
            logger.info("Set DOCKER_CONFIG=%s for cosign (from QUAY_AUTH_FILE=%s)", docker_config_dir, quay_auth_file)
        else:
            logger.warning("QUAY_AUTH_FILE not set; cosign may lack registry credentials")
        self.concurrency_limit = concurrency_limit  # limit on concurrent lookups or signings
        self.batch_retries = batch_retries  # number of batch-level retries for failed images
        self.batch_retry_delay = batch_retry_delay  # seconds to wait between batch retries

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
        registry_config = os.environ.get("QUAY_AUTH_FILE")
        img_info = await get_image_info(pullspec, True, registry_config=registry_config)

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
        registry_config = os.environ.get("QUAY_AUTH_FILE")

        try:
            img_info = await get_image_info(pullspec, True, registry_config=registry_config)
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
        registry_config = os.environ.get("QUAY_AUTH_FILE")
        return set(
            tag["from"]["name"]
            for tag in (await get_release_image_info(pullspec, registry_config=registry_config))["references"]["spec"][
                "tags"
            ]
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

        Failed manifests are retried up to batch_retries times with a delay between attempts.

        :param release_images: List of ReleaseImageInfo with canonical tags and manifests to sign
        :param tag_only: If True, only sign with tag identity (skip digest identity).
            Useful for retroactive signing where digest signatures already exist.
        :return: Dict of any signing errors per pullspec
        """
        args: List[Tuple[str, str, bool]] = []
        for info in release_images:
            for pullspec in info.manifests_to_sign:
                args.append((pullspec, info.canonical_tag, tag_only))

        tag_label = " [TAG ONLY]" if tag_only else ""
        self._logger.info(
            f"Signing {len(args)} release image manifests (from {len(release_images)} release images){tag_label}"
        )

        errors: Dict[str, Exception] = {}
        for attempt in range(1, self.batch_retries + 1):
            results = await run_limited_unordered(self._sign_manifest, args, self.concurrency_limit)
            errors = {ps: err for result in results for ps, err in result.items()}
            if not errors:
                break
            failed = list(errors.keys())
            if attempt < self.batch_retries:
                self._logger.warning(
                    f"Batch attempt {attempt}/{self.batch_retries}: {len(errors)} manifests failed signing, "
                    f"retrying in {self.batch_retry_delay}s: {failed}"
                )
                args = [(ps, tag, to) for ps, tag, to in args if ps in errors]
                await asyncio.sleep(self.batch_retry_delay)
            else:
                self._logger.error(
                    f"Batch attempt {attempt}/{self.batch_retries}: {len(errors)} manifests still failing "
                    f"after all retries: {failed}"
                )
        return errors

    async def sign_component_images(
        self,
        component_images: Iterable[str],
    ) -> Dict[str, Exception]:
        """
        Sign component images with digest-based identity only.

        Component images don't need canonical tag signing since users reference them
        via the release image, not directly by tag.

        Failed images are retried up to batch_retries times with a delay between attempts.

        :param component_images: Pullspecs of component images to sign
        :return: Dict of any signing errors per pullspec
        """
        pullspecs = list(component_images)
        self._logger.info(f"Signing {len(pullspecs)} component images")

        args: List[Tuple[str, None]] = [(ps, None) for ps in pullspecs]
        errors: Dict[str, Exception] = {}
        for attempt in range(1, self.batch_retries + 1):
            results = await run_limited_unordered(self._sign_manifest, args, self.concurrency_limit)
            errors = {ps: err for result in results for ps, err in result.items()}
            if not errors:
                break
            failed = list(errors.keys())
            if attempt < self.batch_retries:
                self._logger.warning(
                    f"Batch attempt {attempt}/{self.batch_retries}: {len(errors)} images failed signing, "
                    f"retrying in {self.batch_retry_delay}s: {failed}"
                )
                args = [(ps, None) for ps in errors]
                await asyncio.sleep(self.batch_retry_delay)
            else:
                self._logger.error(
                    f"Batch attempt {attempt}/{self.batch_retries}: {len(errors)} images still failing "
                    f"after all retries: {failed}"
                )
        return errors

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
            log.warning(f"No identities to sign for {pullspec} (tag_only=True but no canonical_tag)")
            return {}

        log.info(f"Signing manifest {pullspec} with identities: {identities}")

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
                    log.info(f"[DRY RUN] Would sign: {cmd}")
                    continue

                log.debug(f"Running: {cmd}")
                try:
                    stdout = await self._retrying_cosign(cmd)
                    log.debug(f"Signed {pullspec} (identity={identity}): {stdout}")
                    await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))
                except Exception as exc:
                    detail = f"identity={identity}, key={signing_key_id}"
                    log.error(f"Failed signing {pullspec} ({detail}): {exc}")
                    return {pullspec: RuntimeError(f"{detail}: {exc}")}

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
        self._logger.debug("cosign ENV DOCKER_CONFIG=%s", self.ENV.get("DOCKER_CONFIG"))
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False, env=self.ENV)
        if rc:
            raise RuntimeError(stderr)
        return stdout


class KonfluxSignatory:
    """Sign OCP artifacts using the Konflux ``generic-signing`` pipeline.

    Instead of sending signing requests over UMB to RADAS (like
    :class:`AsyncSignatory`), this class packages content as a Trusted
    Artifact (OCI tarball), creates a Kubernetes ``InternalRequest`` custom
    resource that triggers the ``generic-signing`` pipeline on the Konflux
    common cluster, waits for completion, then extracts the resulting
    binary GPG signature from the output Trusted Artifact.

    Architecture overview (see ISV-7449 and
    https://gitlab.cee.redhat.com/signing/signing for full details):

    1. The file to sign is packaged into a tar.gz archive and pushed to an
       OCI registry as a Trusted Artifact using ``oras push``.
    2. An ``InternalRequest`` CR is created in the configured namespace with
       rate-limiting labels required by the signing infrastructure::

           internal-services.appstudio.openshift.io/rate-limited: "true"
           internal-services.appstudio.openshift.io/rate-limiting-group: signing-server

    3. The internal-services operator picks up the request and runs the
       ``generic-signing`` pipeline on the common cluster.
    4. On success the pipeline writes a result Trusted Artifact containing a
       ``signatures/`` directory and a ``results.json`` manifest.
    5. This class downloads the result TA, extracts the signature, and
       writes it to the caller-supplied file handle.

    The ``signMethod`` is always ``gpgsign`` (binary ``.gpg`` files) to
    match the existing mirror format used by ART.

    :param sig_keyname: Signing key name (e.g. ``redhatrelease2``, ``beta2``).
    :param namespace: Kubernetes namespace for InternalRequest CRs.
        Defaults to ``KONFLUX_SIGNING_NAMESPACE`` env var or
        ``art-release--signing``.
    :param kubeconfig: Path to kubeconfig file. ``None`` uses in-cluster
        config or the ``KUBECONFIG`` env var.
    :param ta_registry: OCI registry for Trusted Artifact storage.
        Defaults to ``KONFLUX_TA_REGISTRY`` env var or
        ``quay.io/konflux-ci/release-service-trusted-artifacts``.
    :param on_behalf_of: Human identity for audit trail. Defaults to
        ``ON_BEHALF_OF`` (``jupierce@redhat.com``).
    :param task_git_url: Git URL of the signing repo. Defaults to
        ``https://gitlab.cee.redhat.com/signing/signing.git``.
    :param task_git_revision: Git revision of the signing repo. Defaults
        to ``main``.
    :param poll_interval: Seconds between InternalRequest status polls.
    :param timeout: Maximum seconds to wait for a signing request.
    """

    # InternalRequest CRD coordinates
    IR_GROUP = "appstudio.redhat.com"
    IR_VERSION = "v1alpha1"
    IR_PLURAL = "internalrequests"

    # Rate-limiting labels required by the signing infrastructure
    RATE_LIMIT_LABELS = {
        "internal-services.appstudio.openshift.io/rate-limited": "true",
        "internal-services.appstudio.openshift.io/rate-limiting-group": "signing-server",
    }

    # Default signing pipeline reference
    DEFAULT_PIPELINE = "generic-signing"
    DEFAULT_TASK_GIT_URL = "https://gitlab.cee.redhat.com/signing/signing.git"
    DEFAULT_TASK_GIT_REVISION = "main"

    def __init__(
        self,
        sig_keyname: str,
        namespace: Optional[str] = None,
        kubeconfig: Optional[str] = None,
        ta_registry: Optional[str] = None,
        on_behalf_of: Optional[str] = None,
        task_git_url: Optional[str] = None,
        task_git_revision: Optional[str] = None,
        poll_interval: float = 10.0,
        timeout: float = 600.0,
    ):
        self.sig_keyname = sig_keyname
        self.namespace = namespace or os.environ.get("KONFLUX_SIGNING_NAMESPACE", "art-release--signing")
        self.kubeconfig = kubeconfig
        self.ta_registry = ta_registry or os.environ.get(
            "KONFLUX_TA_REGISTRY",
            "quay.io/konflux-ci/release-service-trusted-artifacts",
        )
        self.on_behalf_of = on_behalf_of or ON_BEHALF_OF
        self.task_git_url = task_git_url or self.DEFAULT_TASK_GIT_URL
        self.task_git_revision = task_git_revision or self.DEFAULT_TASK_GIT_REVISION
        self.poll_interval = poll_interval
        self.timeout = timeout

        self._api = None  # Lazily initialized kubernetes.client.CustomObjectsApi

    # -- Context manager support -----------------------------------------------

    async def __aenter__(self):
        self._ensure_k8s_client()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    # -- Public signing interface (Signatory protocol) -------------------------

    async def sign_json_digest(
        self,
        product: str,
        release_name: str,
        pullspec: str,
        digest: str,
        sig_file: BinaryIO,
    ) -> None:
        """Sign a JSON digest claim via the Konflux generic-signing pipeline.

        Constructs the same JSON claim format as :class:`AsyncSignatory`,
        packages it as a Trusted Artifact, and submits it for signing.
        """
        json_claim = {
            "critical": {
                "image": {"docker-manifest-digest": digest},
                "type": "atomic container signature",
                "identity": {"docker-reference": pullspec},
            },
            "optional": {
                "creator": "Red Hat OpenShift Signing Authority 0.0.1",
            },
        }
        content = json.dumps(json_claim).encode()
        filename = f"{digest.replace(':', '=')}.json"

        await self._sign_content(content, filename, sig_file)

    async def sign_message_digest(
        self,
        product: str,
        release_name: str,
        artifact: BinaryIO,
        sig_file: BinaryIO,
    ) -> None:
        """Sign a message digest file via the Konflux generic-signing pipeline.

        Reads the artifact content and submits it for GPG signing.
        """
        content = artifact.read()
        filename = "sha256sum.txt"

        await self._sign_content(content, filename, sig_file)

    # -- Internal implementation -----------------------------------------------

    def _ensure_k8s_client(self):
        """Lazily initialise the Kubernetes API client."""
        if self._api is not None:
            return
        from kubernetes import client
        from kubernetes import config as k8s_config

        if self.kubeconfig:
            k8s_config.load_kube_config(config_file=self.kubeconfig)
        else:
            try:
                k8s_config.load_incluster_config()
            except k8s_config.ConfigException:
                k8s_config.load_kube_config()
        self._api = client.CustomObjectsApi()

    async def _sign_content(
        self,
        content: bytes,
        filename: str,
        sig_file: BinaryIO,
    ) -> None:
        """Core signing flow: package → push TA → create IR → poll → extract.

        :param content: Raw bytes to sign.
        :param filename: Filename inside the Trusted Artifact archive.
        :param sig_file: Writable binary file to receive the binary GPG signature.
        """
        self._ensure_k8s_client()
        request_id = f"art-sign-{uuid.uuid4().hex[:12]}"

        with tempfile.TemporaryDirectory(prefix="konflux-sign-") as tmpdir:
            # 1. Create the input archive for the Trusted Artifact
            input_dir = os.path.join(tmpdir, "input")
            os.makedirs(input_dir)
            input_file = os.path.join(input_dir, filename)
            with open(input_file, "wb") as f:
                f.write(content)

            # 2. Push the input as a Trusted Artifact via oras
            ta_uri = await self._push_trusted_artifact(input_dir, request_id)

            # 3. Create the InternalRequest CR
            ir_name = await self._create_internal_request(request_id, ta_uri)

            # 4. Wait for completion and get the result TA URI
            result_ta_uri = await self._wait_for_completion(ir_name)

            # 5. Pull the result TA and extract the signature
            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)
            await self._pull_trusted_artifact(result_ta_uri, output_dir)
            await self._extract_signature(output_dir, filename, sig_file)

    async def _push_trusted_artifact(self, input_dir: str, request_id: str) -> str:
        """Package the input directory and push it as a Trusted Artifact.

        Uses ``oras push`` to upload a tar.gz archive to the OCI registry.

        :param input_dir: Directory containing the file(s) to sign.
        :param request_id: Unique request ID used as the tag.
        :returns: The OCI URI of the pushed artifact (``oci:<ref>``).
        """
        tag = f"art-input-{request_id}"
        ref = f"{self.ta_registry}:{tag}"

        # Create a tar.gz from the input directory
        archive_path = os.path.join(os.path.dirname(input_dir), "input.tar.gz")
        with tarfile.open(archive_path, "w:gz") as tar:
            for entry in os.listdir(input_dir):
                tar.add(os.path.join(input_dir, entry), arcname=entry)

        cmd = [
            "oras",
            "push",
            ref,
            f"{archive_path}:application/vnd.oci.image.layer.v1.tar+gzip",
        ]
        _LOGGER.info("Pushing Trusted Artifact to %s", ref)
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)
        if rc:
            raise SignatoryServerError(f"Failed to push Trusted Artifact: {stderr}")

        return f"oci:{ref}"

    async def _create_internal_request(self, request_id: str, source_data_artifact: str) -> str:
        """Create an InternalRequest CR to trigger the generic-signing pipeline.

        :param request_id: Unique identifier for this signing request.
        :param source_data_artifact: OCI URI of the input Trusted Artifact.
        :returns: The name of the created InternalRequest resource.
        """
        ir_name = request_id
        body = {
            "apiVersion": f"{self.IR_GROUP}/{self.IR_VERSION}",
            "kind": "InternalRequest",
            "metadata": {
                "name": ir_name,
                "namespace": self.namespace,
                "labels": {
                    **self.RATE_LIMIT_LABELS,
                    "art.openshift.io/signing-request": "true",
                },
            },
            "spec": {
                "request": self.DEFAULT_PIPELINE,
                "params": {
                    "sourceDataArtifact": source_data_artifact,
                    "keyname": self.sig_keyname,
                    "signMethod": "gpgsign",
                    "onbehalfof": self.on_behalf_of,
                    "taskGitUrl": self.task_git_url,
                    "taskGitRevision": self.task_git_revision,
                },
            },
        }

        _LOGGER.info(
            "Creating InternalRequest %s in namespace %s (key=%s)",
            ir_name,
            self.namespace,
            self.sig_keyname,
        )
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._api.create_namespaced_custom_object(
                group=self.IR_GROUP,
                version=self.IR_VERSION,
                namespace=self.namespace,
                plural=self.IR_PLURAL,
                body=body,
            ),
        )
        return ir_name

    async def _wait_for_completion(self, ir_name: str) -> str:
        """Poll the InternalRequest until it completes or times out.

        :param ir_name: Name of the InternalRequest to monitor.
        :returns: OCI URI of the result Trusted Artifact.
        :raises SignatoryServerError: If the request fails or times out.
        """
        deadline = asyncio.get_event_loop().time() + self.timeout

        while True:
            ir = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._api.get_namespaced_custom_object(
                    group=self.IR_GROUP,
                    version=self.IR_VERSION,
                    namespace=self.namespace,
                    plural=self.IR_PLURAL,
                    name=ir_name,
                ),
            )

            status = ir.get("status", {})
            conditions = status.get("conditions", [])

            for condition in conditions:
                if condition.get("type") == "Succeeded":
                    cond_status = condition.get("status", "")
                    if cond_status == "True":
                        result_uri = status.get("results", {}).get("sourceDataArtifact", "")
                        if not result_uri:
                            raise SignatoryServerError(
                                f"InternalRequest {ir_name} succeeded but has no sourceDataArtifact result"
                            )
                        _LOGGER.info(
                            "InternalRequest %s completed successfully, result: %s",
                            ir_name,
                            result_uri,
                        )
                        return result_uri
                    if cond_status == "False":
                        reason = condition.get("reason", "Unknown")
                        message = condition.get("message", "")
                        raise SignatoryServerError(f"InternalRequest {ir_name} failed: {reason} - {message}")
                    # status == "Unknown" → still running

            if asyncio.get_event_loop().time() > deadline:
                raise SignatoryServerError(f"InternalRequest {ir_name} timed out after {self.timeout}s")

            _LOGGER.debug("InternalRequest %s still running, polling in %ss", ir_name, self.poll_interval)
            await asyncio.sleep(self.poll_interval)

    async def _pull_trusted_artifact(self, ta_uri: str, output_dir: str) -> None:
        """Download and extract a Trusted Artifact.

        :param ta_uri: OCI URI of the Trusted Artifact (``oci:<ref>``).
        :param output_dir: Directory to extract the artifact contents into.
        """
        # Strip "oci:" prefix if present
        ref = ta_uri.removeprefix("oci:")

        cmd = ["oras", "pull", ref, "--output", output_dir]
        _LOGGER.info("Pulling result Trusted Artifact from %s", ref)
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)
        if rc:
            raise SignatoryServerError(f"Failed to pull result Trusted Artifact: {stderr}")

        # If a tar.gz was pulled, extract it
        for entry in os.listdir(output_dir):
            path = os.path.join(output_dir, entry)
            if entry.endswith(".tar.gz") or entry.endswith(".tgz"):
                with tarfile.open(path, "r:gz") as tar:
                    tar.extractall(path=output_dir)
                os.remove(path)

    async def _extract_signature(
        self,
        output_dir: str,
        original_filename: str,
        sig_file: BinaryIO,
    ) -> None:
        """Extract the binary GPG signature from the signing pipeline results.

        The output TA contains a ``results.json`` with pointers into a
        ``signatures/`` directory. This method reads the results, finds the
        signature for the original input file, and writes it to *sig_file*.

        :param output_dir: Directory containing the extracted result TA.
        :param original_filename: Name of the original input file.
        :param sig_file: Writable binary file to receive the signature.
        :raises SignatoryServerError: If results.json is missing or the
            signature for the input file cannot be found.
        """
        results_path = os.path.join(output_dir, "results.json")
        if not os.path.exists(results_path):
            raise SignatoryServerError(f"results.json not found in signing output (contents: {os.listdir(output_dir)})")

        with open(results_path) as f:
            results = json.load(f)

        # Find the signature for our input file
        for entry in results.get("results", []):
            if entry.get("file", "").endswith(original_filename):
                sig_path = os.path.join(output_dir, entry["signature_file"])
                if not os.path.exists(sig_path):
                    raise SignatoryServerError(
                        f"Signature file {entry['signature_file']} referenced in results.json not found"
                    )
                with open(sig_path, "rb") as sf:
                    sig_file.write(sf.read())
                _LOGGER.info(
                    "Extracted signature for %s (key: %s)",
                    original_filename,
                    entry.get("key", "unknown"),
                )
                return

        raise SignatoryServerError(f"No signature found for {original_filename} in results.json: {results}")
