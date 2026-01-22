import asyncio
import base64
import io
import itertools
import json
import logging
import os
import uuid
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
        sign_release: bool,
        sign_components: bool,
        verify_release: bool,
    ) -> None:
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
        """form the pullspec for a digest in the same repo as an existing pullspec"""
        if len(halves := pullspec.split("@sha256:")) == 2:  # assume that was a digest at the end
            return f"{halves[0]}@{digest}"
        elif len(halves := pullspec.rsplit(":", 1)) == 2:
            # assume that was a tag at the end, while allowing for ":" in the registry spec
            return f"{halves[0]}@{digest}"
        return f"{pullspec}@{digest}"  # assume it was a bare registry/repo

    async def discover_pullspecs(
        self, pullspecs: Iterable[str], release_name: str
    ) -> Tuple[Dict[str, List[str]], Dict[str, Exception]]:
        """
        Recursively discover pullspecs that need signatures. Given manifest lists, examine the
        digests of each platform. Given a release image, examine the digests of all payload
        components. Come up with a list of the individual manifests we will actually sign.

        :param pullspecs: List of pullspecs to begin discovery
        :param release_name: Require any release images to have this release name
        :return:
            - Dict mapping each original input pullspec to the list of pullspecs discovered from it
              that need signing. For manifest lists, this maps to the individual arch manifests.
              For single manifests, this maps to [itself] plus any component images.
            - Dict of any discovery errors
        """
        original_pullspecs = list(pullspecs)
        seen: Set[str] = set(original_pullspecs)  # prevent re-examination and multiple signings
        errors: Dict[str, Exception] = {}  # pullspec -> error when examining it

        # Track which original input each discovered pullspec came from
        # This allows canonical tags to be propagated from manifest lists to their members
        origin_map: Dict[str, str] = {}  # discovered pullspec -> original input it came from
        for ps in original_pullspecs:
            origin_map[ps] = ps  # initially, each input maps to itself

        need_signing: Set[str] = set()  # pullspecs for manifests to be signed
        need_examining: List[str] = original_pullspecs.copy()

        while need_examining:
            args = [(ps, release_name) for ps in need_examining]
            results = await run_limited_unordered(self._examine_pullspec, args, self.concurrency_limit)

            next_to_examine = []
            for idx, (next_signing, next_examining, next_errors) in enumerate(results):
                current_ps = args[idx][0]
                current_origin = origin_map.get(current_ps, current_ps)

                need_signing.update(next_signing)
                errors.update(next_errors)

                # For newly discovered pullspecs, track their origin
                for ps in next_signing:
                    if ps not in origin_map:
                        origin_map[ps] = current_origin

                for ps in next_examining:
                    if ps not in seen:
                        seen.add(ps)
                        next_to_examine.append(ps)
                        # Inherit the origin from the parent
                        origin_map[ps] = current_origin

            need_examining = next_to_examine

        # Build the result: original input -> list of discovered pullspecs
        result: Dict[str, List[str]] = {ps: [] for ps in original_pullspecs}
        for ps in need_signing:
            origin = origin_map.get(ps)
            if origin and origin in result:
                result[origin].append(ps)
            else:
                # Shouldn't happen, but handle gracefully
                self._logger.warning("Pullspec %s has unknown origin, adding to first input", ps)
                if original_pullspecs:
                    result[original_pullspecs[0]].append(ps)

        return result, errors

    async def _examine_pullspec(self, pullspec: str, release_name: str) -> (Set[str], Set[str], Dict[str, Exception]):
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
        elif this_rn := img_info["config"]["config"]["Labels"].get("io.openshift.release"):
            # release image; get references and examine those
            self._logger.info("%s is a release image with name %s", pullspec, this_rn)
            if release_name != this_rn:
                errors[pullspec] = RuntimeError(
                    f"release image at {pullspec} has release name {this_rn}, not the expected {release_name}",
                )
            elif self.verify_release and not await self.verify_legacy_signature(img_info):
                errors[pullspec] = RuntimeError(
                    f"release image at {pullspec} does not have a required legacy signature",
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
        """Retrieve the pullspecs referenced by a release image"""
        return set(
            tag["from"]["name"] for tag in (await get_release_image_info(pullspec))["references"]["spec"]["tags"]
        )

    async def sign_pullspecs(
        self,
        discovered: Dict[str, List[str]],
        canonical_tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Exception]:
        """
        Sign the discovered pullspecs via cosign with our KMS.

        :param discovered: Dict mapping original input pullspecs to lists of discovered pullspecs
            that need signing. This structure preserves the origin of each discovered pullspec.
        :param canonical_tags: Optional mapping of original input pullspec -> canonical tag.
            The canonical tag from the original is applied to ALL pullspecs discovered from it.
            This is important for manifest lists: when a user pulls "ocp-release:4.16.1-multi",
            the registry returns an arch-specific manifest. The signature for that manifest must
            have identity "ocp-release:4.16.1-multi" (the tag the user requested), not an
            arch-specific tag. This enables tag-based signature verification without
            signedIdentity overrides.
        :return: dict with any signing errors per pullspec
        """
        tags = canonical_tags or {}

        # Build args list: for each discovered pullspec, use the canonical tag from its origin.
        # This ensures manifest list members are signed with the manifest list's tag.
        args: List[Tuple[str, Optional[str]]] = []
        for original, to_sign_list in discovered.items():
            canonical_tag = tags.get(original)
            for pullspec in to_sign_list:
                args.append((pullspec, canonical_tag))

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

    async def _sign_single_manifest(
        self,
        pullspec: str,
        canonical_tag: Optional[str] = None,
    ) -> Dict[str, Exception]:
        """
        Use sigstore to sign a single image manifest, with one or more signing keys, and upload the signature.

        When a canonical_tag is provided and the pullspec is a release image (in the ocp-release repo),
        the image will be signed twice: once with the digest-based identity (pullspec) and once with
        the tag-based identity. This enables customers to verify images referenced by tag without
        needing signedIdentity overrides.

        :param pullspec: Digest-based pullspec to be signed (e.g., "quay.io/.../ocp-release@sha256:...")
        :param canonical_tag: Optional canonical tag for the release image (e.g., "4.16.1-x86_64").
            When provided and pullspec is a release image, an additional signature with the
            tag-based identity will be created.
        :return: dict with any signing errors for pullspec
        """
        log = self._logger

        # Build list of identities to sign with
        # Always sign with the digest-based identity first
        identities_to_sign: List[str] = [pullspec]

        # If we have a canonical tag and this is a release image (not a component image),
        # also sign with tag identity. This enables customers to verify images referenced
        # by tag without needing signedIdentity overrides.
        # Release images are in ocp-release repo; component images are in ocp-v4.0-art-dev.
        if canonical_tag and "/ocp-release" in pullspec:
            # Extract the repo from the pullspec (everything before @sha256:)
            repo = pullspec.split("@sha256:")[0] if "@sha256:" in pullspec else pullspec.rsplit(":", 1)[0]
            tag_identity = f"{repo}:{canonical_tag}"
            identities_to_sign.append(tag_identity)
            log.info("Will also sign with canonical tag identity: %s", tag_identity)

        for identity in identities_to_sign:
            for signing_key_id in self.signing_key_ids:
                cmd = [
                    "cosign",
                    "sign",
                    "--yes",
                    # https://issues.redhat.com/browse/ART-10052
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
                    log.info("[DRY RUN] Would have signed image: %s", cmd)
                    continue

                log.info("Signing %s (identity=%s) with %s...", pullspec, identity, signing_key_id)
                try:
                    stdout = await self._retrying_sign_single_manifest(cmd)
                    log.debug("Successfully signed %s (identity=%s) with %s:\n%s", pullspec, identity, signing_key_id, stdout)
                    await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))  # introduce jitter to avoid rate limits
                except Exception as exc:
                    log.error("Failure signing %s (identity=%s) with %s:\n%s", pullspec, identity, signing_key_id, exc)
                    return {pullspec: exc}

        return {}

    @retry(wait=wait_random_exponential(), stop=stop_after_attempt(5), reraise=True)
    async def _retrying_sign_single_manifest(self, cmd: List[str]) -> str:
        await asyncio.sleep(uniform(0, self.THROTTLE_DELAY))  # introduce jitter to avoid rate limits
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False, env=self.ENV)
        if rc:
            raise RuntimeError(stderr)
        return stdout
