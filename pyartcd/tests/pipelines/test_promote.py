import os
import shutil
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.exceptions import VerificationError
from artcommonlib.model import Model

from pyartcd.pipelines.promote import PromotePipeline


class TestPromotePipeline(IsolatedAsyncioTestCase):
    FAKE_DEST_MANIFEST_LIST = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "manifests": [
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 1583,
                "digest": "fake:deadbeef-dest-multi-amd64",
                "platform": {
                    "architecture": "amd64",
                    "os": "linux",
                },
            },
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 1583,
                "digest": "fake:deadbeef-dest-multi-ppc64le",
                "platform": {
                    "architecture": "ppc64le",
                    "os": "linux",
                },
            },
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 1583,
                "digest": "fake:deadbeef-dest-multi-s390x",
                "platform": {
                    "architecture": "s390x",
                    "os": "linux",
                },
            },
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 1583,
                "digest": "fake:deadbeef-dest-multi-aarch64",
                "platform": {
                    "architecture": "arm64",
                    "os": "linux",
                },
            },
        ],
    }
    FAKE_SOURCE_MANIFEST_LIST = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "manifests": [
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 1583,
                "digest": "fake:deadbeef-source-multi-amd64",
                "platform": {
                    "architecture": "amd64",
                    "os": "linux",
                },
            },
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 1583,
                "digest": "fake:deadbeef-source-multi-ppc64le",
                "platform": {
                    "architecture": "ppc64le",
                    "os": "linux",
                },
            },
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 1583,
                "digest": "fake:deadbeef-source-multi-s390x",
                "platform": {
                    "architecture": "s390x",
                    "os": "linux",
                },
            },
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 1583,
                "digest": "fake:deadbeef-source-multi-arm64",
                "platform": {
                    "architecture": "arm64",
                    "os": "linux",
                },
            },
        ],
    }

    def setUp(self) -> None:
        os.environ.update(
            {
                "GITHUB_TOKEN": "fake-github-token",
                "JIRA_TOKEN": "fake-jira-token",
                "QUAY_PASSWORD": "fake-quay-password",
                "SIGNING_CERT": "/path/to/signing.crt",
                "SIGNING_KEY": "/path/to/signing.key",
                "REDIS_SERVER_PASSWORD": "fake-redis-server-password",
                "JENKINS_SERVICE_ACCOUNT": "fake-jenkins-service-account",
                "JENKINS_SERVICE_ACCOUNT_TOKEN": "fake-jenkins-service-account-token",
                "AWS_SHARED_CREDENTIALS_FILE": "/path/to/credentials/file",
                "CLOUDFLARE_ENDPOINT": "fake-cloudflare-endpoint",
                "ART_CLUSTER_ART_CD_PIPELINE_KUBECONFIG": "/path/to/kube/config",
            }
        )

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch("pyartcd.pipelines.promote.util.load_releases_config", return_value={})
    @patch("pyartcd.pipelines.promote.util.load_group_config", return_value=dict(arches=["x86_64", "s390x"]))
    async def test_run_without_explicit_assembly_definition(
        self, load_group_config: AsyncMock, load_releases_config: AsyncMock, _
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        pipeline = await PromotePipeline.create(
            runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod", skip_sigstore=True
        )
        with self.assertRaisesRegex(ValueError, "must be explicitly defined"):
            await pipeline.run()
        load_group_config.assert_awaited_once()
        load_releases_config.assert_awaited_once_with(
            group='openshift-4.10', data_path='https://example.com/ocp-build-data.git'
        )

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.util.load_releases_config",
        return_value={
            "releases": {"stream": {"assembly": {"type": "stream"}}},
        },
    )
    @patch("pyartcd.pipelines.promote.util.load_group_config", return_value=dict(arches=["x86_64", "s390x"]))
    async def test_run_with_stream_assembly(self, load_group_config: AsyncMock, load_releases_config: AsyncMock, _):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        pipeline = await PromotePipeline.create(
            runtime, group="openshift-4.10", assembly="stream", signing_env="prod", skip_sigstore=True
        )
        with self.assertRaisesRegex(ValueError, "not supported"):
            await pipeline.run()
        load_group_config.assert_awaited_once()
        load_releases_config.assert_awaited_once_with(
            group='openshift-4.10', data_path='https://example.com/ocp-build-data.git'
        )

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.util.load_releases_config",
        return_value={
            "releases": {"art0001": {"assembly": {"type": "custom", "basis": {}}}},
        },
    )
    @patch("pyartcd.pipelines.promote.util.load_group_config", return_value=dict(arches=["x86_64", "s390x"]))
    async def test_run_with_custom_assembly_and_missing_release_offset(
        self, load_group_config: AsyncMock, load_releases_config: AsyncMock, _
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
            new_slack_client=MagicMock(return_value=AsyncMock()),
        )
        pipeline = await PromotePipeline.create(
            runtime, group="openshift-4.10", assembly="art0001", signing_env="prod", skip_sigstore=True
        )
        with self.assertRaisesRegex(ValueError, "patch_version is not set"):
            await pipeline.run()
        load_group_config.assert_awaited_once()
        load_releases_config.assert_awaited_once_with(
            group='openshift-4.10', data_path='https://example.com/ocp-build-data.git'
        )

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch("pyartcd.pipelines.promote.PromotePipeline.build_release_image", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.get_release_image_info",
        side_effect=lambda pullspec, raise_if_not_found=False: {
            "image": pullspec,
            "digest": f"fake:deadbeef-{pullspec}",
            "metadata": {
                "version": "4.10.99-assembly.art0001",
            },
            "references": {
                "spec": {
                    "tags": [
                        {
                            "name": "machine-os-content",
                            "annotations": {"io.openshift.build.versions": "machine-os=00.00.212301010000-0"},
                        },
                    ],
                },
            },
        }
        if raise_if_not_found
        else None,
    )
    @patch(
        "pyartcd.pipelines.promote.util.load_releases_config",
        return_value={
            "releases": {"art0001": {"assembly": {"type": "custom", "basis": {"patch_version": 99}}}},
        },
    )
    @patch("pyartcd.pipelines.promote.util.load_group_config", return_value=Model(dict(arches=["x86_64", "s390x"])))
    @patch("pyartcd.pipelines.promote.PromotePipeline.get_image_stream")
    @patch("pyartcd.pipelines.promote.PromotePipeline.send_promote_complete_email")
    @patch("pyartcd.pipelines.promote.PromotePipeline.sign_artifacts")
    async def test_run_with_custom_assembly(
        self,
        sign_artifacts: Mock,
        send_promote_complete_email: Mock,
        get_image_stream: AsyncMock,
        load_group_config: AsyncMock,
        load_releases_config: AsyncMock,
        get_release_image_info: AsyncMock,
        build_release_image: AsyncMock,
        _,
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        runtime.new_slack_client.return_value = AsyncMock()
        runtime.new_slack_client.return_value.say.return_value = {'message': {'ts': ''}}
        runtime.new_slack_client.return_value.bind_channel = MagicMock()

        pipeline = await PromotePipeline.create(
            runtime,
            group="openshift-4.10",
            assembly="art0001",
            skip_attached_bug_check=True,
            skip_mirror_binaries=True,
            signing_env="prod",
            skip_sigstore=True,
        )

        await pipeline.run()
        load_group_config.assert_awaited_once()
        load_releases_config.assert_awaited_once_with(
            group='openshift-4.10', data_path='https://example.com/ocp-build-data.git'
        )
        get_release_image_info.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-assembly.art0001-x86_64", raise_if_not_found=ANY
        )
        get_release_image_info.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-assembly.art0001-s390x", raise_if_not_found=ANY
        )
        build_release_image.assert_any_await(
            "4.10.99-assembly.art0001",
            "x86_64",
            [],
            [],
            {},
            "quay.io/openshift-release-dev/ocp-release:4.10.99-assembly.art0001-x86_64",
            None,
            '4.10-art-assembly-art0001',
            keep_manifest_list=False,
        )
        build_release_image.assert_any_await(
            "4.10.99-assembly.art0001",
            "s390x",
            [],
            [],
            {},
            "quay.io/openshift-release-dev/ocp-release:4.10.99-assembly.art0001-s390x",
            None,
            '4.10-art-assembly-art0001-s390x',
            keep_manifest_list=False,
        )
        pipeline._slack_client.bind_channel.assert_called_once_with("4.10.99-assembly.art0001")

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.util.load_releases_config",
        return_value={
            "releases": {"4.10.99": {"assembly": {"type": "standard"}}},
        },
    )
    @patch("pyartcd.pipelines.promote.util.load_group_config", return_value=Model(dict(arches=["x86_64", "s390x"])))
    async def test_run_with_standard_assembly_without_upgrade_edges(
        self, load_group_config: AsyncMock, load_releases_config: AsyncMock, _
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        runtime.new_slack_client.return_value = AsyncMock()
        runtime.new_slack_client.return_value.say.return_value = {'message': {'ts': ''}}
        runtime.new_slack_client.return_value.bind_channel = MagicMock()
        pipeline = await PromotePipeline.create(
            runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod", skip_sigstore=True
        )

        with self.assertRaisesRegex(ValueError, "missing the required `upgrades` field"):
            await pipeline.run()
        load_group_config.assert_awaited_once()
        load_releases_config.assert_awaited_once_with(
            group='openshift-4.10', data_path='https://example.com/ocp-build-data.git'
        )

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.util.load_releases_config",
        return_value={
            "releases": {"4.10.99": {"assembly": {"type": "standard"}}},
        },
    )
    @patch(
        "pyartcd.pipelines.promote.util.load_group_config",
        return_value=Model(dict(arches=["x86_64", "s390x"], upgrades="4.10.98,4.9.99")),
    )
    async def test_run_with_standard_assembly_without_image_advisory(
        self, load_group_config: AsyncMock, load_releases_config: AsyncMock, _
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        runtime.new_slack_client.return_value = AsyncMock()
        runtime.new_slack_client.return_value.say.return_value = {'message': {'ts': ''}}
        runtime.new_slack_client.return_value.bind_channel = MagicMock()

        pipeline = await PromotePipeline.create(
            runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod", skip_sigstore=True
        )
        pipeline.check_blocker_bugs = AsyncMock()

        with self.assertRaisesRegex(VerificationError, "No associated image advisory"):
            await pipeline.run()
        load_group_config.assert_awaited_once()
        load_releases_config.assert_awaited_once_with(
            group='openshift-4.10', data_path='https://example.com/ocp-build-data.git'
        )

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.util.load_releases_config",
        return_value={
            "releases": {"4.10.99": {"assembly": {"type": "standard"}}},
        },
    )
    @patch(
        "pyartcd.pipelines.promote.util.load_group_config",
        return_value=Model(dict(arches=["x86_64", "s390x"], upgrades="4.10.98,4.9.99", advisories={"image": 2})),
    )
    async def test_run_with_standard_assembly_without_liveid(
        self, load_group_config: AsyncMock, load_releases_config: AsyncMock, _
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        runtime.new_slack_client.return_value = AsyncMock()
        runtime.new_slack_client.return_value.say.return_value = {'message': {'ts': ''}}
        runtime.new_slack_client.return_value.bind_channel = MagicMock()

        pipeline = await PromotePipeline.create(
            runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod", skip_sigstore=True
        )
        pipeline.check_blocker_bugs = AsyncMock()
        pipeline.change_advisory_state_qe = AsyncMock()
        pipeline.get_advisory_info = AsyncMock(
            return_value={
                "id": 2,
                "errata_id": 2,
                "status": "QE",
            }
        )

        with self.assertRaisesRegex(VerificationError, "doesn't have a live ID"):
            await pipeline.run()
        load_group_config.assert_awaited_once()
        load_releases_config.assert_awaited_once_with(
            group='openshift-4.10', data_path='https://example.com/ocp-build-data.git'
        )

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.util.load_releases_config",
        return_value={
            "releases": {"4.10.99": {"assembly": {"type": "standard"}}},
        },
    )
    @patch(
        "pyartcd.pipelines.promote.util.load_group_config",
        return_value=Model(dict(arches=["x86_64", "s390x"], upgrades="4.10.98,4.9.99", advisories={"image": 2})),
    )
    async def test_run_with_standard_assembly_invalid_errata_status(
        self, load_group_config: AsyncMock, load_releases_config: AsyncMock, _
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        runtime.new_slack_client.return_value = AsyncMock()
        runtime.new_slack_client.return_value.say.return_value = {'message': {'ts': ''}}
        runtime.new_slack_client.return_value.bind_channel = MagicMock()

        pipeline = await PromotePipeline.create(
            runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod", skip_sigstore=True
        )
        pipeline.check_blocker_bugs = AsyncMock()
        pipeline.change_advisory_state_qe = AsyncMock()
        pipeline.get_advisory_info = AsyncMock(
            return_value={"id": 2, "errata_id": 2222, "fulladvisory": "RHBA-2099:2222-02", "status": "NEW_FILES"}
        )

        with self.assertRaisesRegex(VerificationError, "should not be in NEW_FILES state"):
            await pipeline.run()
        load_group_config.assert_awaited_once()
        load_releases_config.assert_awaited_once_with(
            group='openshift-4.10', data_path='https://example.com/ocp-build-data.git'
        )

    @patch("pyartcd.locks.run_with_lock", new_callable=MagicMock)
    @patch("pyartcd.pipelines.promote.PromotePipeline.sign_artifacts")
    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch("pyartcd.jenkins.start_cincinnati_prs")
    @patch("pyartcd.pipelines.promote.PromotePipeline.build_release_image", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.get_release_image_info",
        side_effect=lambda pullspec, raise_if_not_found=False: {
            "image": pullspec,
            "digest": f"fake:deadbeef-{pullspec}",
            "metadata": {
                "version": "4.10.99",
            },
            "references": {
                "spec": {
                    "tags": [
                        {
                            "name": "machine-os-content",
                            "annotations": {"io.openshift.build.versions": "machine-os=00.00.212301010000-0"},
                        },
                    ],
                },
                "metadata": {
                    "annotations": {
                        "release.openshift.io/from-release": 'registry.ci.openshift.org/ocp/release:nightly'
                    }
                },
            },
        }
        if raise_if_not_found
        else None,
    )
    @patch(
        "pyartcd.pipelines.promote.util.load_releases_config",
        return_value={
            "releases": {"4.10.99": {"assembly": {"type": "standard"}}},
        },
    )
    @patch(
        "pyartcd.pipelines.promote.util.load_group_config",
        return_value=Model(
            {
                "upgrades": "4.10.98,4.9.99",
                "upgrades_next": "4.11.45",
                "advisories": {"rpm": 1, "image": 2, "extras": 3, "metadata": 4},
                "description": "whatever",
                "arches": ["x86_64", "s390x", "ppc64le", "aarch64"],
            }
        ),
    )
    @patch("pyartcd.pipelines.promote.PromotePipeline.get_image_stream")
    @patch("pyartcd.pipelines.promote.PromotePipeline.send_promote_complete_email")
    @patch("pyartcd.pipelines.promote.PromotePipeline.create_cincinnati_prs")
    async def test_run_with_standard_assembly(
        self,
        create_cincinnati_prs: AsyncMock,
        send_promote_complete_email: Mock,
        get_image_stream: AsyncMock,
        load_group_config: AsyncMock,
        load_releases_config: AsyncMock,
        get_release_image_info: AsyncMock,
        build_release_image: AsyncMock,
        start_cincinnati_prs: Mock,
        from_url: Mock,
        sign_artifacts: AsyncMock,
        run_with_lock: AsyncMock,
    ):
        def fake_run_with_lock(*args, **kwargs):
            async def inner():
                return await kwargs["coro"]

            return inner()

        run_with_lock.side_effect = fake_run_with_lock
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        runtime.new_slack_client.return_value = AsyncMock()
        runtime.new_slack_client.return_value.say.return_value = {'message': {'ts': ''}}
        runtime.new_slack_client.return_value.bind_channel = MagicMock()
        pipeline = await PromotePipeline.create(
            runtime,
            group="openshift-4.10",
            assembly="4.10.99",
            skip_mirror_binaries=True,
            signing_env="prod",
            skip_sigstore=True,
        )
        pipeline.check_blocker_bugs = AsyncMock()
        pipeline.change_advisory_state_qe = AsyncMock()
        pipeline.get_advisory_info = AsyncMock(
            return_value={
                "id": 2,
                "errata_id": 2222,
                "fulladvisory": "RHBA-2099:2222-02",
                "status": "QE",
            }
        )
        pipeline.verify_attached_bugs = AsyncMock(return_value=None)
        pipeline.get_image_stream_tag = AsyncMock(return_value=None)
        pipeline.tag_release = AsyncMock(return_value=None)
        pipeline.wait_for_stable = AsyncMock(return_value=None)
        pipeline.send_image_list_email = AsyncMock()
        pipeline.is_accepted = AsyncMock(return_value=False)
        pipeline.ocp_doomsday_backup = AsyncMock(return_value=None)
        await pipeline.run()
        load_group_config.assert_awaited_once()
        load_releases_config.assert_awaited_once_with(
            group='openshift-4.10', data_path='https://example.com/ocp-build-data.git'
        )
        pipeline.check_blocker_bugs.assert_awaited_once_with()
        for advisory in [1, 2, 3, 4]:
            pipeline.change_advisory_state_qe.assert_any_await(advisory)
        pipeline.get_advisory_info.assert_awaited_once_with(2)
        pipeline.verify_attached_bugs.assert_awaited_once_with(
            [1, 2, 3, 4], no_verify_blocking_bugs=False, verify_flaws=True
        )
        get_release_image_info.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-x86_64", raise_if_not_found=ANY
        )
        get_release_image_info.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-s390x", raise_if_not_found=ANY
        )
        build_release_image.assert_any_await(
            "4.10.99",
            "x86_64",
            ["4.10.98", "4.9.99"],
            ["4.11.45"],
            {"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"},
            "quay.io/openshift-release-dev/ocp-release:4.10.99-x86_64",
            None,
            "4.10-art-assembly-4.10.99",
            keep_manifest_list=False,
        )
        build_release_image.assert_any_await(
            "4.10.99",
            "s390x",
            ["4.10.98", "4.9.99"],
            ["4.11.45"],
            {"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"},
            "quay.io/openshift-release-dev/ocp-release:4.10.99-s390x",
            None,
            "4.10-art-assembly-4.10.99-s390x",
            keep_manifest_list=False,
        )
        build_release_image.assert_any_await(
            "4.10.99",
            "ppc64le",
            ["4.10.98", "4.9.99"],
            ["4.11.45"],
            {"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"},
            "quay.io/openshift-release-dev/ocp-release:4.10.99-ppc64le",
            None,
            "4.10-art-assembly-4.10.99-ppc64le",
            keep_manifest_list=False,
        )
        build_release_image.assert_any_await(
            "4.10.99",
            "aarch64",
            ["4.10.98", "4.9.99"],
            ["4.11.45"],
            {"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"},
            "quay.io/openshift-release-dev/ocp-release:4.10.99-aarch64",
            None,
            "4.10-art-assembly-4.10.99-arm64",
            keep_manifest_list=False,
        )
        pipeline._slack_client.bind_channel.assert_called_once_with("4.10.99")
        pipeline.get_image_stream_tag.assert_any_await("ocp", "release:4.10.99")
        pipeline.tag_release.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-x86_64", "ocp/release:4.10.99"
        )
        pipeline.tag_release.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-s390x", "ocp-s390x/release-s390x:4.10.99"
        )
        pipeline.tag_release.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-ppc64le", "ocp-ppc64le/release-ppc64le:4.10.99"
        )
        pipeline.tag_release.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-aarch64", "ocp-arm64/release-arm64:4.10.99"
        )
        pipeline.wait_for_stable.assert_any_await("4.10.99", "x86_64", "4-stable")
        pipeline.wait_for_stable.assert_any_await("4.10.99", "s390x", "4-stable-s390x")
        pipeline.wait_for_stable.assert_any_await("4.10.99", "ppc64le", "4-stable-ppc64le")
        pipeline.wait_for_stable.assert_any_await("4.10.99", "aarch64", "4-stable-arm64")
        pipeline.send_image_list_email.assert_awaited_once_with("4.10.99", 2, ANY)
        sign_artifacts.assert_awaited_once_with("4.10.99", "ocp", ANY, [])

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch("pyartcd.pipelines.promote.PromotePipeline.tag_release", return_value=None)
    @patch("pyartcd.pipelines.promote.PromotePipeline.get_image_stream_tag", return_value=None)
    @patch("pyartcd.pipelines.promote.PromotePipeline.build_release_image", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.get_release_image_info",
        side_effect=lambda pullspec, raise_if_not_found=False: {
            "image": pullspec,
            "digest": "fake:deadbeef",
            "metadata": {
                "version": "4.10.99",
            },
            "references": {
                "spec": {
                    "tags": [
                        {
                            "name": "machine-os-content",
                            "annotations": {"io.openshift.build.versions": "machine-os=00.00.212301010000-0"},
                        },
                    ],
                },
            },
        }
        if raise_if_not_found
        else None,
    )
    @patch("pyartcd.pipelines.promote.PromotePipeline.get_image_stream")
    async def test_promote_arch(
        self,
        get_image_stream: AsyncMock,
        get_release_image_info: AsyncMock,
        build_release_image: AsyncMock,
        get_image_stream_tag: AsyncMock,
        tag_release: AsyncMock,
        _,
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        pipeline = PromotePipeline(runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod")
        previous_list = ["4.10.98", "4.10.97", "4.9.99"]
        metadata = {"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"}

        # test x86_64
        actual = await pipeline._promote_arch(
            release_name="4.10.99",
            arch="x86_64",
            previous_list=previous_list,
            next_list=[],
            metadata=metadata,
            tag_stable=True,
            assembly_type=AssemblyTypes.CUSTOM,
        )
        get_release_image_info.assert_any_await("quay.io/openshift-release-dev/ocp-release:4.10.99-x86_64")
        build_release_image.assert_awaited_once_with(
            "4.10.99",
            "x86_64",
            previous_list,
            [],
            metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-x86_64",
            None,
            '4.10-art-assembly-4.10.99',
            keep_manifest_list=False,
        )
        get_image_stream_tag.assert_awaited_once_with("ocp", "release:4.10.99")
        tag_release.assert_awaited_once_with(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-x86_64", "ocp/release:4.10.99"
        )
        self.assertEqual(actual["image"], "quay.io/openshift-release-dev/ocp-release:4.10.99-x86_64")

        # test aarch64
        get_release_image_info.reset_mock()
        build_release_image.reset_mock()
        get_image_stream_tag.reset_mock()
        tag_release.reset_mock()
        actual = await pipeline._promote_arch(
            release_name="4.10.99",
            arch="aarch64",
            previous_list=previous_list,
            next_list=[],
            metadata=metadata,
            tag_stable=True,
            assembly_type=AssemblyTypes.CUSTOM,
        )
        get_release_image_info.assert_any_await("quay.io/openshift-release-dev/ocp-release:4.10.99-aarch64")
        build_release_image.assert_awaited_once_with(
            "4.10.99",
            "aarch64",
            previous_list,
            [],
            metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-aarch64",
            None,
            '4.10-art-assembly-4.10.99-arm64',
            keep_manifest_list=False,
        )
        get_image_stream_tag.assert_awaited_once_with("ocp-arm64", "release-arm64:4.10.99")
        tag_release.assert_awaited_once_with(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-aarch64", "ocp-arm64/release-arm64:4.10.99"
        )
        self.assertEqual(actual["image"], "quay.io/openshift-release-dev/ocp-release:4.10.99-aarch64")

        # test release tag already exists but doesn't match the to-be-promoted release image
        get_image_stream_tag.return_value = {
            "image": {
                "dockerImageReference": "quay.io/openshift-release-dev/ocp-release@fake:foobar",
            },
        }
        get_release_image_info.reset_mock()
        build_release_image.reset_mock()
        get_image_stream_tag.reset_mock()
        tag_release.reset_mock()
        with self.assertRaisesRegex(ValueError, "already exists, but it has a different digest"):
            await pipeline._promote_arch(
                release_name="4.10.99",
                arch="aarch64",
                previous_list=previous_list,
                next_list=[],
                metadata=metadata,
                tag_stable=True,
                assembly_type=AssemblyTypes.CUSTOM,
            )
        get_release_image_info.assert_any_await("quay.io/openshift-release-dev/ocp-release:4.10.99-aarch64")
        build_release_image.assert_awaited_once_with(
            "4.10.99",
            "aarch64",
            previous_list,
            [],
            metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-aarch64",
            None,
            '4.10-art-assembly-4.10.99-arm64',
            keep_manifest_list=False,
        )
        get_image_stream_tag.assert_awaited_once_with("ocp-arm64", "release-arm64:4.10.99")
        tag_release.assert_not_awaited()

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch("pyartcd.pipelines.promote.exectools.cmd_gather_async", return_value=0)
    async def test_build_release_image_from_reference_release(self, cmd_gather_async: AsyncMock, _):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        pipeline = PromotePipeline(runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod")
        previous_list = ["4.10.98", "4.10.97", "4.9.99"]
        metadata = {"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"}

        # test x86_64
        reference_release = "registry.ci.openshift.org/ocp/release:whatever-x86_64"
        dest_pullspec = "example.com/foo/release:4.10.99-x86_64"
        await pipeline.build_release_image(
            "4.10.99",
            "x86_64",
            previous_list,
            [],
            metadata,
            dest_pullspec,
            reference_release,
            None,
            keep_manifest_list=False,
        )
        expected_cmd = [
            "oc",
            "adm",
            "release",
            "new",
            "-n",
            "ocp",
            "--name=4.10.99",
            "--to-image=example.com/foo/release:4.10.99-x86_64",
            f"--from-release={reference_release}",
            "--previous=4.10.98,4.10.97,4.9.99",
            "--metadata",
            "{\"description\": \"whatever\", \"url\": \"https://access.redhat.com/errata/RHBA-2099:2222\"}",
        ]
        cmd_gather_async.assert_awaited_once_with(expected_cmd, env=ANY)

        # test aarch64
        reference_release = "registry.ci.openshift.org/ocp-arm64/release-arm64:whatever-aarch64"
        dest_pullspec = "example.com/foo/release:4.10.99-aarch64"
        cmd_gather_async.reset_mock()
        await pipeline.build_release_image(
            "4.10.99",
            "aarch64",
            previous_list,
            [],
            metadata,
            dest_pullspec,
            reference_release,
            None,
            keep_manifest_list=False,
        )
        expected_cmd = [
            "oc",
            "adm",
            "release",
            "new",
            "-n",
            "ocp-arm64",
            "--name=4.10.99",
            "--to-image=example.com/foo/release:4.10.99-aarch64",
            f"--from-release={reference_release}",
            "--previous=4.10.98,4.10.97,4.9.99",
            "--metadata",
            "{\"description\": \"whatever\", \"url\": \"https://access.redhat.com/errata/RHBA-2099:2222\"}",
        ]
        cmd_gather_async.assert_awaited_once_with(expected_cmd, env=ANY)

        # test multi-aarch64
        reference_release = "registry.ci.openshift.org/ocp-arm64/release-arm64:whatever-multi-aarch64"
        dest_pullspec = "example.com/foo/release:4.10.99-multi-aarch64"
        cmd_gather_async.reset_mock()
        await pipeline.build_release_image(
            "4.10.99",
            "aarch64",
            previous_list,
            [],
            metadata,
            dest_pullspec,
            reference_release,
            None,
            keep_manifest_list=True,
        )
        expected_cmd = [
            "oc",
            "adm",
            "release",
            "new",
            "-n",
            "ocp-arm64",
            "--name=4.10.99",
            "--to-image=example.com/foo/release:4.10.99-multi-aarch64",
            f"--from-release={reference_release}",
            "--keep-manifest-list",
            "--previous=4.10.98,4.10.97,4.9.99",
            "--metadata",
            "{\"description\": \"whatever\", \"url\": \"https://access.redhat.com/errata/RHBA-2099:2222\"}",
        ]
        cmd_gather_async.assert_awaited_once_with(expected_cmd, env=ANY)

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch("pyartcd.pipelines.promote.exectools.cmd_gather_async", return_value=0)
    async def test_build_release_image_from_image_stream(self, cmd_gather_async: AsyncMock, _):
        runtime = MagicMock(
            config={
                "build_config": {"ocp_build_data_url": "https://example.com/ocp-build-data.git"},
                "jira": {"url": "https://issues.redhat.com/"},
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        pipeline = PromotePipeline(runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod")
        previous_list = ["4.10.98", "4.10.97", "4.9.99"]
        metadata = {"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"}

        # test x86_64
        reference_release = None
        dest_pullspec = "example.com/foo/release:4.10.99-x86_64"
        from_image_stream = "4.10-art-assembly-4.10.99"
        await pipeline.build_release_image(
            "4.10.99",
            "x86_64",
            previous_list,
            [],
            metadata,
            dest_pullspec,
            reference_release,
            from_image_stream,
            keep_manifest_list=False,
        )
        expected_cmd = [
            'oc',
            'adm',
            'release',
            'new',
            '-n',
            'ocp',
            '--name=4.10.99',
            '--to-image=example.com/foo/release:4.10.99-x86_64',
            '--reference-mode=source',
            '--from-image-stream=4.10-art-assembly-4.10.99',
            '--previous=4.10.98,4.10.97,4.9.99',
            '--metadata',
            '{"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"}',
        ]
        cmd_gather_async.assert_awaited_once_with(expected_cmd, env=ANY)

        # test aarch64
        reference_release = None
        dest_pullspec = "example.com/foo/release:4.10.99-aarch64"
        from_image_stream = "4.10-art-assembly-4.10.99-arm64"
        cmd_gather_async.reset_mock()
        await pipeline.build_release_image(
            "4.10.99",
            "aarch64",
            previous_list,
            [],
            metadata,
            dest_pullspec,
            reference_release,
            from_image_stream,
            keep_manifest_list=False,
        )
        expected_cmd = [
            'oc',
            'adm',
            'release',
            'new',
            '-n',
            'ocp-arm64',
            '--name=4.10.99',
            '--to-image=example.com/foo/release:4.10.99-aarch64',
            '--reference-mode=source',
            '--from-image-stream=4.10-art-assembly-4.10.99-arm64',
            '--previous=4.10.98,4.10.97,4.9.99',
            '--metadata',
            '{"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"}',
        ]
        cmd_gather_async.assert_awaited_once_with(expected_cmd, env=ANY)

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch("pyartcd.pipelines.promote.PromotePipeline.tag_release", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.PromotePipeline.get_image_stream_tag",
        return_value={
            "tag": {
                "from": {
                    "name": "quay.io/openshift-release-dev/ocp-release:4.10.99-multi",
                },
            },
        },
    )
    @patch("pyartcd.pipelines.promote.PromotePipeline.push_manifest_list", return_value=None)
    @patch("pyartcd.pipelines.promote.PromotePipeline.build_release_image", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.PromotePipeline.get_image_stream",
        return_value={
            "spec": {
                "tags": [
                    {
                        "name": "4.10.99-0.art-assembly-4.10.99-multi-2022-07-26-210300",
                        "from": {"name": "example.com/ocp-release@fake:deadbeef-source-manifest-list"},
                    },
                ],
            },
        },
    )
    @patch(
        'pyartcd.pipelines.promote.PromotePipeline.get_image_info',
        side_effect=lambda pullspec, raise_if_not_found=False: {
            (
                "quay.io/openshift-release-dev/ocp-release:4.10.99-multi",
                True,
            ): TestPromotePipeline.FAKE_DEST_MANIFEST_LIST,
        }[pullspec, raise_if_not_found],
    )
    @patch(
        'pyartcd.pipelines.promote.PromotePipeline.get_multi_image_digest',
        return_value='fake:deadbeef-toplevel-manifest-list',
    )
    async def test_promote_heterogeneous_payload(
        self,
        get_image_digest: AsyncMock,
        get_image_info: AsyncMock,
        get_image_stream: AsyncMock,
        build_release_image: AsyncMock,
        push_manifest_list: AsyncMock,
        get_image_stream_tag: AsyncMock,
        tag_release: AsyncMock,
        _,
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        pipeline = PromotePipeline(runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod")
        previous_list = ["4.10.98", "4.10.97", "4.9.99"]
        metadata = {"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"}

        # test: heterogeneous payload already exists
        await pipeline._promote_heterogeneous_payload(
            release_name="4.10.99",
            include_arches=["x86_64", "aarch64"],
            previous_list=previous_list,
            next_list=[],
            metadata=metadata,
            tag_stable=True,
            assembly_type=AssemblyTypes.CUSTOM,
        )
        get_image_digest.assert_awaited_once_with("quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        get_image_info.assert_awaited_once_with(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", raise_if_not_found=True
        )
        get_image_stream_tag.assert_awaited_once_with("ocp-multi", "release-multi:4.10.99")
        build_release_image.assert_not_called()
        tag_release.assert_not_called()

        # test: promote a GA heterogeneous payload
        get_image_digest.reset_mock()
        get_image_digest.side_effect = lambda pullspec, raise_if_not_found=False: {
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", False): None,
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", True): "fake:deadbeef-dest-multi",
        }[pullspec, raise_if_not_found]
        get_image_info.reset_mock()

        get_image_info.side_effect = lambda pullspec, raise_if_not_found=False: {
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", True): None
            if not push_manifest_list.called
            else TestPromotePipeline.FAKE_DEST_MANIFEST_LIST,
            ('example.com/ocp-release@fake:deadbeef-source-manifest-list', True): {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
                "manifests": [
                    {
                        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                        "size": 1583,
                        "digest": "fake:deadbeef-source-multi-amd64",
                        "platform": {
                            "architecture": "amd64",
                            "os": "linux",
                        },
                    },
                    {
                        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                        "size": 1583,
                        "digest": "fake:deadbeef-source-multi-ppc64le",
                        "platform": {
                            "architecture": "ppc64le",
                            "os": "linux",
                        },
                    },
                    {
                        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                        "size": 1583,
                        "digest": "fake:deadbeef-source-multi-s390x",
                        "platform": {
                            "architecture": "s390x",
                            "os": "linux",
                        },
                    },
                    {
                        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                        "size": 1583,
                        "digest": "fake:deadbeef-source-multi-arm64",
                        "platform": {
                            "architecture": "arm64",
                            "os": "linux",
                        },
                    },
                ],
            },
        }[pullspec, raise_if_not_found]
        get_image_stream.reset_mock()
        get_image_stream_tag.reset_mock()
        get_image_stream_tag.return_value = None
        build_release_image.reset_mock()
        push_manifest_list.reset_mock()
        tag_release.reset_mock()
        actual = await pipeline._promote_heterogeneous_payload(
            release_name="4.10.99",
            include_arches=["x86_64", "aarch64"],
            previous_list=previous_list,
            next_list=[],
            metadata=metadata,
            tag_stable=True,
            assembly_type=AssemblyTypes.CUSTOM,
        )
        get_image_digest.assert_any_await("quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        get_image_digest.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", raise_if_not_found=True
        )
        get_image_info.assert_any_await(
            "example.com/ocp-release@fake:deadbeef-source-manifest-list", raise_if_not_found=True
        )
        get_image_info.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", raise_if_not_found=True
        )
        get_image_stream.assert_awaited_once_with("ocp-multi", "4.10-art-assembly-4.10.99-multi")
        get_image_stream_tag.assert_awaited_once_with("ocp-multi", "release-multi:4.10.99")
        dest_metadata = metadata.copy()
        dest_metadata["release.openshift.io/architecture"] = "multi"
        build_release_image.assert_any_await(
            "4.10.99",
            "aarch64",
            previous_list,
            [],
            dest_metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi-aarch64",
            'example.com/ocp-release@fake:deadbeef-source-multi-arm64',
            None,
            keep_manifest_list=True,
        )
        build_release_image.assert_any_await(
            "4.10.99",
            "x86_64",
            previous_list,
            [],
            dest_metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi-x86_64",
            'example.com/ocp-release@fake:deadbeef-source-multi-amd64',
            None,
            keep_manifest_list=True,
        )
        dest_manifest_list = {
            'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi',
            'manifests': [
                {
                    'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi-x86_64',
                    'platform': {'os': 'linux', 'architecture': 'amd64'},
                },
                {
                    'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi-aarch64',
                    'platform': {'os': 'linux', 'architecture': 'arm64'},
                },
            ],
        }
        push_manifest_list.assert_awaited_once_with("4.10.99", dest_manifest_list)
        tag_release.assert_awaited_once_with(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", "ocp-multi/release-multi:4.10.99"
        )
        self.assertEqual(actual["image"], "quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        self.assertEqual(actual["digest"], "fake:deadbeef-dest-multi")

        # test: promote GA heterogeneous payload
        get_image_digest.reset_mock()
        get_image_digest.side_effect = lambda pullspec, raise_if_not_found=False: {
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", False): None,
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", True): "fake:deadbeef-dest-multi",
        }[pullspec, raise_if_not_found]
        get_image_info.reset_mock()
        get_image_info.side_effect = lambda pullspec, raise_if_not_found=False: {
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", True): None
            if not push_manifest_list.called
            else TestPromotePipeline.FAKE_DEST_MANIFEST_LIST,
            (
                'example.com/ocp-release@fake:deadbeef-source-manifest-list',
                True,
            ): TestPromotePipeline.FAKE_SOURCE_MANIFEST_LIST,
        }[pullspec, raise_if_not_found]
        get_image_stream.reset_mock()
        get_image_stream_tag.reset_mock()
        get_image_stream_tag.return_value = None
        build_release_image.reset_mock()
        push_manifest_list.reset_mock()
        tag_release.reset_mock()
        actual = await pipeline._promote_heterogeneous_payload(
            release_name="4.10.99",
            include_arches=["x86_64", "aarch64"],
            previous_list=previous_list,
            next_list=[],
            metadata=metadata,
            tag_stable=True,
            assembly_type=AssemblyTypes.CUSTOM,
        )
        get_image_digest.assert_any_await("quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        get_image_digest.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", raise_if_not_found=True
        )
        get_image_info.assert_any_await(
            "example.com/ocp-release@fake:deadbeef-source-manifest-list", raise_if_not_found=True
        )
        get_image_info.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", raise_if_not_found=True
        )
        get_image_stream.assert_awaited_once_with("ocp-multi", "4.10-art-assembly-4.10.99-multi")
        get_image_stream_tag.assert_awaited_once_with("ocp-multi", "release-multi:4.10.99")
        dest_metadata = metadata.copy()
        dest_metadata["release.openshift.io/architecture"] = "multi"
        build_release_image.assert_any_await(
            "4.10.99",
            "aarch64",
            previous_list,
            [],
            dest_metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi-aarch64",
            'example.com/ocp-release@fake:deadbeef-source-multi-arm64',
            None,
            keep_manifest_list=True,
        )
        build_release_image.assert_any_await(
            "4.10.99",
            "x86_64",
            previous_list,
            [],
            dest_metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi-x86_64",
            'example.com/ocp-release@fake:deadbeef-source-multi-amd64',
            None,
            keep_manifest_list=True,
        )
        dest_manifest_list = {
            'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi',
            'manifests': [
                {
                    'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi-x86_64',
                    'platform': {'os': 'linux', 'architecture': 'amd64'},
                },
                {
                    'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi-aarch64',
                    'platform': {'os': 'linux', 'architecture': 'arm64'},
                },
            ],
        }
        push_manifest_list.assert_awaited_once_with("4.10.99", dest_manifest_list)
        tag_release.assert_awaited_once_with(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", "ocp-multi/release-multi:4.10.99"
        )
        self.assertEqual(actual["image"], "quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        self.assertEqual(actual["digest"], "fake:deadbeef-dest-multi")

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    @patch("pyartcd.pipelines.promote.PromotePipeline.tag_release", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.PromotePipeline.get_image_stream_tag",
        return_value={
            "tag": {
                "from": {
                    "name": "quay.io/openshift-release-dev/ocp-release:4.10.99-multi",
                },
            },
        },
    )
    @patch("pyartcd.pipelines.promote.PromotePipeline.push_manifest_list", return_value=None)
    @patch("pyartcd.pipelines.promote.PromotePipeline.build_release_image", return_value=None)
    @patch(
        "pyartcd.pipelines.promote.PromotePipeline.get_image_stream",
        return_value={
            "spec": {
                "tags": [
                    {
                        "name": "4.10.99-0.art-assembly-4.10.99-multi-2022-07-26-210300",
                        "from": {"name": "example.com/ocp-release@fake:deadbeef-source-manifest-list"},
                    },
                ],
            },
        },
    )
    @patch(
        'pyartcd.pipelines.promote.PromotePipeline.get_image_info',
        side_effect=lambda pullspec, raise_if_not_found=False: {
            (
                "quay.io/openshift-release-dev/ocp-release:4.10.99-multi",
                True,
            ): TestPromotePipeline.FAKE_DEST_MANIFEST_LIST,
        }[pullspec, raise_if_not_found],
    )
    @patch(
        'pyartcd.pipelines.promote.PromotePipeline.get_multi_image_digest',
        return_value='fake:deadbeef-toplevel-manifest-list',
    )
    async def test_build_release_image_from_heterogeneous_image_stream(
        self,
        get_image_digest: AsyncMock,
        get_image_info: AsyncMock,
        get_image_stream: AsyncMock,
        build_release_image: AsyncMock,
        push_manifest_list: AsyncMock,
        get_image_stream_tag: AsyncMock,
        tag_release: AsyncMock,
        _,
    ):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        pipeline = PromotePipeline(
            runtime, group="openshift-4.10", assembly="4.10.99", use_multi_hack=True, signing_env="prod"
        )
        previous_list = ["4.10.98", "4.10.97", "4.9.99"]
        metadata = {"description": "whatever", "url": "https://access.redhat.com/errata/RHBA-2099:2222"}

        # test: heterogeneous payload already exists
        await pipeline._promote_heterogeneous_payload(
            release_name="4.10.99",
            include_arches=["x86_64", "aarch64"],
            previous_list=previous_list,
            next_list=[],
            metadata=metadata,
            tag_stable=True,
            assembly_type=AssemblyTypes.CUSTOM,
        )
        get_image_digest.assert_awaited_once_with("quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        get_image_info.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", raise_if_not_found=True
        )
        get_image_stream_tag.assert_awaited_once_with("ocp-multi", "release-multi:4.10.99-multi")
        build_release_image.assert_not_called()
        tag_release.assert_not_called()

        # test: promote a GA heterogeneous payload
        get_image_digest.reset_mock()
        get_image_digest.side_effect = lambda pullspec, raise_if_not_found=False: {
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", False): None,
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", True): "fake:deadbeef-dest-multi",
        }[pullspec, raise_if_not_found]
        get_image_info.reset_mock()
        get_image_info.side_effect = lambda pullspec, raise_if_not_found=False: {
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", True): None
            if not push_manifest_list.called
            else TestPromotePipeline.FAKE_DEST_MANIFEST_LIST,
            (
                'example.com/ocp-release@fake:deadbeef-source-manifest-list',
                True,
            ): TestPromotePipeline.FAKE_SOURCE_MANIFEST_LIST,
        }[pullspec, raise_if_not_found]
        get_image_stream.reset_mock()
        get_image_stream_tag.reset_mock()
        get_image_stream_tag.return_value = None
        build_release_image.reset_mock()
        push_manifest_list.reset_mock()
        tag_release.reset_mock()
        actual = await pipeline._promote_heterogeneous_payload(
            release_name="4.10.99",
            include_arches=["x86_64", "aarch64"],
            previous_list=previous_list,
            next_list=[],
            metadata=metadata,
            tag_stable=True,
            assembly_type=AssemblyTypes.CUSTOM,
        )
        get_image_digest.assert_any_await("quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        get_image_digest.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", raise_if_not_found=True
        )
        get_image_info.assert_any_await(
            "example.com/ocp-release@fake:deadbeef-source-manifest-list", raise_if_not_found=True
        )
        get_image_stream.assert_awaited_once_with("ocp-multi", "4.10-art-assembly-4.10.99-multi")
        get_image_stream_tag.assert_awaited_once_with("ocp-multi", "release-multi:4.10.99-multi")
        dest_metadata = metadata.copy()
        dest_metadata["release.openshift.io/architecture"] = "multi"
        build_release_image.assert_any_await(
            "4.10.99-multi",
            "aarch64",
            [],
            [],
            dest_metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi-aarch64",
            'example.com/ocp-release@fake:deadbeef-source-multi-arm64',
            None,
            keep_manifest_list=True,
        )
        build_release_image.assert_any_await(
            "4.10.99-multi",
            "x86_64",
            [],
            [],
            dest_metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi-x86_64",
            'example.com/ocp-release@fake:deadbeef-source-multi-amd64',
            None,
            keep_manifest_list=True,
        )
        dest_manifest_list = {
            'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi',
            'manifests': [
                {
                    'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi-x86_64',
                    'platform': {'os': 'linux', 'architecture': 'amd64'},
                },
                {
                    'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi-aarch64',
                    'platform': {'os': 'linux', 'architecture': 'arm64'},
                },
            ],
        }
        push_manifest_list.assert_awaited_once_with("4.10.99-multi", dest_manifest_list)
        tag_release.assert_awaited_once_with(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", "ocp-multi/release-multi:4.10.99-multi"
        )
        self.assertEqual(actual["image"], "quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        self.assertEqual(actual["digest"], "fake:deadbeef-dest-multi")

        # test: promote GA heterogeneous payload
        get_image_digest.reset_mock()
        get_image_digest.side_effect = lambda pullspec, raise_if_not_found=False: {
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", False): None,
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", True): "fake:deadbeef-dest-multi",
        }[pullspec, raise_if_not_found]
        get_image_info.reset_mock()
        get_image_info.side_effect = lambda pullspec, raise_if_not_found=False: {
            ("quay.io/openshift-release-dev/ocp-release:4.10.99-multi", True): None
            if not push_manifest_list.called
            else TestPromotePipeline.FAKE_DEST_MANIFEST_LIST,
            (
                'example.com/ocp-release@fake:deadbeef-source-manifest-list',
                True,
            ): TestPromotePipeline.FAKE_SOURCE_MANIFEST_LIST,
        }[pullspec, raise_if_not_found]
        get_image_stream.reset_mock()
        get_image_stream_tag.reset_mock()
        get_image_stream_tag.return_value = None
        build_release_image.reset_mock()
        push_manifest_list.reset_mock()
        tag_release.reset_mock()
        actual = await pipeline._promote_heterogeneous_payload(
            release_name="4.10.99",
            include_arches=["x86_64", "aarch64"],
            previous_list=previous_list,
            next_list=[],
            metadata=metadata,
            tag_stable=True,
            assembly_type=AssemblyTypes.CUSTOM,
        )
        get_image_digest.assert_any_await("quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        get_image_digest.assert_any_await(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", raise_if_not_found=True
        )
        get_image_info.assert_any_await(
            "example.com/ocp-release@fake:deadbeef-source-manifest-list", raise_if_not_found=True
        )
        get_image_stream.assert_awaited_once_with("ocp-multi", "4.10-art-assembly-4.10.99-multi")
        get_image_stream_tag.assert_awaited_once_with("ocp-multi", "release-multi:4.10.99-multi")
        dest_metadata = metadata.copy()
        dest_metadata["release.openshift.io/architecture"] = "multi"
        build_release_image.assert_any_await(
            "4.10.99-multi",
            "aarch64",
            [],
            [],
            dest_metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi-aarch64",
            'example.com/ocp-release@fake:deadbeef-source-multi-arm64',
            None,
            keep_manifest_list=True,
        )
        build_release_image.assert_any_await(
            "4.10.99-multi",
            "x86_64",
            [],
            [],
            dest_metadata,
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi-x86_64",
            'example.com/ocp-release@fake:deadbeef-source-multi-amd64',
            None,
            keep_manifest_list=True,
        )
        dest_manifest_list = {
            'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi',
            'manifests': [
                {
                    'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi-x86_64',
                    'platform': {'os': 'linux', 'architecture': 'amd64'},
                },
                {
                    'image': 'quay.io/openshift-release-dev/ocp-release:4.10.99-multi-aarch64',
                    'platform': {'os': 'linux', 'architecture': 'arm64'},
                },
            ],
        }
        push_manifest_list.assert_awaited_once_with("4.10.99-multi", dest_manifest_list)
        tag_release.assert_awaited_once_with(
            "quay.io/openshift-release-dev/ocp-release:4.10.99-multi", "ocp-multi/release-multi:4.10.99-multi"
        )
        self.assertEqual(actual["image"], "quay.io/openshift-release-dev/ocp-release:4.10.99-multi")
        self.assertEqual(actual["digest"], "fake:deadbeef-dest-multi")

    @patch("pyartcd.jira_client.JIRAClient.from_url", return_value=None)
    def test_build_create_symlink(self, _):
        runtime = MagicMock(
            config={
                "build_config": {
                    "ocp_build_data_url": "https://example.com/ocp-build-data.git",
                },
                "jira": {
                    "url": "https://issues.redhat.com/",
                },
            },
            working_dir=Path("/path/to/working"),
            dry_run=False,
        )
        pipeline = PromotePipeline(runtime, group="openshift-4.10", assembly="4.10.99", signing_env="prod")
        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "openshift-client-linux-4.3.0-0.nightly-2019-12-06-161135.tar.gz").open("w").close()
            Path(temp_dir, "openshift-client-mac-4.3.0-0.nightly-2019-12-06-161135.tar.gz").open("w").close()
            Path(temp_dir, "openshift-install-mac-4.3.0-0.nightly-2019-12-06-161135.tar.gz").open("w").close()
            pipeline.create_symlink(temp_dir, False, False)
            self.assertTrue(os.path.exists(os.path.join(temp_dir, 'openshift-client-linux.tar.gz')))
            self.assertTrue(os.path.exists(os.path.join(temp_dir, 'openshift-client-mac.tar.gz')))
            self.assertTrue(os.path.exists(os.path.join(temp_dir, 'openshift-install-mac.tar.gz')))
