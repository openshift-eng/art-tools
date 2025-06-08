from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.model import Model
from doozerlib.backend.konflux_client import API_VERSION, KIND_APPLICATION, KIND_RELEASE, KIND_RELEASE_PLAN
from elliottlib.cli.konflux_release_cli import CreateReleaseCli
from elliottlib.cli.konflux_release_watch_cli import WatchReleaseCli
from elliottlib.shipment_model import (
    Data,
    Environments,
    Metadata,
    ReleaseNotes,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    Spec,
)


class TestWatchReleaseCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.18"
        self.runtime.get_major_minor.return_value = (4, 18)

        self.dry_run = False

        self.konflux_config = dict(
            namespace="test-namespace",
            kubeconfig="/path/to/kubeconfig",
            context=None,
        )

        self.konflux_client = AsyncMock()
        self.konflux_client.verify_connection.return_value = True

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_happy_path(self, mock_runtime, mock_konflux_client_init):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        release = "test-release-prod"
        cli = WatchReleaseCli(
            release=release,
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            timeout=0,
            dry_run=self.dry_run,
        )

        release = {
            'apiVersion': API_VERSION,
            'kind': KIND_RELEASE,
            'metadata': {
                'name': release,
                'namespace': self.konflux_config['namespace'],
            },
            'status': {
                'conditions': [
                    {
                        'type': 'Released',
                        'status': 'True',
                        'reason': 'Succeeded',
                    },
                ],
            },
        }
        self.konflux_client.wait_for_release.return_value = Model(release)

        status = await cli.run()
        self.assertEqual(status, True)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_skipped(self, mock_runtime, mock_konflux_client_init):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        release = "test-release-prod"
        cli = WatchReleaseCli(
            release=release,
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            timeout=0,
            dry_run=self.dry_run,
        )

        release = {
            'apiVersion': API_VERSION,
            'kind': KIND_RELEASE,
            'metadata': {
                'name': release,
                'namespace': self.konflux_config['namespace'],
            },
            'status': {
                'conditions': [
                    {
                        'type': 'Released',
                        'status': 'True',
                        'reason': 'Skipped',
                    },
                ],
            },
        }
        self.konflux_client.wait_for_release.return_value = Model(release)

        status = await cli.run()
        self.assertEqual(status, False)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_failed(self, mock_runtime, mock_konflux_client_init):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        release = "test-release-prod"
        cli = WatchReleaseCli(
            release=release,
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            timeout=0,
            dry_run=self.dry_run,
        )

        release = {
            'apiVersion': API_VERSION,
            'kind': KIND_RELEASE,
            'metadata': {
                'name': release,
                'namespace': self.konflux_config['namespace'],
            },
            'status': {
                'conditions': [
                    {
                        'type': 'Released',
                        'status': 'False',
                        'reason': 'Failed',
                    },
                ],
            },
        }
        self.konflux_client.wait_for_release.return_value = Model(release)

        status = await cli.run()
        self.assertEqual(status, False)


class TestCreateReleaseCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.18"
        self.runtime.product = "ocp"
        self.runtime.assembly = "4.18.2"
        self.runtime.get_major_minor.return_value = (4, 18)
        self.runtime.shipment_gitdata = MagicMock()

        self.dry_run = False
        self.force = False

        self.konflux_config = dict(
            namespace="test-namespace",
            kubeconfig="/path/to/kubeconfig",
            context=None,
        )

        self.config_path = "shipment/ocp/openshift-4.18/openshift-4-18/4.18.2.202503210000.yml"
        self.release_env = "stage"
        self.image_repo_pull_secret = "/path/to/pull-secret"

        self.konflux_client = AsyncMock()
        self.konflux_client.verify_connection.return_value = True

    @patch("elliottlib.cli.konflux_release_cli.get_utc_now_formatted_str", return_value="timestamp")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.cli.konflux_release_cli.GetSnapshotCli")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_happy_path(self, mock_runtime, mock_get_snapshot_cli, mock_konflux_client_init, _):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        shipment_config = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product="ocp",
                    application="openshift-4-18",
                    group="openshift-4.18",
                    assembly="4.18.2",
                    fbc=False,
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan="test-stage-rp"),
                    prod=ShipmentEnv(releasePlan="test-prod-rp"),
                ),
                snapshot=Snapshot(name="test-snapshot", spec=Spec(nvrs=["test-nvr-1", "test-nvr-2"])),
                data=Data(
                    releaseNotes=ReleaseNotes(
                        type="RHBA",
                        synopsis="Red Hat Openshift Test Release",
                        topic="Topic for a test release for Red Hat Openshift.",
                        description="Description for a test release for Red Hat Openshift.",
                        solution="Solution for a test release for Red Hat Openshift.",
                    ),
                ),
            ),
        )
        self.runtime.shipment_gitdata.load_yaml_file.return_value = shipment_config.model_dump(exclude_none=True)

        # Mock API queries
        self.konflux_client._get_api.return_value = MagicMock()
        self.konflux_client._get.return_value = MagicMock()

        # Mock snapshot verification
        mock_get_snapshot_instance = AsyncMock()
        mock_get_snapshot_instance.run.return_value = shipment_config.shipment.snapshot.spec.nvrs
        mock_get_snapshot_cli.return_value = mock_get_snapshot_instance

        expected_release = {
            "apiVersion": API_VERSION,
            "kind": KIND_RELEASE,
            'metadata': {
                'name': 'ose-4-18-stage-timestamp',
                'namespace': self.konflux_config['namespace'],
                'labels': {'appstudio.openshift.io/application': 'openshift-4-18'},
            },
            'spec': {
                'releasePlan': shipment_config.shipment.environments.stage.releasePlan,
                'snapshot': shipment_config.shipment.snapshot.name,
                'data': {
                    'releaseNotes': {
                        'type': shipment_config.shipment.data.releaseNotes.type,
                        'synopsis': shipment_config.shipment.data.releaseNotes.synopsis,
                        'topic': shipment_config.shipment.data.releaseNotes.topic,
                        'description': shipment_config.shipment.data.releaseNotes.description,
                        'solution': shipment_config.shipment.data.releaseNotes.solution,
                        'cves': [],
                    },
                },
            },
        }
        self.konflux_client._create.return_value = Model(expected_release)

        cli = CreateReleaseCli(
            runtime=self.runtime,
            config_path=self.config_path,
            release_env=self.release_env,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            dry_run=self.dry_run,
            force=self.force,
        )

        result = await cli.run()

        # Verify runtime was initialized with shipment
        self.runtime.initialize.assert_called_once_with(with_shipment=True)

        # Verify resource existence was checked
        self.konflux_client._get.assert_any_call(
            API_VERSION, KIND_APPLICATION, shipment_config.shipment.metadata.application
        )
        self.konflux_client._get.assert_any_call(
            API_VERSION, KIND_RELEASE_PLAN, shipment_config.shipment.environments.stage.releasePlan
        )

        # Verify release was created with correct parameters
        self.konflux_client._create.assert_called_once_with(expected_release)

        # Check result
        self.assertEqual(result, Model(expected_release))

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_config_validation_error(self, mock_runtime, mock_konflux_client_init):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        # initialize a regular shipment without a required field i.e. metadata.assembly
        # so that the shipment schema validation fails
        shipment_config = {
            "shipment": {
                "metadata": {
                    "product": "ocp",
                    "application": "openshift-4-18",
                    "group": "openshift-4.18",
                    "fbc": True,
                },
                "environments": {
                    "stage": {
                        "releasePlan": "test-stage-rp",
                    },
                    "prod": {
                        "releasePlan": "test-prod-rp",
                    },
                },
                "snapshot": {
                    "name": "test-snapshot",
                    "spec": {
                        "nvrs": ["test-nvr-1", "test-nvr-2"],
                    },
                },
            },
        }
        self.runtime.shipment_gitdata.load_yaml_file.return_value = shipment_config

        cli = CreateReleaseCli(
            runtime=self.runtime,
            config_path=self.config_path,
            release_env=self.release_env,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            dry_run=self.dry_run,
            force=self.force,
        )

        with self.assertRaises(ValueError) as context:
            await cli.run()

        self.assertIn(
            "1 validation error for ShipmentConfig\nshipment.metadata.assembly\n  Field required",
            str(context.exception),
        )

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_config_data_validation_error(self, mock_runtime, mock_konflux_client_init):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        # initialize a regular shipment without `data`
        # so that the shipment schema validation fails
        shipment_config = {
            "shipment": {
                "metadata": {
                    "product": "ocp",
                    "application": "openshift-4-18",
                    "group": "openshift-4.18",
                    "assembly": "4.18.2",
                    "fbc": False,
                },
                "environments": {
                    "stage": {
                        "releasePlan": "test-stage-rp",
                    },
                    "prod": {
                        "releasePlan": "test-prod-rp",
                    },
                },
                "snapshot": {
                    "name": "test-snapshot",
                    "spec": {
                        "nvrs": ["test-nvr-1", "test-nvr-2"],
                    },
                },
            },
        }
        self.runtime.shipment_gitdata.load_yaml_file.return_value = shipment_config

        cli = CreateReleaseCli(
            runtime=self.runtime,
            config_path=self.config_path,
            release_env=self.release_env,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            dry_run=self.dry_run,
            force=self.force,
        )

        with self.assertRaises(ValueError) as context:
            await cli.run()

        self.assertIn("A regular shipment is expected to have data.releaseNotes defined", str(context.exception))

    @patch("elliottlib.cli.konflux_release_cli.get_utc_now_formatted_str", return_value="timestamp")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.cli.konflux_release_cli.GetSnapshotCli")
    @patch("elliottlib.runtime.Runtime")
    async def test_shipped_no_force(self, mock_runtime, mock_get_snapshot_cli, mock_konflux_client_init, _):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        shipment_config = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product="ocp",
                    application="openshift-4-18",
                    group="openshift-4.18",
                    assembly="4.18.2",
                    fbc=False,
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan="test-stage-rp"),
                    prod=ShipmentEnv(
                        releasePlan="test-prod-rp",
                        advisoryInternalUrl="foo-bar",
                    ),
                ),
                snapshot=Snapshot(name="test-snapshot", spec=Spec(nvrs=["test-nvr-1", "test-nvr-2"])),
                data=Data(
                    releaseNotes=ReleaseNotes(
                        type="RHBA",
                        synopsis="Red Hat Openshift Test Release",
                        topic="Topic for a test release for Red Hat Openshift.",
                        description="Description for a test release for Red Hat Openshift.",
                        solution="Solution for a test release for Red Hat Openshift.",
                    ),
                ),
            ),
        )
        self.runtime.shipment_gitdata.load_yaml_file.return_value = shipment_config.model_dump(exclude_none=True)

        # Mock API queries
        self.konflux_client._get_api.return_value = MagicMock()
        self.konflux_client._get.return_value = MagicMock()

        cli = CreateReleaseCli(
            runtime=self.runtime,
            config_path=self.config_path,
            release_env="prod",
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            dry_run=self.dry_run,
            force=False,
        )

        with self.assertRaises(ValueError) as context:
            await cli.run()

        self.assertIn("existing release metadata is not empty for prod", str(context.exception))

    @patch("elliottlib.cli.konflux_release_cli.get_utc_now_formatted_str", return_value="timestamp")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.cli.konflux_release_cli.GetSnapshotCli")
    @patch("elliottlib.runtime.Runtime")
    async def test_snapshot_validation_error(self, mock_runtime, mock_get_snapshot_cli, mock_konflux_client_init, _):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        shipment_config = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product="ocp",
                    application="openshift-4-18",
                    group="openshift-4.18",
                    assembly="4.18.2",
                    fbc=False,
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan="test-stage-rp"),
                    prod=ShipmentEnv(releasePlan="test-prod-rp"),
                ),
                snapshot=Snapshot(name="test-snapshot", spec=Spec(nvrs=["test-nvr-1", "test-nvr-2"])),
                data=Data(
                    releaseNotes=ReleaseNotes(
                        type="RHBA",
                        synopsis="Red Hat Openshift Test Release",
                        topic="Topic for a test release for Red Hat Openshift.",
                        description="Description for a test release for Red Hat Openshift.",
                        solution="Solution for a test release for Red Hat Openshift.",
                    ),
                ),
            ),
        )
        self.runtime.shipment_gitdata.load_yaml_file.return_value = shipment_config.model_dump(exclude_none=True)

        # Mock API queries
        self.konflux_client._get_api.return_value = MagicMock()
        self.konflux_client._get.return_value = MagicMock()

        # Mock snapshot verification
        mock_get_snapshot_instance = AsyncMock()
        # not all NVRs are present in snapshot
        mock_get_snapshot_instance.run.return_value = ["test-nvr-1", "test-nvr-3"]
        mock_get_snapshot_cli.return_value = mock_get_snapshot_instance

        cli = CreateReleaseCli(
            runtime=self.runtime,
            config_path=self.config_path,
            release_env=self.release_env,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            dry_run=self.dry_run,
            force=self.force,
        )

        with self.assertRaises(ValueError) as context:
            await cli.run()

        self.assertIn(
            "snapshot includes missing or extra nvrs than what's defined in spec: missing={'test-nvr-2'} "
            "extra={'test-nvr-3'}",
            str(context.exception),
        )
