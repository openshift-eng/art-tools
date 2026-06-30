from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.model import Model
from doozerlib.backend.konflux_client import API_VERSION, KIND_APPLICATION, KIND_RELEASE, KIND_RELEASE_PLAN
from elliottlib.cli.konflux_release_cli import CreateReleaseCli, validate_snapshot_against_rpa
from elliottlib.cli.konflux_release_watch_cli import WatchReleaseCli
from elliottlib.shipment_model import (
    ComponentSource,
    Data,
    EnvAdvisory,
    Environments,
    GitSource,
    Metadata,
    ReleaseNotes,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    SnapshotComponent,
    SnapshotSpec,
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
        # Patch verify_connection and resource_url to be regular Mocks, not AsyncMock
        self.konflux_client.verify_connection = MagicMock(return_value=True)
        self.konflux_client.resource_url = MagicMock()

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

        status, obj = await cli.run()
        self.assertEqual(status, True)
        self.assertEqual(obj, Model(release))

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

        status, obj = await cli.run()
        self.assertEqual(status, False)
        self.assertEqual(obj, Model(release))

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

        status, obj = await cli.run()
        self.assertEqual(status, False)
        self.assertEqual(obj, Model(release))


class TestCreateReleaseCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.18"
        self.runtime.product = "ocp"
        self.runtime.assembly = "4.18.2"
        self.runtime.initialized = False  # Ensure initialization guard works
        self.runtime.get_major_minor.return_value = (4, 18)
        self.runtime.shipment_gitdata = MagicMock()

        self.dry_run = False

        self.konflux_config = dict(
            namespace="test-namespace",
            kubeconfig="/path/to/kubeconfig",
            context=None,
        )

        self.config_path = "shipment/ocp/openshift-4.18/openshift-4-18/4.18.2.202503210000.yml"
        self.release_env = "prod"
        self.image_repo_pull_secret = {}  # Use a dict as required by CreateReleaseCli

        self.konflux_client = AsyncMock()
        # Patch verify_connection and resource_url to be regular Mocks, not AsyncMock
        self.konflux_client.verify_connection = MagicMock(return_value=True)
        self.konflux_client.resource_url = MagicMock()

    @patch("elliottlib.cli.konflux_release_cli.validate_snapshot_against_rpa", new_callable=AsyncMock)
    @patch("elliottlib.cli.konflux_release_cli.get_utc_now_formatted_str", return_value="timestamp")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_prod_happy_path(self, mock_runtime, mock_konflux_client_init, _, __mock_validate):
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
                snapshot=Snapshot(
                    nvrs=["test-nvr-1", "test-nvr-2"],
                    spec=SnapshotSpec(
                        application="openshift-4-18",
                        components=[
                            SnapshotComponent(
                                name="test-rpm",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test-rpm.git", revision="abc123")
                                ),
                                containerImage="foo",
                            ),
                            SnapshotComponent(
                                name="test-container",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test-container.git", revision="def456")
                                ),
                                containerImage="bar",
                            ),
                        ],
                    ),
                ),
                data=Data(
                    releaseNotes=ReleaseNotes(
                        type="RHBA",
                        live_id=123456,
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

        # Mock snapshot creation
        created_snapshot_name = "ocp-prod-4-18-2-image-timestamp"
        created_snapshot = MagicMock()
        created_snapshot.metadata.name = created_snapshot_name

        expected_snapshot = {
            "apiVersion": API_VERSION,
            "kind": "Snapshot",
            "metadata": {
                "name": created_snapshot_name,
                "namespace": self.konflux_config['namespace'],
                "labels": {
                    "test.appstudio.openshift.io/type": "override",
                    "appstudio.openshift.io/application": shipment_config.shipment.metadata.application,
                },
                "annotations": {
                    "art.redhat.com/assembly": self.runtime.assembly,
                    "art.redhat.com/env": self.release_env,
                    "art.redhat.com/kind": "image",
                },
            },
            "spec": shipment_config.shipment.snapshot.spec.model_dump(exclude_none=True),
        }

        expected_release = {
            "apiVersion": API_VERSION,
            "kind": KIND_RELEASE,
            'metadata': {
                'name': 'ocp-prod-4-18-2-image-timestamp',
                'namespace': self.konflux_config['namespace'],
                'labels': {'appstudio.openshift.io/application': 'openshift-4-18'},
                "annotations": {
                    "art.redhat.com/assembly": self.runtime.assembly,
                    "art.redhat.com/env": self.release_env,
                    "art.redhat.com/kind": "image",
                },
            },
            'spec': {
                'releasePlan': shipment_config.shipment.environments.prod.releasePlan,
                'snapshot': created_snapshot_name,
                'data': {
                    'releaseNotes': {
                        'type': shipment_config.shipment.data.releaseNotes.type,
                        'live_id': shipment_config.shipment.data.releaseNotes.live_id,
                        'synopsis': shipment_config.shipment.data.releaseNotes.synopsis,
                        'topic': shipment_config.shipment.data.releaseNotes.topic,
                        'description': shipment_config.shipment.data.releaseNotes.description,
                        'solution': shipment_config.shipment.data.releaseNotes.solution,
                    },
                },
            },
        }

        created_release = Model(expected_release)
        self.konflux_client._create.side_effect = [
            created_snapshot,
            created_release,
        ]  # First call for snapshot, second for release
        self.konflux_client.resource_url.return_value = f"https://cluster/api/snapshot/{created_snapshot_name}"

        cli = CreateReleaseCli(
            runtime=self.runtime,
            config_path=self.config_path,
            release_env=self.release_env,
            konflux_config=self.konflux_config,
            image_repo_pull_secret={},
            dry_run=self.dry_run,
            kind="image",
        )

        result = await cli.run()

        # Verify runtime was initialized with shipment
        self.runtime.initialize.assert_called_once_with(build_system='konflux', with_shipment=True)

        # Verify resource existence was checked
        self.konflux_client._get.assert_any_call(
            API_VERSION, KIND_APPLICATION, shipment_config.shipment.metadata.application
        )
        self.konflux_client._get.assert_any_call(
            API_VERSION, KIND_RELEASE_PLAN, shipment_config.shipment.environments.prod.releasePlan
        )

        # Verify snapshot was created first, then release
        expected_calls = [
            ((expected_snapshot,), {}),
            ((expected_release,), {}),
        ]
        self.assertEqual(len(self.konflux_client._create.call_args_list), 2)
        self.assertEqual(
            self.konflux_client._create.call_args_list[0], expected_calls[0], "Snapshot resources do not match"
        )
        self.assertEqual(
            self.konflux_client._create.call_args_list[1], expected_calls[1], "Release resources do not match"
        )

        # Check result
        self.assertEqual(result, created_release)

    @patch("elliottlib.cli.konflux_release_cli.validate_snapshot_against_rpa", new_callable=AsyncMock)
    @patch("elliottlib.cli.konflux_release_cli.get_utc_now_formatted_str", return_value="timestamp")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_stage_happy_path(self, mock_runtime, mock_konflux_client_init, _, __mock_validate):
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
                snapshot=Snapshot(
                    nvrs=["test-nvr-1", "test-nvr-2"],
                    spec=SnapshotSpec(
                        application="openshift-4-18",
                        components=[
                            SnapshotComponent(
                                name="test-rpm",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test-rpm.git", revision="abc123")
                                ),
                                containerImage="foo",
                            ),
                            SnapshotComponent(
                                name="test-container",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test-container.git", revision="def456")
                                ),
                                containerImage="bar",
                            ),
                        ],
                    ),
                ),
                data=Data(
                    releaseNotes=ReleaseNotes(
                        type="RHBA",
                        live_id=123456,
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

        # Mock snapshot creation
        created_snapshot_name = "ocp-stage-4-18-2-image-timestamp"
        created_snapshot = MagicMock()
        created_snapshot.metadata.name = created_snapshot_name

        expected_snapshot = {
            "apiVersion": API_VERSION,
            "kind": "Snapshot",
            "metadata": {
                "name": created_snapshot_name,
                "namespace": self.konflux_config['namespace'],
                "labels": {
                    "test.appstudio.openshift.io/type": "override",
                    "appstudio.openshift.io/application": shipment_config.shipment.metadata.application,
                },
                "annotations": {
                    "art.redhat.com/assembly": self.runtime.assembly,
                    "art.redhat.com/env": "stage",
                    "art.redhat.com/kind": "image",
                },
            },
            "spec": shipment_config.shipment.snapshot.spec.model_dump(exclude_none=True),
        }

        expected_release = {
            "apiVersion": API_VERSION,
            "kind": KIND_RELEASE,
            'metadata': {
                'name': 'ocp-stage-4-18-2-image-timestamp',
                'namespace': self.konflux_config['namespace'],
                'labels': {'appstudio.openshift.io/application': 'openshift-4-18'},
                "annotations": {
                    "art.redhat.com/assembly": self.runtime.assembly,
                    "art.redhat.com/env": "stage",
                    "art.redhat.com/kind": "image",
                },
            },
            'spec': {
                'releasePlan': shipment_config.shipment.environments.stage.releasePlan,
                'snapshot': created_snapshot_name,
                'data': {
                    'releaseNotes': {
                        'type': shipment_config.shipment.data.releaseNotes.type,
                        'synopsis': shipment_config.shipment.data.releaseNotes.synopsis,
                        'topic': shipment_config.shipment.data.releaseNotes.topic,
                        'description': shipment_config.shipment.data.releaseNotes.description,
                        'solution': shipment_config.shipment.data.releaseNotes.solution,
                    },
                },
            },
        }

        created_release = Model(expected_release)
        self.konflux_client._create.side_effect = [
            created_snapshot,
            created_release,
        ]  # First call for snapshot, second for release
        self.konflux_client.resource_url.return_value = f"https://cluster/api/snapshot/{created_snapshot_name}"

        cli = CreateReleaseCli(
            runtime=self.runtime,
            config_path=self.config_path,
            release_env="stage",
            konflux_config=self.konflux_config,
            image_repo_pull_secret={},
            dry_run=self.dry_run,
            kind="image",
        )

        result = await cli.run()

        # Verify runtime was initialized with shipment
        self.runtime.initialize.assert_called_once_with(build_system='konflux', with_shipment=True)

        # Verify resource existence was checked
        self.konflux_client._get.assert_any_call(
            API_VERSION, KIND_APPLICATION, shipment_config.shipment.metadata.application
        )
        self.konflux_client._get.assert_any_call(
            API_VERSION, KIND_RELEASE_PLAN, shipment_config.shipment.environments.stage.releasePlan
        )

        # Verify snapshot was created first, then release
        expected_calls = [
            ((expected_snapshot,), {}),
            ((expected_release,), {}),
        ]
        self.assertEqual(len(self.konflux_client._create.call_args_list), 2)
        self.assertEqual(
            self.konflux_client._create.call_args_list[0], expected_calls[0], "Snapshot resources do not match"
        )
        self.assertEqual(
            self.konflux_client._create.call_args_list[1], expected_calls[1], "Release resources do not match"
        )

        # Check result
        self.assertEqual(result, created_release)

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
                    "nvrs": ["test-nvr-1", "test-nvr-2"],
                    "spec": {
                        "application": "test-app",
                        "components": [],
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
            image_repo_pull_secret={},
            dry_run=self.dry_run,
            kind="image",
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
                    "nvrs": ["test-nvr-1", "test-nvr-2"],
                    "spec": {
                        "application": "test-app",
                        "components": [],
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
            image_repo_pull_secret={},
            dry_run=self.dry_run,
            kind="image",
        )

        with self.assertRaises(ValueError) as context:
            await cli.run()

        self.assertIn(
            "A regular shipment is expected to have data.releaseNotes defined",
            str(context.exception),
        )

    @patch("elliottlib.cli.konflux_release_cli.get_utc_now_formatted_str", return_value="timestamp")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_shipped(self, mock_runtime, mock_konflux_client_init, _):
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
                        advisory=EnvAdvisory(
                            url="https://foo-bar",
                            internal_url="https://foo-bar-internal",
                        ),
                    ),
                ),
                snapshot=Snapshot(
                    nvrs=["test-nvr-1", "test-nvr-2"],
                    spec=SnapshotSpec(
                        application="openshift-4-18",
                        components=[
                            SnapshotComponent(
                                name="test-component",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test.git", revision="abc123")
                                ),
                                containerImage="test-image",
                            ),
                        ],
                    ),
                ),
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
            kind="image",
        )

        with patch("elliottlib.cli.konflux_release_cli.LOGGER") as mock_logger:
            result = await cli.run()

        self.assertIsNone(result)

        mock_logger.warning.assert_called_once_with(
            "existing release metadata is not empty for prod: "
            "{'releasePlan': 'test-prod-rp', 'advisory': {'internal_url': 'https://foo-bar-internal', 'url': 'https://foo-bar'}, 'result': None}. If you want to proceed, either remove the release metadata from the shipment config or use the --force flag."
        )

        # assert that release did not get created
        self.assertEqual(self.konflux_client._create.call_count, 0)

    @patch("elliottlib.cli.konflux_release_cli.fetch_rpa", new_callable=AsyncMock)
    async def test_validate_rpa_success(self, mock_fetch_rpa):
        rpa_data = {
            "spec": {
                "data": {
                    "mapping": {
                        "components": [{"name": "test-rpm"}, {"name": "test-container"}, {"name": "extra-component"}]
                    }
                }
            }
        }
        mock_fetch_rpa.return_value = rpa_data

        await validate_snapshot_against_rpa("openshift-4.18", "prod", "image", ["test-rpm", "test-container"])
        mock_fetch_rpa.assert_awaited_once_with("ocp-art-advisory-prod-4-18")

    @patch("elliottlib.cli.konflux_release_cli.fetch_rpa", new_callable=AsyncMock)
    async def test_validate_rpa_missing_components(self, mock_fetch_rpa):
        rpa_data = {"spec": {"data": {"mapping": {"components": [{"name": "test-rpm"}]}}}}
        mock_fetch_rpa.return_value = rpa_data

        with self.assertRaises(ValueError) as ctx:
            await validate_snapshot_against_rpa("openshift-4.18", "prod", "image", ["test-rpm", "missing-component"])

        self.assertIn("missing-component", str(ctx.exception))
        self.assertIn("silently filtered out", str(ctx.exception))
        mock_fetch_rpa.assert_awaited_once_with("ocp-art-advisory-prod-4-18")

    @patch("elliottlib.cli.konflux_release_cli.fetch_rpa", new_callable=AsyncMock)
    async def test_validate_rpa_fbc_success(self, mock_fetch_rpa):
        rpa_data = {"spec": {"data": {"fbc": {"allowedPackages": ["sriov-network-operator", "metallb-operator"]}}}}
        mock_fetch_rpa.return_value = rpa_data

        await validate_snapshot_against_rpa("openshift-4.18", "prod", "fbc", ["sriov-network-operator"])
        mock_fetch_rpa.assert_awaited_once_with("ocp-art-fbc-prod-4-18")

    @patch("elliottlib.cli.konflux_release_cli.fetch_rpa", new_callable=AsyncMock)
    async def test_validate_rpa_fbc_missing_packages(self, mock_fetch_rpa):
        rpa_data = {"spec": {"data": {"fbc": {"allowedPackages": ["sriov-network-operator"]}}}}
        mock_fetch_rpa.return_value = rpa_data

        with self.assertRaises(ValueError) as ctx:
            await validate_snapshot_against_rpa(
                "openshift-4.18", "prod", "fbc", ["sriov-network-operator", "unknown-operator"]
            )

        self.assertIn("unknown-operator", str(ctx.exception))
        mock_fetch_rpa.assert_awaited_once_with("ocp-art-fbc-prod-4-18")

    @patch("elliottlib.cli.konflux_release_cli.fetch_rpa", new_callable=AsyncMock)
    async def test_validate_rpa_fetch_failure(self, mock_fetch_rpa):
        mock_fetch_rpa.side_effect = ValueError("HTTP 404")

        with self.assertRaises(ValueError) as ctx:
            await validate_snapshot_against_rpa("openshift-4.18", "prod", "image", ["comp1"])

        self.assertIn("HTTP 404", str(ctx.exception))
        mock_fetch_rpa.assert_awaited_once_with("ocp-art-advisory-prod-4-18")

    @patch("elliottlib.cli.konflux_release_cli.fetch_rpa", new_callable=AsyncMock)
    async def test_validate_rpa_skipped_for_non_openshift(self, mock_fetch_rpa):
        await validate_snapshot_against_rpa("oadp-1.5", "prod", "image", ["comp1"])
        mock_fetch_rpa.assert_not_called()

    @patch("elliottlib.cli.konflux_release_cli.fetch_rpa", new_callable=AsyncMock)
    @patch("elliottlib.cli.konflux_release_cli.get_utc_now_formatted_str", return_value="timestamp")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_validates_rpa_before_snapshot(self, mock_runtime, mock_konflux_client_init, _, mock_fetch_rpa):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        rpa_data = {"spec": {"data": {"mapping": {"components": [{"name": "test-rpm"}]}}}}
        mock_fetch_rpa.return_value = rpa_data

        shipment_config = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product="ocp", application="openshift-4-18", group="openshift-4.18", assembly="4.18.2"
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan="test-stage-rp"),
                    prod=ShipmentEnv(releasePlan="test-prod-rp"),
                ),
                snapshot=Snapshot(
                    nvrs=["nvr1"],
                    spec=SnapshotSpec(
                        application="openshift-4-18",
                        components=[
                            SnapshotComponent(
                                name="test-rpm",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test.git", revision="abc")
                                ),
                                containerImage="img1",
                            ),
                            SnapshotComponent(
                                name="unauthorized-component",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test.git", revision="def")
                                ),
                                containerImage="img2",
                            ),
                        ],
                    ),
                ),
                data=Data(
                    releaseNotes=ReleaseNotes(type="RHBA", synopsis="s", topic="t", description="d", solution="s")
                ),
            ),
        )
        self.runtime.shipment_gitdata.load_yaml_file.return_value = shipment_config.model_dump(exclude_none=True)
        self.konflux_client._get_api.return_value = MagicMock()
        self.konflux_client._get.return_value = MagicMock()

        cli = CreateReleaseCli(
            runtime=self.runtime,
            config_path=self.config_path,
            release_env=self.release_env,
            konflux_config=self.konflux_config,
            image_repo_pull_secret={},
            dry_run=self.dry_run,
            kind="image",
        )

        with self.assertRaises(ValueError) as ctx:
            await cli.run()

        self.assertIn("unauthorized-component", str(ctx.exception))
        self.assertEqual(self.konflux_client._create.call_count, 0)
