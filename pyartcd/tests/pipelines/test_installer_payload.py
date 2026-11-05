import asyncio
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

import click
from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from doozerlib.cli.images_konflux import images_konflux_build
from pyartcd.pipelines.build_layered_products import BuildLayeredProductsPipeline, build_layered_products
from pyartcd.runtime import Runtime


class TestInstallerPayload(IsolatedAsyncioTestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.runtime = Runtime({}, Path(tmpdir.name), dry_run=True)

    def pipeline(self, payload=None):
        with patch('pyartcd.pipelines.build_layered_products.tekton.is_tekton_context', return_value=True):
            return BuildLayeredProductsPipeline(
                runtime=self.runtime,
                group='installer-ove-ui-4.22',
                version=None,
                assembly='test',
                image_list='art-agent-installer-iso',
                data_path='https://github.com/openshift-eng/ocp-build-data',
                skip_bundle_build=True,
                skip_rebase=True,
                payload=payload,
            )

    async def test_installer_requires_nonempty_payload_before_build(self):
        for payload in (None, '', ' \t '):
            with self.subTest(payload=payload):
                pipeline = self.pipeline(payload)
                with (
                    patch(
                        'pyartcd.pipelines.build_layered_products.load_group_config',
                        return_value={'product': 'openshift_agent_installer', 'version': '4.22'},
                    ),
                    patch.object(pipeline, '_rebase_and_build', new_callable=AsyncMock) as build,
                    self.assertRaisesRegex(click.UsageError, '--payload is required'),
                ):
                    await pipeline.run()
                build.assert_not_awaited()

    async def test_other_products_reject_payload_but_work_without_it(self):
        for product in ('oadp', 'coo', 'quay', 'ocp', None):
            for payload in (None, 'quay.io/example/release:4.22.1', ''):
                with self.subTest(product=product, payload=payload):
                    pipeline = self.pipeline(payload)
                    with (
                        patch(
                            'pyartcd.pipelines.build_layered_products.load_group_config',
                            return_value={'product': product, 'version': '4.22'},
                        ),
                        patch.object(pipeline, '_rebase_and_build', new_callable=AsyncMock) as build,
                        patch('pyartcd.pipelines.build_layered_products.oc.get_release_image_info') as info,
                    ):
                        if payload is not None:
                            with self.assertRaisesRegex(click.UsageError, 'only supported'):
                                await pipeline.run()
                            build.assert_not_awaited()
                        else:
                            await pipeline.run()
                            build.assert_awaited_once()
                            self.assertEqual(pipeline._build_args, {})
                        info.assert_not_called()

    async def test_payload_metadata_drives_build_arguments_for_tags_and_digests(self):
        for payload in ('quay.io/example/release:custom-tag', 'quay.io/example/release@sha256:' + 'a' * 64):
            with self.subTest(payload=payload):
                pipeline = self.pipeline(payload)
                with (
                    patch(
                        'pyartcd.pipelines.build_layered_products.load_group_config',
                        return_value={'product': 'openshift_agent_installer', 'version': '4.22'},
                    ),
                    patch(
                        'pyartcd.pipelines.build_layered_products.oc.get_release_image_info',
                        return_value={'metadata': {'version': '4.22.19'}},
                    ),
                    patch(
                        'pyartcd.pipelines.build_layered_products.resolve_konflux_kubeconfig_by_product',
                        return_value='/test/kubeconfig',
                    ),
                    patch('pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async') as command,
                ):
                    await pipeline.run()
                args = command.call_args.args[0]
                overrides = [args[index + 1] for index, arg in enumerate(args) if arg == '--build-arg']
                self.assertEqual(
                    overrides,
                    [
                        'RELEASE_FLAG=--release-image-url',
                        f'RELEASE_VALUE={payload}',
                        'MAJOR_MINOR_VERSION=4.22',
                        'PATCH_VERSION=19',
                    ],
                )
                self.assertIn('--dry-run', args)
                self.assertIn('--konflux-namespace=art-installer-agent-tenant', args)

    async def test_wrong_minor_fails_before_build(self):
        pipeline = self.pipeline('quay.io/example/release:4.21.19')
        with (
            patch(
                'pyartcd.pipelines.build_layered_products.load_group_config',
                return_value={'product': 'openshift_agent_installer', 'version': '4.22'},
            ),
            patch(
                'pyartcd.pipelines.build_layered_products.oc.get_release_image_info',
                return_value={'metadata': {'version': '4.21.19'}},
            ),
            patch.object(pipeline, '_rebase_and_build', new_callable=AsyncMock) as build,
            self.assertRaisesRegex(ValueError, 'does not match installer group'),
        ):
            await pipeline.run()
        build.assert_not_awaited()

    async def test_payload_overrides_only_apply_to_build(self):
        pipeline = self.pipeline()
        pipeline._build_args = {'RELEASE_VALUE': 'quay.io/example/release:4.22.19'}
        with (
            patch('pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async') as command,
            patch('pyartcd.pipelines.build_layered_products.update_rebase_fail_counters'),
        ):
            await pipeline._rebase('art-agent-installer-iso', BuildVariant.AGENT_INSTALLER)
        self.assertNotIn('--build-arg', command.call_args.args[0])

    async def test_explicit_build_version_must_also_match_payload_minor(self):
        for version in ('4.23.0', '4.22.0', 'auto'):
            with self.subTest(version=version):
                pipeline = self.pipeline('quay.io/example/release:4.22.21')
                pipeline.version = version
                with (
                    patch(
                        'pyartcd.pipelines.build_layered_products.load_group_config',
                        return_value={'product': 'openshift_agent_installer', 'version': '4.22'},
                    ),
                    patch(
                        'pyartcd.pipelines.build_layered_products.oc.get_release_image_info',
                        return_value={'metadata': {'version': '4.22.21'}},
                    ),
                    patch.object(pipeline, '_rebase_and_build', new_callable=AsyncMock) as build,
                ):
                    if version == '4.23.0':
                        with self.assertRaisesRegex(ValueError, 'does not match installer build version'):
                            await pipeline.run()
                        build.assert_not_awaited()
                    else:
                        await pipeline.run()
                        build.assert_awaited_once()


class TestInstallerPayloadCli(TestCase):
    def test_product_matrix_through_artcd_and_doozer_option_parsing(self):
        """Exercise CLI validation and the generated Doozer options without launching remote builds."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.addCleanup(asyncio.set_event_loop, None)
        self.addCleanup(loop.close)
        payload = 'quay.io/example/release@sha256:' + 'b' * 64
        for product in ('openshift_agent_installer', 'oadp', 'cluster-observability-operator', 'quay', 'ocp'):
            for supplied in (False, True):
                with self.subTest(product=product, supplied=supplied), tempfile.TemporaryDirectory() as tmpdir:
                    runtime = Runtime({}, Path(tmpdir), dry_run=True)
                    commands = []

                    async def run_doozer(command, **kwargs):
                        index = command.index('beta:images:konflux:build')
                        with images_konflux_build.make_context(
                            'beta:images:konflux:build', command[index + 1 :]
                        ) as ctx:
                            commands.append(ctx.params)

                    args = [
                        '--group=installer-ove-ui-4.22',
                        '--assembly=test',
                        '--image-build-strategy=all',
                        '--skip-rebase',
                        '--skip-bundle-build',
                        '--ignore-locks',
                        '--kubeconfig=/test/kubeconfig',
                    ]
                    if supplied:
                        args.append('--payload=' + payload)
                    with (
                        patch('pyartcd.pipelines.build_layered_products.tekton.is_tekton_context', return_value=True),
                        patch(
                            'pyartcd.pipelines.build_layered_products.load_group_config',
                            return_value={'product': product, 'version': '4.22'},
                        ),
                        patch(
                            'pyartcd.pipelines.build_layered_products.oc.get_release_image_info',
                            return_value={'metadata': {'version': '4.22.21'}},
                        ),
                        patch(
                            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
                            side_effect=run_doozer,
                        ),
                    ):
                        result = CliRunner().invoke(build_layered_products, args, obj=runtime)
                    accepted = supplied == (product == 'openshift_agent_installer')
                    self.assertEqual(result.exit_code, 0 if accepted else 2, result.output or str(result.exception))
                    if not accepted:
                        self.assertEqual(commands, [])
                        continue
                    self.assertEqual(len(commands), 1)
                    self.assertTrue(commands[0]['dry_run'])
                    if supplied:
                        self.assertIn('RELEASE_VALUE=' + payload, commands[0]['build_args'])
                        self.assertIn('PATCH_VERSION=21', commands[0]['build_args'])
                    else:
                        self.assertEqual(commands[0]['build_args'], ())
