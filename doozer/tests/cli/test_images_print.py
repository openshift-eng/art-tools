import unittest
from unittest.mock import MagicMock

from artcommonlib.model import Model
from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from doozerlib import Runtime
from doozerlib.cli.images import images_print
from doozerlib.image import ImageMetadata


class TestImagesPrintVariantMode(unittest.TestCase):
    def test_effective_enabled_mode(self):
        """Enumerate real metadata using variant overrides unless disabled loading is explicit."""
        cases = [
            (BuildVariant.OKD, 'disabled', 'enabled', False, True),
            (BuildVariant.OKD, 'enabled', 'disabled', False, False),
            (BuildVariant.OKD, 'enabled', None, False, True),
            (BuildVariant.OKD, 'disabled', None, False, False),
            (BuildVariant.OKD, None, None, False, True),
            (BuildVariant.OCP, 'disabled', 'enabled', False, False),
            (BuildVariant.OCP, 'enabled', 'disabled', False, True),
            (BuildVariant.OCP, 'enabled', None, False, True),
            (BuildVariant.OCP, 'disabled', None, False, False),
            (BuildVariant.OKD, 'enabled', 'disabled', True, True),
            (BuildVariant.OKD, 'disabled', None, True, True),
            (BuildVariant.OCP, 'disabled', 'enabled', True, True),
            (BuildVariant.OKD, 'wip', None, False, False),
            (BuildVariant.OKD, 'wip', 'enabled', False, True),
            (BuildVariant.OKD, 'enabled', 'wip', False, False),
            (BuildVariant.OCP, 'wip', 'enabled', False, False),
        ]
        for variant, mode, okd_mode, load_disabled, included in cases:
            with self.subTest(variant=variant, mode=mode, okd_mode=okd_mode, load_disabled=load_disabled):
                runtime = MagicMock(spec=Runtime)
                runtime.variant = variant
                runtime.branch = 'openshift-5.1'
                runtime.assembly = 'stream'
                runtime.assembly_basis_event = None
                runtime.group_config = Model({})
                runtime.get_releases_config.return_value = Model({})
                runtime.load_disabled = load_disabled
                config = {'name': 'openshift/okd-base', 'base_only': True, 'for_release': False}
                if mode is not None:
                    config['mode'] = mode
                if okd_mode is not None:
                    config['okd'] = {'mode': okd_mode}
                image_meta = ImageMetadata(
                    runtime, Model({'key': 'okd-base', 'filename': 'okd-base.yml', 'data': config})
                )
                runtime.image_metas.return_value = [image_meta]

                result = CliRunner().invoke(
                    images_print, ['--short', '--show-base', '--show-non-release', '{distgit_key}'], obj=runtime
                )

                self.assertEqual(result.exit_code, 0, str(result.exception))
                self.assertEqual(result.output.strip(), 'okd-base' if included else '')
                self.assertEqual(image_meta.enabled, mode in (None, 'enabled'))
                runtime.initialize.assert_called_once_with(clone_distgits=False, prevent_cloning=True)
