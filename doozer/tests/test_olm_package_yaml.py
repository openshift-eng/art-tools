import unittest

from doozerlib.olm.package_yaml import format_bundle_channels_label, resolve_package_channels


class TestResolvePackageChannels(unittest.TestCase):
    def test_single_channel_matches_historical_channels0(self):
        package_yaml = {
            'packageName': 'cluster-observability-operator',
            'channels': [
                {'name': 'stable', 'currentCSV': 'cluster-observability-operator.v1.5.4'},
            ],
        }
        resolved = resolve_package_channels(package_yaml)
        self.assertEqual(resolved.package_name, 'cluster-observability-operator')
        self.assertEqual(resolved.primary_channel, 'stable')
        self.assertEqual(resolved.additional_channels, ())
        self.assertEqual(resolved.default_channel, 'stable')
        self.assertEqual(resolved.csv_name, 'cluster-observability-operator.v1.5.4')
        self.assertEqual(resolved.all_channels, ('stable',))

    def test_single_channel_ignores_unknown_default_channel(self):
        package_yaml = {
            'packageName': 'test-package',
            'defaultChannel': 'not-listed',
            'channels': [
                {'name': 'stable', 'currentCSV': 'test.v1.0.0'},
            ],
        }
        resolved = resolve_package_channels(package_yaml)
        self.assertEqual(resolved.primary_channel, 'stable')
        self.assertEqual(resolved.default_channel, 'stable')

    def test_multi_channel_preserves_order_and_default(self):
        package_yaml = {
            'packageName': 'cluster-observability-operator',
            'defaultChannel': 'stable',
            'channels': [
                {'name': 'stable', 'currentCSV': 'cluster-observability-operator.v1.5.4'},
                {'name': 'fast', 'currentCSV': 'cluster-observability-operator.v1.5.4'},
            ],
        }
        resolved = resolve_package_channels(package_yaml)
        self.assertEqual(resolved.primary_channel, 'stable')
        self.assertEqual(resolved.additional_channels, ('fast',))
        self.assertEqual(resolved.default_channel, 'stable')
        self.assertEqual(resolved.all_channels, ('stable', 'fast'))
        self.assertEqual(resolved.csv_name, 'cluster-observability-operator.v1.5.4')

    def test_multi_channel_default_selects_csv(self):
        package_yaml = {
            'packageName': 'test-package',
            'defaultChannel': 'fast',
            'channels': [
                {'name': 'stable', 'currentCSV': 'test.v1.0.0-stable'},
                {'name': 'fast', 'currentCSV': 'test.v1.0.0-fast'},
            ],
        }
        resolved = resolve_package_channels(package_yaml)
        self.assertEqual(resolved.default_channel, 'fast')
        self.assertEqual(resolved.csv_name, 'test.v1.0.0-fast')

    def test_multi_channel_dedupes_duplicate_names(self):
        package_yaml = {
            'packageName': 'test-package',
            'channels': [
                {'name': 'stable', 'currentCSV': 'test.v1.0.0'},
                {'name': 'stable', 'currentCSV': 'test.v1.0.0'},
                {'name': 'fast', 'currentCSV': 'test.v1.0.0'},
            ],
        }
        resolved = resolve_package_channels(package_yaml)
        self.assertEqual(resolved.all_channels, ('stable', 'fast'))

    def test_empty_channels_raises(self):
        with self.assertRaises(ValueError):
            resolve_package_channels({'packageName': 'test-package', 'channels': []})


class TestFormatBundleChannelsLabel(unittest.TestCase):
    def test_single_channel_no_art3107(self):
        channels, default = format_bundle_channels_label('stable')
        self.assertEqual(channels, 'stable')
        self.assertEqual(default, 'stable')

    def test_single_channel_art3107_default_preserves_duplicate_stable(self):
        channels, default = format_bundle_channels_label(
            'stable',
            operator_channel_stable='default',
        )
        self.assertEqual(channels, 'stable,stable')
        self.assertEqual(default, 'stable')

    def test_single_channel_art3107_extra(self):
        channels, default = format_bundle_channels_label(
            'test-channel',
            operator_channel_stable='extra',
        )
        self.assertEqual(channels, 'test-channel,stable')
        self.assertEqual(default, 'test-channel')

    def test_single_channel_ignores_package_default_channel(self):
        # Historical ART used channels[0] as default; a stale defaultChannel must not change that.
        channels, default = format_bundle_channels_label(
            'stable',
            package_default_channel='fast',
        )
        self.assertEqual(channels, 'stable')
        self.assertEqual(default, 'stable')

    def test_multi_channel_coo_style(self):
        channels, default = format_bundle_channels_label(
            'stable',
            ('fast',),
            package_default_channel='stable',
        )
        self.assertEqual(channels, 'stable,fast')
        self.assertEqual(default, 'stable')

    def test_multi_channel_art3107_does_not_duplicate_stable(self):
        channels, default = format_bundle_channels_label(
            'stable',
            ('fast',),
            package_default_channel='stable',
            operator_channel_stable='default',
        )
        self.assertEqual(channels, 'stable,fast')
        self.assertEqual(default, 'stable')

    def test_multi_channel_art3107_appends_missing_stable(self):
        channels, default = format_bundle_channels_label(
            'fast',
            ('candidate',),
            package_default_channel='fast',
            operator_channel_stable='extra',
        )
        self.assertEqual(channels, 'fast,candidate,stable')
        self.assertEqual(default, 'fast')


if __name__ == '__main__':
    unittest.main()
