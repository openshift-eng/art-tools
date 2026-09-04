from unittest import TestCase

from artcommonlib.build_visibility import (
    BuildVisibility,
    find_all_pflags_in_release,
    get_all_visibility_suffixes,
    get_build_system,
    get_visibility_suffix,
    is_nvr_embargoed,
    is_release_embargoed,
    isolate_pflag_in_release,
)


class TestBuildVisibility(TestCase):
    def test_get_build_system(self):
        self.assertEqual(get_build_system('p0'), 'brew')
        self.assertEqual(get_build_system('p1'), 'brew')
        self.assertEqual(get_build_system('p2'), 'konflux')
        self.assertEqual(get_build_system('p3'), 'konflux')
        with self.assertRaises(ValueError):
            get_build_system('.p1')
        with self.assertRaises(ValueError):
            get_build_system('p5')

    def test_is_embargoed(self):
        self.assertFalse(is_release_embargoed('v4.17.0-202503120643.p0', 'brew'))
        self.assertTrue(is_release_embargoed('v4.17.0-202503120643.p1', 'brew'))
        self.assertFalse(is_release_embargoed('v4.17.0-202503120643.p2', 'konflux'))
        self.assertTrue(is_release_embargoed('v4.17.0-202503120643.p3', 'konflux'))

        # Build system/suffix mismatch: treat as embargo
        self.assertTrue(is_release_embargoed('v4.17.0-202503120643.p0', 'konflux'))
        self.assertTrue(is_release_embargoed('v4.17.0-202503120643.p2', 'brew'))

        # Unmatched .p? flag: treat as embargo
        self.assertTrue(is_release_embargoed('v4.17.0-202503120643.p5', 'brew'))
        self.assertTrue(is_release_embargoed('v4.17.0-202503120643.p?', 'brew'))

        # p? flag not found: treat as embargo
        self.assertTrue(is_release_embargoed('v4.17.0-202503120643', 'brew'))

        # Wrong build system: KeyError is raised
        with self.assertRaises(KeyError):
            is_release_embargoed('v4.17.0-202503120643.p0', 'wrong')

    def test_is_nvr_embargoed(self):
        # Konflux (default build_system)
        self.assertFalse(is_nvr_embargoed('foo-1.2.3-1.p2'))
        self.assertTrue(is_nvr_embargoed('foo-1.2.3-1.p3'))

        # Explicit konflux build_system
        self.assertFalse(is_nvr_embargoed('foo-1.2.3-1.p2', 'konflux'))
        self.assertTrue(is_nvr_embargoed('foo-1.2.3-1.p3', 'konflux'))

        # Brew build_system
        self.assertFalse(is_nvr_embargoed('foo-1.2.3-1.p0', 'brew'))
        self.assertTrue(is_nvr_embargoed('foo-1.2.3-1.p1', 'brew'))

        # No p? flag found anywhere in the NVR: treat as embargoed
        self.assertTrue(is_nvr_embargoed('foo-1.2.3-1'))

        # Malformed NVR (no name/version/release separators): no p? flag found, so also
        # defaults to embargoed. is_nvr_embargoed() searches the whole string for a p-flag
        # pattern rather than parsing name/version/release, so it never raises here.
        self.assertTrue(is_nvr_embargoed('not_an_nvr'))

        # Bundle ("metadata-container") style NVRs embed the referenced operator's p-flag in
        # the *version* segment (bundle_version = f'{operator_version}.{operator_release}'),
        # not in the release segment, which is just a trivial build counter. Searching the
        # whole NVR must still find it.
        self.assertFalse(
            is_nvr_embargoed(
                'openshift-migration-operator-metadata-container-1.8.16.202607131859.p2.gafc12a7.assembly.stream.el8-1'
            )
        )
        self.assertTrue(
            is_nvr_embargoed(
                'openshift-migration-operator-metadata-container-1.8.16.202607131859.p3.gafc12a7.assembly.stream.el8-1'
            )
        )

        # Two distinct visibility-suffix matches in the same NVR: ambiguous, so refuse to
        # guess which one applies and raise rather than silently trusting whichever comes first.
        with self.assertRaises(ValueError):
            is_nvr_embargoed('foo-1.0.p2.gabc123-1.p3')

        # Even two matches of the *same* suffix are still treated as ambiguous: an NVR is
        # never expected to carry a p-flag twice, so seeing it twice signals something
        # unexpected about the NVR's shape that's worth investigating rather than papering over.
        with self.assertRaises(ValueError):
            is_nvr_embargoed('foo-1.0.p2.gabc123-1.p2')

    def test_get_visibility_suffix(self):
        self.assertEqual(get_visibility_suffix('brew', BuildVisibility.PUBLIC), 'p0')
        self.assertEqual(get_visibility_suffix('brew', BuildVisibility.PRIVATE), 'p1')
        self.assertEqual(get_visibility_suffix('konflux', BuildVisibility.PUBLIC), 'p2')
        self.assertEqual(get_visibility_suffix('konflux', BuildVisibility.PRIVATE), 'p3')
        with self.assertRaises(KeyError):
            get_visibility_suffix('invalid', BuildVisibility.PUBLIC)
        with self.assertRaises(KeyError):
            get_visibility_suffix('invalid', BuildVisibility.PRIVATE)

    def test_get_all_visibility_suffixes(self):
        self.assertEqual(get_all_visibility_suffixes(), ['p0', 'p1', 'p2', 'p3'])

    def test_isolate_pflag(self):
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p1'), 'p1')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p1.p'), 'p1')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p1.el7'), 'p1')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p0'), 'p0')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p2'), 'p2')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p1.assembly.p'), 'p1')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p0.assembly.test'), 'p0')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p2.assembly.stream'), 'p2')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p0.assembly.test.el7'), 'p0')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p3'), 'p3')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p3.p'), 'p3')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p4.p'), None)
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p.p?'), 'p?')
        self.assertEqual(isolate_pflag_in_release('1.2.3-y.p?.p4'), 'p?')

    def test_find_all_pflags_in_release(self):
        self.assertEqual(find_all_pflags_in_release('1.2.3-y.p.p1'), ['p1'])
        self.assertEqual(find_all_pflags_in_release('1.2.3-y.p4.p'), [])
        self.assertEqual(find_all_pflags_in_release('foo-1.0.p2.gabc123-1.p3'), ['p2', 'p3'])
        self.assertEqual(find_all_pflags_in_release('foo-1.0.p2.gabc123-1.p2'), ['p2', 'p2'])
