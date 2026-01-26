from unittest import TestCase

from artcommonlib.build_visibility import (
    BuildVisibility,
    get_all_visibility_suffixes,
    get_build_system,
    get_visibility_suffix,
    is_release_embargoed,
    isolate_pflag_in_release,
)


class TestBuildVisibility(TestCase):
    def test_get_build_system(self):
        self.assertEqual(get_build_system("p0"), "brew")
        self.assertEqual(get_build_system("p1"), "brew")
        self.assertEqual(get_build_system("p2"), "konflux")
        self.assertEqual(get_build_system("p3"), "konflux")
        with self.assertRaises(ValueError):
            get_build_system(".p1")
        with self.assertRaises(ValueError):
            get_build_system("p5")

    def test_is_embargoed(self):
        self.assertFalse(is_release_embargoed("v4.17.0-202503120643.p0", "brew"))
        self.assertTrue(is_release_embargoed("v4.17.0-202503120643.p1", "brew"))
        self.assertFalse(is_release_embargoed("v4.17.0-202503120643.p2", "konflux"))
        self.assertTrue(is_release_embargoed("v4.17.0-202503120643.p3", "konflux"))

        # Build system/suffix mismatch: treat as embargo
        self.assertTrue(is_release_embargoed("v4.17.0-202503120643.p0", "konflux"))
        self.assertTrue(is_release_embargoed("v4.17.0-202503120643.p2", "brew"))

        # Unmatched .p? flag: treat as embargo
        self.assertTrue(is_release_embargoed("v4.17.0-202503120643.p5", "brew"))
        self.assertTrue(is_release_embargoed("v4.17.0-202503120643.p?", "brew"))

        # p? flag not found: treat as embargo
        self.assertTrue(is_release_embargoed("v4.17.0-202503120643", "brew"))

        # Wrong build system: KeyError is raised
        with self.assertRaises(KeyError):
            is_release_embargoed("v4.17.0-202503120643.p0", "wrong")

    def test_get_visibility_suffix(self):
        self.assertEqual(get_visibility_suffix("brew", BuildVisibility.PUBLIC), "p0")
        self.assertEqual(get_visibility_suffix("brew", BuildVisibility.PRIVATE), "p1")
        self.assertEqual(get_visibility_suffix("konflux", BuildVisibility.PUBLIC), "p2")
        self.assertEqual(get_visibility_suffix("konflux", BuildVisibility.PRIVATE), "p3")
        with self.assertRaises(KeyError):
            get_visibility_suffix("invalid", BuildVisibility.PUBLIC)
        with self.assertRaises(KeyError):
            get_visibility_suffix("invalid", BuildVisibility.PRIVATE)

    def test_get_all_visibility_suffixes(self):
        self.assertEqual(get_all_visibility_suffixes(), ["p0", "p1", "p2", "p3"])

    def test_isolate_pflag(self):
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p1"), "p1")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p1.p"), "p1")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p1.el7"), "p1")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p0"), "p0")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p2"), "p2")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p1.assembly.p"), "p1")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p0.assembly.test"), "p0")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p2.assembly.stream"), "p2")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p0.assembly.test.el7"), "p0")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p3"), "p3")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p3.p"), "p3")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p4.p"), None)
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p.p?"), "p?")
        self.assertEqual(isolate_pflag_in_release("1.2.3-y.p?.p4"), "p?")
