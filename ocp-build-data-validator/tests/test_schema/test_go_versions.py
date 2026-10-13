import tempfile
import unittest
from pathlib import Path

from ruamel.yaml import YAML
from validator.schema import image_schema


class TestGoVersionConsistency(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.images = self.root / 'images'
        self.images.mkdir()
        self._write('group.yml', {'vars': {'GO_LATEST': '1.25', 'GO_EXTRA': '1.26'}})
        self.streams = {
            'rhel-9-golang': {
                'aliases': ['rhel-9-golang-{GO_LATEST}'],
                'image': 'registry.redhat.io/openshift/golang-builder:openshift-golang-builder-container-v1.25.14',
                'upstream_image': 'registry.ci.openshift.org/ocp/builder:rhel-9-golang-{GO_LATEST}',
            },
            'rhel-9-golang-1.26': {
                'aliases': ['rhel-9-golang-{GO_EXTRA}'],
                'image': 'quay.io/art/golang-builder:golang-builder-v1.26.7',
                'upstream_image': 'registry.ci.openshift.org/ocp/builder:rhel-9-golang-{GO_EXTRA}',
            },
            'rhel9': {'image': 'registry.redhat.io/ubi9:9.7'},
        }
        self._write('streams.yml', self.streams)
        for kind, variable in [('latest', 'GO_LATEST'), ('extra', 'GO_EXTRA')]:
            self._write(
                f'images/go-{kind}.yml',
                {'from': {'stream': f'rhel-9-golang-{{{variable}}}'}},
            )
            self._write(
                f'images/root-{kind}.yml',
                {
                    'from': {'member': f'go-{kind}'},
                    'content': {
                        'source': {
                            'ci_alignment': {
                                'upstream_image': f'registry.ci.openshift.org/openshift/release:rhel-9-release-golang-{{{variable}}}'
                            }
                        }
                    },
                },
            )
        self.data = {
            'name': 'component',
            'for_release': False,
            'from': {'builder': [{'stream': 'rhel-9-golang'}]},
            'content': {'source': {'ci_alignment': {'streams_prs': {'ci_build_root': {'member': 'root-latest'}}}}},
        }

    def _write(self, filename, data):
        with (self.root / filename).open('w') as destination:
            YAML().dump(data, destination)

    def _root(self, reference):
        self.data['content']['source']['ci_alignment']['streams_prs']['ci_build_root'] = reference

    def _validate(self):
        return image_schema.validate(str(self.images / 'component.yml'), self.data, images_dir=str(self.images))

    def test_matching_latest_and_aliases(self):
        for stream in ['rhel-9-golang', 'rhel-9-golang-1.25', 'rhel-9-golang-{GO_LATEST}']:
            with self.subTest(stream=stream):
                self.data['from']['builder'] = [{'stream': stream}]
                self.assertIsNone(self._validate())

    def test_mismatched_extra_builder_and_latest_root(self):
        self.data['from']['builder'] = [{'stream': 'rhel-9-golang-{GO_EXTRA}'}]
        error = self._validate()
        self.assertIn('component: Go version mismatch: from.builder uses 1.26', error)
        self.assertIn('ci_build_root uses 1.25', error)

    def test_matching_extra_member_chain(self):
        self.data['from']['builder'] = [{'member': 'go-extra'}]
        self._root({'member': 'root-extra'})
        self.assertIsNone(self._validate())

    def test_root_member_inherits_version_without_upstream_tag(self):
        self._write('images/root-latest.yml', {'from': {'member': 'go-latest'}})
        self.assertIsNone(self._validate())
        self.data['from']['builder'] = [{'member': 'go-extra'}]
        self.assertIn('Go version mismatch', self._validate())

    def test_root_stream_reference(self):
        self._root({'stream': 'rhel-9-golang-{GO_EXTRA}'})
        self.assertIn('ci_build_root uses 1.26', self._validate())

    def test_pinned_builder_version_takes_precedence_over_stream_name(self):
        self.streams['rhel-9-golang-1.26']['image'] = self.streams['rhel-9-golang']['image']
        self._write('streams.yml', self.streams)
        self.data['from']['builder'] = [{'stream': 'rhel-9-golang-1.26'}]
        self._root({'member': 'root-extra'})
        self.assertIn('from.builder uses 1.25', self._validate())

    def test_ci_root_uses_published_upstream_version(self):
        self._write(
            'images/root-latest.yml',
            {
                'from': {'member': 'go-latest'},
                'content': {
                    'source': {'ci_alignment': {'upstream_image': 'registry.ci.openshift.org/ocp/root:golang-1.26'}}
                },
            },
        )
        self.assertIn('ci_build_root uses 1.26', self._validate())

    def test_digest_only_builder_uses_declared_upstream_version(self):
        self.streams['rhel-9-golang']['image'] = 'registry.example.com/golang-builder@sha256:abcd'
        self._write('streams.yml', self.streams)
        self._root({'member': 'root-extra'})
        self.assertIn('from.builder uses 1.25', self._validate())

    def test_patch_versions_do_not_need_to_match(self):
        self.data['from']['builder'] = [{'image': 'registry.example.com/golang:v1.25.1'}]
        self._root({'stream': 'rhel-9-golang'})
        self.assertIsNone(self._validate())

    def test_checks_all_go_builder_stages(self):
        self.data['from']['builder'] = [{'stream': 'rhel9'}, {'member': 'go-latest'}, {'member': 'go-extra'}]
        self.assertIn('from.builder uses 1.26', self._validate())

    def test_non_go_and_unversioned_roots_are_not_rejected(self):
        self.data['from']['builder'] = [{'stream': 'rhel9'}]
        self._root({'member': 'not-needed'})
        self.assertIsNone(self._validate())
        self.data['from']['builder'] = [{'member': 'go-extra'}]
        self._root({'stream': 'rhel9'})
        self.assertIsNone(self._validate())

    def test_member_build_stages_do_not_establish_its_toolchain(self):
        self._write(
            'images/tools.yml',
            {'from': {'stream': 'rhel9', 'builder': [{'stream': 'rhel-9-golang-1.26'}]}},
        )
        self.data['from']['builder'] = [{'member': 'tools'}]
        self.assertIsNone(self._validate())

    def test_no_root_or_builders(self):
        del self.data['content']
        self.assertIsNone(self._validate())
        self.data['content'] = {'source': {'ci_alignment': {'streams_prs': {'ci_build_root': {'member': 'missing'}}}}}
        self.data['from'] = {}
        self.assertIsNone(self._validate())

    def test_unresolvable_ci_root_reports_an_error(self):
        for reference in [{'member': 'missing'}, {'stream': 'missing'}]:
            with self.subTest(reference=reference):
                self._root(reference)
                self.assertIn('component: cannot validate Go version consistency: Cannot resolve', self._validate())

    def test_ambiguous_stream_alias_reports_an_error(self):
        self.streams['duplicate'] = dict(self.streams['rhel-9-golang'])
        self._write('streams.yml', self.streams)
        self.data['from']['builder'] = [{'stream': 'rhel-9-golang-1.25'}]
        self.assertIn('expected one definition, found 2', self._validate())

    def test_circular_member_chain_reports_an_error(self):
        self._write('images/root-latest.yml', {'from': {'member': 'root-latest'}})
        self.assertIn('circular from.member reference', self._validate())

    def test_missing_go_variable_reports_an_error(self):
        self._write('group.yml', {'vars': {'GO_LATEST': '1.25'}})
        self._root({'member': 'root-extra'})
        self.assertIn('missing group variable', self._validate())

    def test_invalid_member_yaml_reports_a_validation_error(self):
        for content in ['not a mapping', 'from: [']:
            with self.subTest(content=content):
                (self.images / 'root-latest.yml').write_text(content)
                self.assertIn('component: cannot validate Go version consistency:', self._validate())

    def test_invalid_schema_does_not_run_version_resolution(self):
        self.data['from']['builder'] = [{'stream': 123}]
        self.assertIn("123 is not of type 'string'", self._validate())
