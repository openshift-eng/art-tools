"""Check the GitOps pipeline contract without a cluster or external builds."""

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

BASE = Path(__file__).resolve().parents[2] / 'art-cluster/pipelines/config/argocd/project/art-quay-tenant'
TASK = yaml.safe_load((BASE / '02_task-artcd-merge-layered-fbc.yaml').read_text())
PIPELINE = yaml.safe_load((BASE / '23_pipeline-merge-layered-fbc.yaml').read_text())


class TestMergeLayeredFbcManifests(unittest.TestCase):
    def test_pipeline_task_parameter_and_result_contract(self):
        task_params = {param['name']: param['type'] for param in TASK['spec']['params']}
        pipeline_params = {param['name']: param['type'] for param in PIPELINE['spec']['params']}
        self.assertEqual(task_params, pipeline_params)
        invocation = PIPELINE['spec']['tasks'][0]
        self.assertEqual(invocation['taskRef']['name'], TASK['metadata']['name'])
        self.assertEqual({param['name'] for param in invocation['params']}, set(task_params))
        results = {result['name'] for result in TASK['spec']['results']}
        self.assertEqual(results, {result['name'] for result in PIPELINE['spec']['results']})
        for result in PIPELINE['spec']['results']:
            self.assertEqual(result['value'], '$(tasks.merge-fbc.results.' + result['name'] + ')')
        for resource in (TASK, PIPELINE):
            defaults = {param['name']: param.get('default') for param in resource['spec']['params']}
            self.assertEqual(defaults['dry-run'], 'true')

    def test_team_can_start_and_read_new_pipeline(self):
        role = yaml.safe_load((BASE / '05_role-release-from-fbc-operator.yaml').read_text())
        pipeline_rule = next(rule for rule in role['rules'] if rule['resources'] == ['pipelines'])
        task_rule = next(rule for rule in role['rules'] if rule['resources'] == ['tasks'])
        self.assertIn(PIPELINE['metadata']['name'], pipeline_rule['resourceNames'])
        self.assertIn(TASK['metadata']['name'], task_rule['resourceNames'])
        resources = yaml.safe_load_all((BASE / '22_quay-team-pipelinerun-admission.yaml').read_text())
        policy = next(resource for resource in resources if resource['kind'] == 'ValidatingAdmissionPolicy')
        self.assertIn('"merge-layered-fbc"', policy['spec']['validations'][0]['expression'])

    def _invoke_script(self, dry_run):
        env = {key: '' for key in ('MERGE_FBC_REPO', 'MERGE_IMAGE_REPO', 'MERGE_PLR_TEMPLATE')}
        env.update(
            {
                'MERGE_OCP_TARGET_VERSION': '5.0',
                'MERGE_DATA_PATH': 'https://github.com/openshift-eng/ocp-build-data',
                'MERGE_DRY_RUN': 'true' if dry_run else 'false',
                'MERGE_SKIP_CHECKS': 'false',
                'MERGE_SKIP_TASKS': 'clair-scan, sast-snyk-check',
                'MERGE_TASKRUN': 'merge-test',
                'MERGE_RESULT_NVR': '/tekton/results/nvr',
                'MERGE_RESULT_PULLSPEC': '/tekton/results/pullspec',
                'MERGE_RESULT_BUILD_URL': '/tekton/results/build',
                'MERGE_RESULT_PROVENANCE_URL': '/tekton/results/provenance',
            }
        )
        inputs = ['quay-fbc-3.18.2-1.ocp5.0', 'quay.io/example/fbc:tag-with-"quotes"-$(text)']
        result = {
            'nvr': 'quay-merged-fbc-5.0-1.ocp5.0',
            'pullspec': None if dry_run else 'quay.io/example/fbc@sha256:abc',
            'build_url': None if dry_run else 'https://konflux.example/build',
            'provenance_url': None if dry_run else 'https://github.com/example/fbc/blob/sha/merge-provenance.json',
        }
        with (
            patch.dict(os.environ, env),
            patch.object(sys, 'argv', ['task.py', *inputs]),
            patch('subprocess.run') as subprocess_run,
            patch.object(Path, 'mkdir'),
            patch.object(Path, 'touch'),
            patch.object(Path, 'read_text', return_value=json.dumps(result)),
            patch.object(Path, 'write_text') as write,
            patch('builtins.print'),
        ):
            exec(compile(TASK['spec']['steps'][0]['script'], '<merge-layered-fbc-task>', 'exec'), {})
        cmd = subprocess_run.call_args_list[1].args[0]
        self.assertIn('merge-layered-fbc', cmd)
        self.assertIn('--product=quay', cmd)
        self.assertFalse(any(arg.startswith('--group') for arg in cmd))
        self.assertIn('--ocp-target-version=5.0', cmd)
        for value in inputs:
            self.assertIn('--fbc-build=' + value, cmd)
        self.assertIn('--skip-task=clair-scan', cmd)
        self.assertIn('--skip-task=sast-snyk-check', cmd)
        self.assertEqual('--dry-run' in cmd, dry_run)
        self.assertEqual(
            [call.args[0] for call in write.call_args_list],
            [
                result['nvr'],
                result['pullspec'] or '',
                result['build_url'] or '',
                result['provenance_url'] or '',
            ],
        )

    def test_script_passes_array_inputs_and_writes_build_results(self):
        self._invoke_script(False)

    def test_script_reports_empty_build_outputs_for_dry_run(self):
        self._invoke_script(True)
