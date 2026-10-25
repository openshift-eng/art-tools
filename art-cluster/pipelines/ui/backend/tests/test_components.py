import json
import unittest

from pipeline_ui.components import PREFIX, REPORT, component_details


def run_with(status='Succeeded', *, terminal=False, builds=None, marker=None, **fields):
    entry = {
        'version': 1,
        'type': 'bundle',
        'name': 'operator',
        'displayName': 'operator-bundle',
        'status': status,
        'pipelineRuns': builds or [],
        **fields,
    }
    return {
        'metadata': {
            'annotations': {
                PREFIX + '1': json.dumps(entry),
                REPORT: json.dumps(marker or {'version': 1, 'partial': False}),
            }
        },
        'status': {'completionTime': 'now'} if terminal else {},
    }


class ComponentTests(unittest.TestCase):
    def test_conforma_failure_preserves_native_success(self):
        checks = [
            {
                'kind': 'conforma',
                'name': 'Conforma',
                'blocking': True,
                'status': 'Failed',
                'url': 'https://konflux.example/ec',
                'reason': 'Policy failed',
            }
        ]
        result = component_details(run_with(checks=checks, terminal=True))
        component = result['components'][0]
        self.assertEqual(component['status'], 'Succeeded')
        self.assertEqual(component['resultStatus'], 'Failed')
        self.assertEqual(component['checks'][0]['url'], 'https://konflux.example/ec')
        self.assertTrue(component['checksRecorded'])
        self.assertFalse(result['componentReporting']['partial'])

    def test_optional_failure_does_not_fail_component(self):
        for blocking, expected in ((True, 'Failed'), (False, 'Succeeded')):
            checks = [{'kind': 'custom', 'name': 'qe', 'blocking': blocking, 'status': 'Failed'}]
            component = component_details(run_with(checks=checks))['components'][0]
            self.assertEqual(component['resultStatus'], expected)

    def test_unfinished_blocking_check_prevents_success(self):
        for terminal, expected in ((False, 'Running'), (True, 'Unknown')):
            result = component_details(
                run_with(
                    terminal=terminal,
                    checks=[{'kind': 'conforma', 'name': 'Conforma', 'blocking': True, 'status': 'Running'}],
                )
            )
            self.assertEqual(result['components'][0]['resultStatus'], expected)
            self.assertEqual(result['componentReporting']['partial'], terminal)

    def test_retry_keeps_old_checks_without_failing_recovered_component(self):
        checks = [{'kind': 'conforma', 'name': 'Conforma', 'blocking': True, 'status': 'Succeeded'}]
        failed = [{**checks[0], 'status': 'Failed'}]
        component = component_details(
            run_with(
                checks=checks,
                builds=[
                    {'name': 'first', 'status': 'Succeeded', 'attempt': 1, 'checks': failed},
                    {'name': 'retry', 'status': 'Succeeded', 'attempt': 2, 'checks': checks},
                ],
            )
        )['components'][0]
        self.assertEqual(component['resultStatus'], 'Succeeded')
        self.assertEqual(component['pipelineRuns'][0]['checks'][0]['status'], 'Failed')

    def test_legacy_and_disabled_checks_remain_distinct(self):
        self.assertFalse(component_details(run_with())['components'][0]['checksRecorded'])
        self.assertTrue(component_details(run_with(checks=[]))['components'][0]['checksRecorded'])

    def test_malformed_checks_do_not_break_build_results(self):
        for checks in (None, {}, ['bad'], [{'kind': [], 'name': 'bad'}]):
            result = component_details(run_with(checks=checks))
            self.assertEqual(result['components'][0]['status'], 'Succeeded')
            self.assertEqual(result['components'][0]['resultStatus'], 'Unknown')
            self.assertTrue(result['componentReporting']['partial'])

    def test_invalid_check_links_are_removed(self):
        component = component_details(
            run_with(
                checks=[
                    {
                        'kind': 'custom',
                        'name': 'qe',
                        'blocking': False,
                        'status': 'Failed',
                        'url': 'javascript:alert(1)',
                    }
                ]
            )
        )['components'][0]
        self.assertIsNone(component['checks'][0]['url'])

    def test_old_runs_have_no_report(self):
        result = component_details({})
        self.assertEqual(result['components'], [])
        self.assertFalse(result['componentReporting']['available'])

    def test_successful_and_failed_builds_include_links(self):
        for status in ('Succeeded', 'Failed'):
            component = component_details(
                run_with(status, builds=[{'name': 'build', 'status': status, 'url': 'https://konflux.example/run'}])
            )['components'][0]
            self.assertEqual(component['status'], status)
            self.assertEqual(component['pipelineRuns'][0]['url'], 'https://konflux.example/run')

    def test_recovered_retry_preserves_both_attempts(self):
        builds = [
            {'name': 'first', 'status': 'Failed', 'attempt': 1},
            {'name': 'retry', 'status': 'Succeeded', 'attempt': 2},
        ]
        component = component_details(run_with(builds=builds))['components'][0]
        self.assertEqual(component['status'], 'Succeeded')
        self.assertEqual([b['status'] for b in component['pipelineRuns']], ['Failed', 'Succeeded'])

    def test_active_build_stays_running(self):
        result = component_details(run_with('Running'))
        self.assertEqual(result['components'][0]['status'], 'Running')
        self.assertFalse(result['componentReporting']['partial'])

    def test_unfinished_build_unknown_after_completion(self):
        result = component_details(run_with('Running', terminal=True, builds=[{'name': 'build', 'status': 'Running'}]))
        self.assertEqual(result['components'][0]['status'], 'Unknown')
        self.assertEqual(result['components'][0]['pipelineRuns'][0]['status'], 'Unknown')
        self.assertTrue(result['componentReporting']['partial'])

    def test_terminal_condition_without_completion_time(self):
        run = run_with('Pending')
        run['status'] = {'conditions': [{'type': 'Succeeded', 'status': 'False'}]}
        self.assertEqual(component_details(run)['components'][0]['status'], 'Unknown')

    def test_no_build_status_inferred_from_parent_failure(self):
        run = run_with('Succeeded')
        run['status'] = {'conditions': [{'type': 'Succeeded', 'status': 'False'}]}
        self.assertEqual(component_details(run)['components'][0]['status'], 'Succeeded')

    def test_reused_skipped_and_dry_runs(self):
        for status in ('Reused', 'Skipped', 'DryRun'):
            component = component_details(run_with(status))['components'][0]
            self.assertEqual(component['status'], status)
            self.assertEqual(component['pipelineRuns'], [])

    def test_unknown_terminal_result_is_incomplete(self):
        self.assertTrue(component_details(run_with('Unknown', terminal=True))['componentReporting']['partial'])

    def test_malformed_marker_is_incomplete(self):
        run = run_with()
        run['metadata']['annotations'][REPORT] = 'invalid'
        self.assertTrue(component_details(run)['componentReporting']['partial'])

    def test_bad_annotation_keeps_valid_components(self):
        run = run_with()
        run['metadata']['annotations'][PREFIX + 'bad'] = 'invalid'
        result = component_details(run)
        self.assertEqual(len(result['components']), 1)
        self.assertTrue(result['componentReporting']['partial'])

    def test_unsupported_schema_is_incomplete(self):
        run = run_with(version=2)
        result = component_details(run)
        self.assertEqual(result['components'], [])
        self.assertTrue(result['componentReporting']['partial'])

    def test_invalid_urls_are_not_links(self):
        component = component_details(
            run_with(
                builds=[
                    {'name': 'bad', 'url': 'javascript:alert(1)'},
                    {'name': 'good', 'url': 'https://konflux.example/run'},
                ]
            )
        )['components'][0]
        self.assertIsNone(component['pipelineRuns'][0]['url'])
        self.assertEqual(component['pipelineRuns'][1]['url'], 'https://konflux.example/run')

    def test_capacity_marker_visible(self):
        result = component_details(
            run_with(marker={'version': 1, 'partial': True, 'reason': 'Annotation capacity exceeded'})
        )
        self.assertTrue(result['componentReporting']['partial'])
        self.assertEqual(result['componentReporting']['reason'], 'Annotation capacity exceeded')

    def test_no_check_or_release_data_returned(self):
        component = component_details(
            run_with(stages={'release': {'status': 'Failed'}}, resources=[{'role': 'ITS', 'name': 'test'}])
        )['components'][0]
        self.assertEqual(component['status'], 'Succeeded')
        self.assertNotIn('stages', component)
        self.assertNotIn('resources', component)

    def test_malformed_component_type_is_ignored(self):
        self.assertEqual(component_details(run_with(type=[]))['components'], [])
