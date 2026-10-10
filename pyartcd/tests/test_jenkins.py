import os
import unittest
from unittest import mock

from pyartcd.jenkins import Jobs

from pyartcd import jenkins


class TestJenkinsStartBuild(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            mock.patch.dict(
                os.environ,
                {
                    'BUILD_URL': 'https://jenkins/job/parent/1/',
                    'JOB_NAME': 'parent',
                    'JENKINS_URL': 'https://jenkins',
                },
                clear=True,
            )
        )
        jenkins.current_build_url = None
        jenkins.current_job_name = None

    def tearDown(self):
        jenkins.current_build_url = None
        jenkins.current_job_name = None

    @mock.patch("pyartcd.jenkins.init_jenkins")
    @mock.patch("pyartcd.jenkins.jenkins_client")
    def test_start_build_dont_block(self, mock_client, mock_init_jenkins):
        job = Jobs.OCP4
        params = {"param1": "value1", "param2": "value2"}
        mock_job = mock.MagicMock()
        mock_client.get_job.return_value = mock_job
        jenkins.start_build(job, params, block_until_building=False)

        mock_init_jenkins.assert_called_once()
        mock_client.get_job.assert_called_once_with(job.value)
        mock_job.invoke.assert_called_once_with(build_params=params)

    @mock.patch("pyartcd.jenkins.Build")
    @mock.patch("pyartcd.jenkins.init_jenkins")
    @mock.patch("pyartcd.jenkins.jenkins_client")
    def test_start_build_block_until_building(self, mock_client, mock_init_jenkins, mock_build):
        job = Jobs.OCP4
        params = {"param1": "value1", "param2": "value2"}
        delay = 10
        mock_client.get_job.return_value = mock_job = mock.MagicMock()
        mock_job.invoke.return_value = mock_queue_item = mock.MagicMock()
        mock_queue_item.poll.return_value = {'executable': {'number': 1}, 'task': {'url': 'folder/foo/'}}
        triggered_url = 'folder/foo/1'
        os.environ['BUILD_URL'] = 'folder/bar/1'
        os.environ['JOB_NAME'] = 'bar'
        os.environ['JENKINS_URL'] = 'buildvm.com'

        result = jenkins.start_build(job, params, block_until_building=True, watch_building_delay=delay)
        self.assertEqual(result, None)

        mock_init_jenkins.assert_called_once()
        mock_client.get_job.assert_called_once_with(job.value)
        mock_job.invoke.assert_called_once_with(build_params=params)
        mock_queue_item.poll.assert_called_once()
        mock_build.assert_called_once_with(url=triggered_url, buildno=1, job=mock_job)

    @mock.patch("pyartcd.jenkins.Build")
    @mock.patch("pyartcd.jenkins.init_jenkins")
    @mock.patch("pyartcd.jenkins.jenkins_client")
    def test_start_build_block_until_complete(self, mock_client, mock_init_jenkins, mock_build):
        job = Jobs.OCP4
        params = {"param1": "value1", "param2": "value2"}
        delay = 10
        mock_client.get_job.return_value = mock_job = mock.MagicMock()
        mock_job.invoke.return_value = mock_queue_item = mock.MagicMock()
        mock_queue_item.poll.return_value = {'executable': {'number': 1}, 'task': {'url': 'folder/foo/'}}
        triggered_url = 'folder/foo/1'
        os.environ['BUILD_URL'] = 'folder/bar/1'
        os.environ['JOB_NAME'] = 'bar'
        os.environ['JENKINS_URL'] = 'buildvm.com'
        mock_build.return_value.poll.return_value = {'result': 'SUCCESS'}

        result = jenkins.start_build(
            job, params, block_until_building=True, block_until_complete=True, watch_building_delay=delay
        )
        self.assertEqual(result, 'SUCCESS')

        mock_init_jenkins.assert_called_once()
        mock_client.get_job.assert_called_once_with(job.value)
        mock_job.invoke.assert_called_once_with(build_params=params)
        mock_queue_item.poll.assert_called_once()
        mock_build.assert_called_once_with(url=triggered_url, buildno=1, job=mock_job)

    @mock.patch("pyartcd.jenkins.Build")
    @mock.patch("pyartcd.jenkins.init_jenkins")
    @mock.patch("pyartcd.jenkins.jenkins_client")
    @mock.patch.dict(
        os.environ,
        {
            'BUILD_URL': 'https://jenkins/job/parent/1/',
            'JOB_NAME': 'parent',
            'JENKINS_URL': 'https://jenkins',
        },
        clear=False,
    )
    def test_start_build_can_return_build_url(self, mock_client, mock_init_jenkins, mock_build):
        job = Jobs.OCP4
        mock_job = mock.MagicMock()
        mock_client.get_job.return_value = mock_job
        mock_queue_item = mock.MagicMock()
        mock_job.invoke.return_value = mock_queue_item
        mock_queue_item.poll.return_value = {'executable': {'number': 106}, 'task': {'url': 'https://jenkins/job/1/'}}
        mock_build.return_value.baseurl = 'https://jenkins/job/1/106'
        mock_build.return_value.poll.return_value = {'result': 'SUCCESS'}
        result = jenkins.start_build(job, {}, block_until_complete=True, return_build_url=True)

        self.assertEqual(result, ('SUCCESS', 'https://jenkins/job/1/106'))
        mock_init_jenkins.assert_called_once()

    def test_get_build_url_and_path(self):
        # No BUILD_URL env var defined
        if os.environ.get('BUILD_URL'):
            del os.environ['BUILD_URL']
        self.assertEqual(jenkins.get_build_url(), None)
        self.assertEqual(jenkins.get_build_path(), None)

        # Trailing slash will be removed
        os.environ['BUILD_URL'] = (
            'https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com/'
            'job/aos-cd-builds/job/build%252Focp4/46870/'
        )
        self.assertEqual(
            jenkins.get_build_url(),
            'https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com/'
            'job/aos-cd-builds/job/build%252Focp4/46870',
        )

        # Build path
        build_path = jenkins.get_build_path()
        self.assertEqual(build_path, 'job/aos-cd-builds/job/build%252Focp4/46870')

    def test_get_build_id_from_url(self):
        build_url = (
            'https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com/'
            'job/aos-cd-builds/job/build%252Focp4/46870/'
        )
        self.assertEqual(jenkins.get_build_id_from_url(build_url), 46870)

        build_url = (
            'https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com/'
            'job/aos-cd-builds/job/build%252Focp4/46870'
        )
        self.assertEqual(jenkins.get_build_id_from_url(build_url), 46870)

    @mock.patch("pyartcd.jenkins.start_build")
    def test_start_build_conforma_verify_uses_group_parameter(self, start_build_mock):
        jenkins.start_build_conforma_verify(group="oadp-1.5")

        params = start_build_mock.call_args.kwargs["params"]
        self.assertEqual(start_build_mock.call_args.kwargs["job"], Jobs.BUILD_CONFORMA_VERIFY)
        self.assertEqual(params["GROUP"], "oadp-1.5")
        self.assertNotIn("BUILD_VERSION", params)

    @mock.patch("pyartcd.jenkins.start_build")
    def test_start_olm_bundle_konflux_without_force_release(self, start_build_mock):
        jenkins.start_olm_bundle_konflux(
            build_version='4.18',
            assembly='stream',
            operator_nvrs=['op-1-1', 'op-2-1'],
            group='openshift-4.18',
        )

        params = start_build_mock.call_args.kwargs["params"]
        self.assertEqual(start_build_mock.call_args.kwargs["job"], Jobs.OLM_BUNDLE_KONFLUX)
        self.assertNotIn('FORCE_RELEASE', params)
        self.assertEqual(params['OPERATOR_NVRS'], 'op-1-1,op-2-1')
        self.assertEqual(params['GROUP'], 'openshift-4.18')

    @mock.patch("pyartcd.jenkins.start_build")
    def test_start_olm_bundle_konflux_with_force_release(self, start_build_mock):
        jenkins.start_olm_bundle_konflux(
            build_version='4.18',
            assembly='stream',
            operator_nvrs=['op-1-1'],
            group='openshift-4.18',
            force_release=True,
        )

        params = start_build_mock.call_args.kwargs["params"]
        self.assertEqual(start_build_mock.call_args.kwargs["job"], Jobs.OLM_BUNDLE_KONFLUX)
        self.assertEqual(params['FORCE_RELEASE'], 'true')
        self.assertEqual(params['OPERATOR_NVRS'], 'op-1-1')

    @mock.patch("pyartcd.jenkins.start_build")
    def test_start_olm_bundle_konflux_force_release_false_no_param(self, start_build_mock):
        jenkins.start_olm_bundle_konflux(
            build_version='4.18',
            assembly='stream',
            operator_nvrs=['op-1-1'],
            force_release=False,
        )

        params = start_build_mock.call_args.kwargs["params"]
        self.assertNotIn('FORCE_RELEASE', params)

    def test_start_olm_bundle_konflux_empty_nvrs_returns_none(self):
        result = jenkins.start_olm_bundle_konflux(
            build_version='4.18',
            assembly='stream',
            operator_nvrs=[],
        )
        self.assertIsNone(result)


class TestTektonJenkinsStartBuild(unittest.TestCase):
    PARENT_URL = (
        'https://console-openshift-console.apps.artc2023.pc3z.p1.openshiftapps.com/'
        'k8s/ns/art-openshift-tenant/tekton.dev~v1~PipelineRun/promote-assembly-xyz'
    )
    CHILD_URL = 'https://jenkins/job/child/42'

    def setUp(self):
        self.enterContext(
            mock.patch.dict(
                os.environ,
                {
                    'TASKRUN_NAME': 'promote-assembly-xyz-task',
                    'TEKTON_PIPELINERUN_NAME': 'promote-assembly-xyz',
                    'BUILD_URL': self.PARENT_URL,
                    'JENKINS_URL': 'https://jenkins',
                },
                clear=True,
            )
        )
        self.enterContext(mock.patch('pyartcd.jenkins.current_build_url', None))
        self.enterContext(mock.patch('pyartcd.jenkins.current_job_name', None))
        self.init_jenkins = self.enterContext(mock.patch('pyartcd.jenkins.init_jenkins'))
        self.client = self.enterContext(mock.patch('pyartcd.jenkins.jenkins_client'))
        self.build_class = self.enterContext(mock.patch('pyartcd.jenkins.Build'))
        self.set_description = self.enterContext(mock.patch('pyartcd.jenkins.set_build_description'))
        self.job = self.client.get_job.return_value
        self.queue_item = self.job.invoke.return_value
        self.queue_item.poll.return_value = {
            'executable': {'number': 42},
            'task': {'url': 'https://jenkins/job/child/'},
        }
        self.build = self.build_class.return_value
        self.build.baseurl = self.CHILD_URL
        self.build.poll.return_value = {'result': 'SUCCESS'}

    def test_queue_without_jenkins_parent_variables(self):
        del os.environ['BUILD_URL']
        params = {'BUILD_VERSION': '4.20', 'ASSEMBLY': '4.20.42', 'DRY_RUN': True}

        result = jenkins.start_build(Jobs.BUILD_MICROSHIFT, params, block_until_building=False)

        self.assertIsNone(result)
        self.init_jenkins.assert_called_once_with()
        self.client.get_job.assert_called_once_with(Jobs.BUILD_MICROSHIFT.value)
        self.job.invoke.assert_called_once_with(build_params=params)
        self.queue_item.poll.assert_not_called()
        self.build_class.assert_not_called()
        self.set_description.assert_not_called()

    def test_wait_until_building_links_to_pipelinerun(self):
        params = {'RELEASE_TAG': '4.20.42-x86_64', 'DRY_RUN': True, 'SIGN_ONLY': True}

        result = jenkins.start_build(Jobs.RHCOS_SYNC, params)

        self.assertIsNone(result)
        self.job.invoke.assert_called_once_with(build_params=params)
        self.build_class.assert_called_once_with(url=self.CHILD_URL, buildno=42, job=self.job)
        self.set_description.assert_called_once_with(
            self.build,
            f'Started by upstream Tekton PipelineRun <a href="{self.PARENT_URL}">promote-assembly-xyz</a><br><br>',
        )
        self.build.block_until_complete.assert_not_called()

    def test_wait_until_building_without_console_url(self):
        del os.environ['BUILD_URL']

        result = jenkins.wait_until_building(self.queue_item, self.job)

        self.assertIs(result, self.build)
        self.set_description.assert_called_once_with(
            self.build, 'Started by upstream Tekton PipelineRun <b>promote-assembly-xyz</b><br><br>'
        )

    def test_tekton_ignores_cached_jenkins_parent(self):
        jenkins.current_build_url = 'https://jenkins/job/old-parent/7'
        jenkins.current_job_name = 'old-parent'

        jenkins.start_build(Jobs.RHCOS_SYNC, {})

        description = self.set_description.call_args.args[1]
        self.assertIn(self.PARENT_URL, description)
        self.assertNotIn('old-parent', description)

    def test_polling_does_not_resubmit_build(self):
        sleep = self.enterContext(mock.patch('pyartcd.jenkins.time.sleep'))
        self.queue_item.poll.side_effect = [
            {'executable': None},
            {},
            {'executable': {'number': 42}, 'task': {'url': 'https://jenkins/job/child/'}},
        ]

        jenkins.start_build(Jobs.RHCOS_SYNC, {}, watch_building_delay=3)

        self.job.invoke.assert_called_once_with(build_params={})
        self.assertEqual(self.queue_item.poll.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(3), mock.call(3)])

    def test_returns_build_url_without_waiting_for_completion(self):
        result = jenkins.start_build(Jobs.BUILD_MICROSHIFT, {}, return_build_url=True)

        self.assertEqual(result, (None, self.CHILD_URL))
        self.build.block_until_complete.assert_not_called()

    def test_completion_results_and_build_urls(self):
        for status in ('SUCCESS', 'FAILURE', 'ABORTED'):
            for return_build_url in (False, True):
                with self.subTest(status=status, return_build_url=return_build_url):
                    self.job.invoke.reset_mock()
                    self.build.block_until_complete.reset_mock()
                    self.build.poll.return_value = {'result': status}

                    result = jenkins.start_build(
                        Jobs.BUILD_MICROSHIFT,
                        {},
                        block_until_building=False,
                        block_until_complete=True,
                        return_build_url=return_build_url,
                    )

                    self.assertEqual(result, (status, self.CHILD_URL) if return_build_url else status)
                    self.job.invoke.assert_called_once_with(build_params={})
                    self.build.block_until_complete.assert_called_once_with()

    def test_incomplete_tekton_context_requires_jenkins_parent(self):
        for variable in ('TASKRUN_NAME', 'TEKTON_PIPELINERUN_NAME'):
            with self.subTest(variable=variable), mock.patch.dict(os.environ):
                del os.environ[variable]

                with self.assertRaises(RuntimeError):
                    jenkins.start_build(Jobs.RHCOS_SYNC, {})
                with self.assertRaises(RuntimeError):
                    jenkins.wait_until_building(self.queue_item, self.job)

        self.client.get_job.assert_not_called()
        self.job.invoke.assert_not_called()
        self.queue_item.poll.assert_not_called()

    def test_jenkins_parent_description_is_preserved(self):
        del os.environ['TASKRUN_NAME']
        del os.environ['TEKTON_PIPELINERUN_NAME']
        os.environ['BUILD_URL'] = 'https://jenkins/job/parent/7'
        os.environ['JOB_NAME'] = 'parent'

        jenkins.start_build(Jobs.RHCOS_SYNC, {})

        self.set_description.assert_called_once_with(
            self.build,
            'Started by upstream project <b>parent</b> '
            'build number <a href="https://jenkins/job/parent/7">7</a><br><br>',
        )

    def test_tekton_does_not_allow_jenkins_parent_metadata_updates(self):
        with self.assertRaises(RuntimeError):
            jenkins.update_title('new title')
        with self.assertRaises(RuntimeError):
            jenkins.update_description('new description')

        self.client.get_job.assert_not_called()

    def test_promotion_helpers_preserve_parameters(self):
        jenkins.start_build_microshift('4.20', '4.20.42', dry_run=True)
        self.client.get_job.assert_called_once_with(Jobs.BUILD_MICROSHIFT.value)
        self.job.invoke.assert_called_once_with(
            build_params={'BUILD_VERSION': '4.20', 'ASSEMBLY': '4.20.42', 'DRY_RUN': True}
        )
        self.client.get_job.reset_mock()
        self.job.invoke.reset_mock()

        jenkins.start_rhcos_sync('4.20.42-x86_64', dry_run=True, sign_only=True)
        self.client.get_job.assert_called_once_with(Jobs.RHCOS_SYNC.value)
        self.job.invoke.assert_called_once_with(
            build_params={'RELEASE_TAG': '4.20.42-x86_64', 'DRY_RUN': True, 'SIGN_ONLY': True}
        )
