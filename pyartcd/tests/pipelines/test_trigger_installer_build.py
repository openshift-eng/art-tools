import asyncio
import json
import subprocess
from unittest.mock import AsyncMock, Mock

import pytest
from click.testing import CliRunner
from pyartcd.cli import cli
from pyartcd.pipelines.trigger_installer_build import trigger_installer_build

from pyartcd import tekton


@pytest.fixture
def anyio_backend():
    """Run async installer tests on asyncio."""
    return "asyncio"


@pytest.fixture
def services(mocker):
    """Isolate registry inspection, metadata lookup and cluster submission."""
    info = mocker.patch(
        'pyartcd.pipelines.trigger_installer_build.oc.get_release_image_info',
        new_callable=AsyncMock,
        return_value={
            'metadata': {'version': '4.22.0-0.nightly-2026-11-05-010101'},
            'digest': 'sha256:' + 'a' * 64,
            'config': {'architecture': 'amd64'},
        },
    )
    response = Mock(status_code=200, text='product: openshift_agent_installer\nversion: "4.22"\narches: [x86_64]\n')
    metadata = mocker.patch('pyartcd.pipelines.trigger_installer_build.requests.get', return_value=response)
    submit = mocker.patch(
        'pyartcd.pipelines.trigger_installer_build.tekton.start_pipeline_run', return_value='installer-run'
    )
    return info, metadata, submit


@pytest.mark.anyio
@pytest.mark.parametrize('version', ['4.22.21', '4.22.0-rc.1', '4.22.0-0.nightly-2026-11-05-010101'])
async def test_published_payloads_trigger_without_acceptance_gate(services, version):
    """Verify published payloads trigger without acceptance gate."""
    info, metadata, submit = services
    info.return_value['metadata']['version'] = version
    assert (
        await trigger_installer_build('quay.io/openshift-release-dev/ocp-release:tag', '/test/kubeconfig')
        == 'installer-run'
    )
    params = submit.call_args.args[1]
    assert params['payload'] == 'quay.io/openshift-release-dev/ocp-release@sha256:' + 'a' * 64
    assert params['group'] == 'installer-ove-ui-4.22'
    assert params['assembly'] == 'stream'
    assert submit.call_args.kwargs['namespace'] == 'art-installer-agent-tenant'
    assert submit.call_args.kwargs['kubeconfig'] == '/test/kubeconfig'
    assert 'installer-ove-ui-4.22/group.yml' in metadata.call_args.args[0]


@pytest.mark.anyio
async def test_retries_use_same_name_and_changed_digest_uses_new_name(services):
    """Verify retries use same name and changed digest uses new name."""
    info, _, submit = services
    for tag in ('one', 'two'):
        await trigger_installer_build('quay.io/example/release:' + tag, '/test/kubeconfig')
    names = [call.kwargs['run_name'] for call in submit.call_args_list]
    assert names[0] == names[1]
    info.return_value['digest'] = 'sha256:' + 'b' * 64
    await trigger_installer_build('quay.io/example/release:two', '/test/kubeconfig')
    assert submit.call_args.kwargs['run_name'] != names[0]


@pytest.mark.anyio
async def test_dry_run_does_not_submit_or_require_kubeconfig(services):
    """Verify dry run does not submit or require kubeconfig."""
    assert await trigger_installer_build('quay.io/example/release:tag', dry_run=True) is None
    services[2].assert_not_called()


@pytest.mark.anyio
async def test_missing_installer_group_skips_but_access_failures_raise(services):
    """Verify missing installer group skips but access failures raise."""
    _, metadata, submit = services
    metadata.return_value.status_code = 404
    assert await trigger_installer_build('quay.io/example/release:tag', '/test/kubeconfig') is None
    metadata.return_value.status_code = 503
    with pytest.raises(RuntimeError, match='HTTP 503'):
        await trigger_installer_build('quay.io/example/release:tag', '/test/kubeconfig')
    submit.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize('change', ['architecture', 'digest', 'group'])
async def test_invalid_payload_or_group_never_submits(services, change):
    """Verify invalid payload or group never submits."""
    info, metadata, submit = services
    if change == 'architecture':
        info.return_value['config']['architecture'] = 'arm64'
    elif change == 'digest':
        info.return_value['digest'] = 'bad'
    else:
        metadata.return_value.text = 'product: quay\nversion: "4.22"\narches: [x86_64]\n'
    with pytest.raises(ValueError):
        await trigger_installer_build('quay.io/example/release:tag', '/test/kubeconfig')
    submit.assert_not_called()


def test_named_submission_reuses_only_matching_existing_run(mocker, monkeypatch):
    """Verify named submission reuses only matching existing run."""
    monkeypatch.delenv('ART_TOOLS_COMMIT', raising=False)
    monkeypatch.delenv('PLR_TEMPLATE_COMMIT', raising=False)
    params = {'payload': 'quay.io/example/release@sha256:' + 'a' * 64, 'group': 'installer-ove-ui-4.22'}
    existing = {
        'spec': {
            'pipelineRef': {'name': 'build-layered-products'},
            'params': [{'name': k, 'value': v} for k, v in params.items()],
        }
    }
    command = mocker.patch('pyartcd.tekton.subprocess.run')
    command.side_effect = [
        subprocess.CalledProcessError(1, 'oc', stderr='Error from server (AlreadyExists): exists'),
        Mock(stdout=json.dumps(existing)),
    ]
    assert (
        tekton.start_pipeline_run(
            'build-layered-products',
            params,
            namespace='art-installer-agent-tenant',
            kubeconfig='/test/kubeconfig',
            run_name='installer-one',
        )
        == 'installer-one'
    )
    assert '--kubeconfig' in command.call_args.args[0]
    existing['spec']['params'][0]['value'] = 'different'
    command.side_effect = [
        subprocess.CalledProcessError(1, 'oc', stderr='(AlreadyExists)'),
        Mock(stdout=json.dumps(existing)),
    ]
    with pytest.raises(ValueError, match='different'):
        tekton.start_pipeline_run(
            'build-layered-products',
            params,
            namespace='art-installer-agent-tenant',
            kubeconfig='/test/kubeconfig',
            run_name='installer-one',
        )


def test_named_submission_does_not_swallow_forbidden(mocker):
    """Verify named submission does not swallow forbidden."""
    command = mocker.patch(
        'pyartcd.tekton.subprocess.run', side_effect=subprocess.CalledProcessError(1, 'oc', stderr='Forbidden')
    )
    with pytest.raises(subprocess.CalledProcessError):
        tekton.start_pipeline_run('build-layered-products', {}, run_name='installer-one', kubeconfig='/test/kubeconfig')
    assert command.call_count == 1


def test_named_creation_targets_cluster_and_preserves_parent_namespace(mocker, monkeypatch):
    """Verify named creation targets cluster and preserves parent namespace."""
    monkeypatch.setenv('TEKTON_PIPELINERUN_NAME', 'parent-one')
    monkeypatch.setenv('TASKRUN_NAMESPACE', 'source-namespace')
    command = mocker.patch(
        'pyartcd.tekton.subprocess.run', return_value=Mock(stdout='pipelinerun.tekton.dev/installer-one created')
    )
    assert (
        tekton.start_pipeline_run(
            'build-layered-products',
            {'payload': 'test'},
            namespace='target-namespace',
            kubeconfig='/target/kubeconfig',
            run_name='installer-one',
        )
        == 'installer-one'
    )
    assert command.call_args.args[0] == ['oc', '--kubeconfig', '/target/kubeconfig', 'create', '-f', '-']
    resource = json.loads(command.call_args.kwargs['input'])
    assert resource['metadata']['name'] == 'installer-one'
    assert 'generateName' not in resource['metadata']
    assert resource['metadata']['namespace'] == 'target-namespace'
    assert (
        '/source-namespace/parent-one?'
        in resource['metadata']['annotations']['art.openshift.io/parent-pipelinerun-console-url']
    )


def test_cli_passes_payload_and_dry_run_to_trigger(mocker, tmp_path):
    """Verify cli passes payload and dry run to trigger."""
    config = tmp_path / 'artcd.toml'
    config.write_text('')
    trigger = mocker.patch('pyartcd.pipelines.trigger_installer_build.trigger_installer_build', new_callable=AsyncMock)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = CliRunner().invoke(
            cli,
            [
                '--config',
                str(config),
                '--working-dir',
                str(tmp_path),
                '--dry-run',
                'trigger-installer-build',
                '--payload',
                'quay.io/example/release:tag',
            ],
        )
        assert result.exit_code == 0, result.output or str(result.exception)
        trigger.assert_awaited_once_with('quay.io/example/release:tag', None, dry_run=True)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


@pytest.mark.anyio
async def test_submission_uses_art_cluster_credential(services, monkeypatch):
    """Verify submission uses art cluster credential."""
    monkeypatch.setenv('ART_CLUSTER_ART_CD_PIPELINE_KUBECONFIG', '/art-cluster/kubeconfig')
    monkeypatch.setenv('ASSISTED_INSTALLER_SA_KUBECONFIG', '/konflux/kubeconfig')
    await trigger_installer_build('quay.io/example/release:tag')
    assert services[2].call_args.kwargs['kubeconfig'] == '/art-cluster/kubeconfig'


@pytest.mark.anyio
async def test_submission_requires_art_cluster_credential(services, monkeypatch):
    """Verify submission requires art cluster credential."""
    monkeypatch.delenv('ART_CLUSTER_ART_CD_PIPELINE_KUBECONFIG', raising=False)
    monkeypatch.setenv('ASSISTED_INSTALLER_SA_KUBECONFIG', '/konflux/kubeconfig')
    with pytest.raises(ValueError, match='ART_CLUSTER_ART_CD_PIPELINE_KUBECONFIG'):
        await trigger_installer_build('quay.io/example/release:tag')
    services[2].assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize(
    'payload',
    [
        'https://user:secret-password@internal.example/image:tag',
        'user:secret-token@internal.example/image:tag',
        'internal.example/image:tag?token=secret-token',
    ],
)
async def test_credential_bearing_payload_is_rejected_before_inspection(services, payload, caplog):
    """Verify credential bearing payload is rejected before inspection."""
    with pytest.raises(ValueError, match='OCI image reference'):
        await trigger_installer_build(payload, dry_run=True)
    services[0].assert_not_awaited()
    services[2].assert_not_called()
    assert 'secret-' not in caplog.text


@pytest.mark.anyio
@pytest.mark.parametrize('missing_group', [False, True])
async def test_payload_details_are_not_logged(services, missing_group, caplog):
    """Verify payload details are not logged."""
    caplog.set_level('DEBUG')
    if missing_group:
        services[1].return_value.status_code = 404
    await trigger_installer_build('internal.example/private/image:tag', dry_run=True)
    assert 'internal.example' not in caplog.text
    assert 'private/image' not in caplog.text
    assert 'installer-ove-ui-4.22' in caplog.text


def test_tekton_debug_log_omits_parameter_values(mocker, caplog):
    """Verify tekton debug log omits parameter values."""
    caplog.set_level('DEBUG')
    mocker.patch('pyartcd.tekton.subprocess.run', return_value=Mock(stdout='pipelinerun.tekton.dev/run-one created'))
    tekton.start_pipeline_run('build-layered-products', {'payload': 'internal.example/private/image:tag'})
    assert 'internal.example' not in caplog.text
    assert 'private/image' not in caplog.text
