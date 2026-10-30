import json
import subprocess
from pathlib import Path

import pytest
from validator import ci


def git(root, *args):
    """Execute a Git command in the temporary test repository and return its output."""
    return subprocess.run(['git', '-C', str(root), *args], check=True, capture_output=True).stdout


def repository(tmp_path):
    """Create a minimal committed release checkout with hooks and signing disabled."""
    git(tmp_path, 'init')
    git(tmp_path, 'config', 'user.email', 'test@example.com')
    git(tmp_path, 'config', 'user.name', 'Test')
    git(tmp_path, 'config', 'commit.gpgsign', 'false')
    git(tmp_path, 'config', 'core.hooksPath', '/dev/null')
    (tmp_path / 'images').mkdir()
    for name in ('a', 'b'):
        (tmp_path / 'images' / f'{name}.yml').write_text('mode: disabled\n')
    (tmp_path / 'group.yml').write_text('name: test\nvars: {MAJOR: 1, MINOR: 0}\n')
    git(tmp_path, 'add', '.')
    git(tmp_path, 'commit', '-m', 'base')
    return git(tmp_path, 'rev-parse', 'HEAD').decode().strip()


def test_regular_change_selects_changed_metadata_only(tmp_path):
    """Regular change selects changed metadata only."""
    base = repository(tmp_path)
    (tmp_path / 'images/a.yml').write_text('mode: disabled\nname: updated\n')
    git(tmp_path, 'add', '.')
    git(tmp_path, 'commit', '-m', 'change')
    assert ci.select_files(tmp_path, base) == [Path('images/a.yml')]


def test_shared_change_and_image_deletion_revalidate_remaining_files(tmp_path):
    """Shared change and image deletion revalidate remaining files."""
    base = repository(tmp_path)
    (tmp_path / 'images/a.yml').unlink()
    git(tmp_path, 'add', '.')
    git(tmp_path, 'commit', '-m', 'delete')
    assert ci.select_files(tmp_path, base) == [Path('group.yml'), Path('images/b.yml')]


def test_ci_reports_malformed_yaml_as_failure_and_writes_artifact(tmp_path):
    """Ci reports malformed yaml as failure and writes artifact."""
    repository(tmp_path)
    (tmp_path / 'images/a.yml').write_text('[]\n')
    report = tmp_path / 'report.json'
    assert ci.main(['--repo', str(tmp_path), '--schema-only', '--report', str(report)]) == 1
    data = json.loads(report.read_text())
    assert any('images/a.yml' in error and 'mapping' in error for error in data['errors'])


def test_ci_rejects_metadata_symlink_escaping_checkout(tmp_path):
    """Ci rejects metadata symlink escaping checkout."""
    repository(tmp_path)
    (tmp_path / 'images/a.yml').unlink()
    (tmp_path / 'images/a.yml').symlink_to('/etc/passwd')
    assert ci.main(['--repo', str(tmp_path), '--schema-only']) == 1


def test_no_metadata_changes_is_success(tmp_path):
    """No metadata changes is success."""
    base = repository(tmp_path)
    assert ci.main(['--repo', str(tmp_path), '--base-ref', base, '--schema-only']) == 0


def test_missing_base_revision_fails_instead_of_skipping(tmp_path):
    """Missing base revision fails instead of skipping."""
    repository(tmp_path)
    assert ci.main(['--repo', str(tmp_path), '--base-ref', 'missing', '--schema-only']) == 1


def test_invalid_wip_does_not_hide_invalid_regular_metadata(tmp_path):
    """Invalid wip does not hide invalid regular metadata."""
    repository(tmp_path)
    (tmp_path / 'images/a.yml').write_text('mode: wip\nname: test/image\n')
    (tmp_path / 'images/b.yml').write_text('[]\n')
    report = tmp_path / 'result.json'
    assert ci.main(['--repo', str(tmp_path), '--schema-only', '--report', str(report)]) == 1
    errors = json.loads(report.read_text())['errors']
    assert any('images/b.yml' in error for error in errors)
    assert not any('images/a.yml' in error for error in errors)


def test_bad_reference_structure_still_produces_json_report(tmp_path):
    """Bad reference structure still produces json report."""
    repository(tmp_path)
    (tmp_path / 'images/a.yml').write_text('name: test/image\nfrom: [bad]\n')
    report = tmp_path / 'result.json'
    assert ci.main(['--repo', str(tmp_path), '--schema-only', '--report', str(report)]) == 1
    assert json.loads(report.read_text())['errors']


def test_empty_or_main_checkout_is_not_reported_as_valid(tmp_path):
    """Empty or main checkout is not reported as valid."""
    report = tmp_path / 'result.json'
    assert ci.main(['--repo', str(tmp_path), '--schema-only', '--report', str(report)]) == 1
    assert any('group.yml' in error for error in json.loads(report.read_text())['errors'])


def test_layered_product_shipment_merge_request_is_valid(tmp_path):
    """Layered product shipment merge request is valid."""
    repository(tmp_path)
    (tmp_path / 'releases.yml').write_text(
        'releases:\n  1.0.0:\n    assembly:\n      type: standard\n      group:\n        shipment:\n          mr: https://gitlab.cee.redhat.com/art/data/-/merge_requests/1\n'
    )
    assert ci.main(['--repo', str(tmp_path), '--schema-only']) == 0


def test_malformed_stream_alias_still_writes_report(tmp_path):
    """Malformed stream alias still writes report."""
    repository(tmp_path)
    (tmp_path / 'streams.yml').write_text('builder:\n  image: example/image:tag\n  aliases: [7]\n')
    report = tmp_path / 'result.json'
    assert ci.main(['--repo', str(tmp_path), '--schema-only', '--report', str(report)]) == 1
    assert any('alias' in error for error in json.loads(report.read_text())['errors'])


@pytest.mark.parametrize('base', ['--output={output}', '-R', 'HEAD..HEAD'])
def test_base_revision_cannot_inject_git_options_or_ranges(tmp_path, base):
    """Only a single commit can select metadata; options cannot create files or skip checks."""
    repository(tmp_path)
    output = tmp_path / 'injected-output'
    report = tmp_path / 'result.json'
    assert (
        ci.main(
            [
                '--repo',
                str(tmp_path),
                f'--base-ref={base.format(output=output)}',
                '--schema-only',
                '--report',
                str(report),
            ]
        )
        == 1
    )
    assert not output.exists()
    assert json.loads(report.read_text())['errors']


@pytest.mark.parametrize(
    'release,upgrades',
    [
        ('1.0.0', '0.9.0,1.0.0'),
        ('ec.1', '1.0.0-ec.0,1.0.0-ec.1'),
        ('rc.1', '1.0.0-rc.1'),
    ],
)
def test_root_releases_semantics_use_the_selected_checkout(tmp_path, release, upgrades):
    """Release upgrade self-references fail even when invoked outside the metadata checkout."""
    repository(tmp_path)
    (tmp_path / 'releases.yml').write_text(
        f'releases:\n  {release}:\n    assembly:\n      type: standard\n      group:\n        upgrades: {upgrades}\n'
    )
    report = tmp_path / 'result.json'
    assert ci.main(['--repo', str(tmp_path), '--schema-only', '--report', str(report)]) == 1
    errors = json.loads(report.read_text())['errors']
    assert any('releases.yml' in error and 'references to themselves' in error for error in errors)


def test_root_prerelease_upgrade_can_reference_another_release(tmp_path):
    """Valid prerelease upgrades use group variables from --repo rather than the process cwd."""
    repository(tmp_path)
    (tmp_path / 'releases.yml').write_text(
        'releases:\n  ec.1:\n    assembly:\n      type: standard\n      group:\n        upgrades: 1.0.0-ec.0\n'
    )
    assert ci.main(['--repo', str(tmp_path), '--schema-only']) == 0
