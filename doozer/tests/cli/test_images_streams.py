import pytest
from artcommonlib.model import Missing, Model
from doozerlib.cli import images_streams

# Fixtures


@pytest.fixture
def mock_runtime(mocker):
    """Create a basic mock runtime object"""
    runtime = mocker.MagicMock()
    runtime.group_config.vars = {"MAJOR": "4", "MINOR": "17"}
    runtime.registry_config_dir = None
    runtime.build_system = "brew"
    runtime.streams = {}
    runtime.get_stream_names.return_value = []
    return runtime


@pytest.fixture
def mock_runtime_konflux(mocker, mock_runtime):
    """Create a mock runtime configured for Konflux"""
    mock_runtime.build_system = "konflux"
    mock_runtime.group = "openshift-4.17"
    mock_runtime.assembly = "stream"

    # Mock Konflux DB
    konflux_db = mocker.MagicMock()
    konflux_db.bind = mocker.MagicMock()
    mock_runtime.konflux_db = konflux_db

    return mock_runtime


@pytest.fixture
def mock_image_meta(mocker):
    """Create a basic mock image metadata object"""
    image_meta = mocker.MagicMock()
    image_meta.distgit_key = "ose-test"
    image_meta.config.content.source.ci_alignment.upstream_image = "registry.ci.openshift.org/ocp/4.17:test"
    image_meta.config.content.source.ci_alignment.primitive.return_value = {
        "upstream_image": "registry.ci.openshift.org/ocp/4.17:test",
        "mirror": True,
    }
    image_meta.config.final_stage_user = Missing
    return image_meta


@pytest.fixture
def mock_konflux_build_record(mocker):
    """Create a mock Konflux build record"""
    build_record = mocker.MagicMock()
    build_record.image_pullspec = "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123"
    return build_record


# Tests for connect_issue_with_pr


def test_connect_issue_with_pr(mocker):
    """Test connecting issue to PR when not already in title"""
    pr = mocker.MagicMock()
    pr.title = "test pr"
    pr.html_url = "https://github.com/openshift/OCBUGS/pulls/1234"

    images_streams.connect_issue_with_pr(pr, "OCPBUGS-1234")

    # Verify edit was called once
    pr.edit.assert_called_once()


def test_connect_issue_with_pr_bug_in_title(mocker):
    """Test connecting issue to PR when different bug already in title"""
    pr = mocker.MagicMock()
    pr.title = "OCPBUGS-1111: test pr"
    pr.html_url = "https://github.com/openshift/OCBUGS/pulls/1234"
    pr.get_issue_comments.return_value = []

    images_streams.connect_issue_with_pr(pr, "OCPBUGS-1234")

    # Verify create_issue_comment was called
    pr.create_issue_comment.assert_called_once()


# Tests for resolve_upstream_from


def test_resolve_upstream_from_with_member_and_explicit_upstream_image(mocker, mock_runtime):
    """Test resolving upstream from a member entry with explicit upstream_image"""
    # Create a mock image metadata with explicit upstream_image
    target_meta = mocker.MagicMock()
    target_meta.config.content.source.ci_alignment.upstream_image = "registry.ci.openshift.org/ocp/4.17:custom-base"

    mock_runtime.resolve_image.return_value = target_meta

    image_entry = Model({"member": "openshift-enterprise-base"})

    result = images_streams.resolve_upstream_from(mock_runtime, image_entry)

    assert result == "registry.ci.openshift.org/ocp/4.17:custom-base"
    mock_runtime.resolve_image.assert_called_once_with("openshift-enterprise-base", True)


def test_resolve_upstream_from_with_member_using_heuristic(mocker, mock_runtime):
    """Test resolving upstream from a member entry using name-based heuristic"""
    # Create a mock image metadata without explicit upstream_image
    target_meta = mocker.MagicMock()
    target_meta.config.content.source.ci_alignment.upstream_image = Missing
    target_meta.config.payload_name = None
    target_meta.config.name = "openshift/ose-ansible"

    mock_runtime.resolve_image.return_value = target_meta

    image_entry = Model({"member": "ansible"})

    result = images_streams.resolve_upstream_from(mock_runtime, image_entry)

    # Should strip 'ose-' prefix from image name
    assert result == "registry.ci.openshift.org/ocp/4.17:ansible"


def test_resolve_upstream_from_with_member_using_payload_name(mocker, mock_runtime):
    """Test resolving upstream from a member entry using payload_name"""
    mock_runtime.group_config.vars = {"MAJOR": "4", "MINOR": "18"}

    target_meta = mocker.MagicMock()
    target_meta.config.content.source.ci_alignment.upstream_image = Missing
    target_meta.config.payload_name = "custom-payload-name"
    target_meta.config.name = "openshift/ose-base"

    mock_runtime.resolve_image.return_value = target_meta

    image_entry = Model({"member": "base"})

    result = images_streams.resolve_upstream_from(mock_runtime, image_entry)

    # Should use payload_name and strip path
    assert result == "registry.ci.openshift.org/ocp/4.18:custom-payload-name"


def test_resolve_upstream_from_with_image_entry(mock_runtime):
    """Test resolving upstream from an image entry returns None"""
    image_entry = Model({"image": "quay.io/centos/centos:stream8"})

    result = images_streams.resolve_upstream_from(mock_runtime, image_entry)

    # Image entries cannot be resolved for CI
    assert result is None


def test_resolve_upstream_from_with_stream_entry(mocker, mock_runtime):
    """Test resolving upstream from a stream entry"""
    stream_config = mocker.MagicMock()
    stream_config.upstream_image = "registry.ci.openshift.org/ocp/4.17:golang-1.20"

    mock_runtime.resolve_stream.return_value = stream_config

    image_entry = Model({"stream": "golang"})

    result = images_streams.resolve_upstream_from(mock_runtime, image_entry)

    assert result == "registry.ci.openshift.org/ocp/4.17:golang-1.20"
    mock_runtime.resolve_stream.assert_called_once_with("golang")


# Tests for _get_upstreaming_entries


def test_get_upstreaming_entries_from_streams_only(mocker):
    """Test getting upstreaming entries from streams.yml only"""
    runtime = mocker.MagicMock()
    runtime.streams = {
        "golang": Model({"upstream_image": "registry.ci.openshift.org/ocp/4.17:golang-1.20", "mirror": True}),
        "nodejs": Model({"upstream_image": "registry.ci.openshift.org/ocp/4.17:nodejs-18"}),
        "no-upstream": Model({"some_other_field": "value"}),
    }
    runtime.get_stream_names.return_value = ["golang", "nodejs", "no-upstream"]

    # Specify stream names to avoid fetching image metas
    result = images_streams._get_upstreaming_entries(runtime, stream_names=["golang", "nodejs"])

    assert len(result) == 2
    assert "golang" in result
    assert "nodejs" in result
    assert result["golang"].upstream_image == "registry.ci.openshift.org/ocp/4.17:golang-1.20"
    assert result["nodejs"].upstream_image == "registry.ci.openshift.org/ocp/4.17:nodejs-18"


def test_get_upstreaming_entries_stream_not_found(mocker):
    """Test error when specified stream is not found"""
    runtime = mocker.MagicMock()
    # Create a mock that returns Missing for the nonexistent key
    mock_streams = mocker.MagicMock()
    mock_streams.__getitem__ = lambda self, key: Missing if key == "nonexistent" else Model({"upstream_image": "test"})
    runtime.streams = mock_streams

    with pytest.raises(IOError, match="Did not find stream nonexistent"):
        images_streams._get_upstreaming_entries(runtime, stream_names=["nonexistent"])


def test_get_upstreaming_entries_with_brew_build_system(mock_runtime, mock_image_meta):
    """Test getting upstreaming entries from image metas using Brew build system"""
    # Customize image metadata for this test
    mock_image_meta.distgit_key = "ose-ansible"
    mock_image_meta.config.content.source.ci_alignment.upstream_image = "registry.ci.openshift.org/ocp/4.17:ansible"
    mock_image_meta.config.content.source.ci_alignment.primitive.return_value = {
        "upstream_image": "registry.ci.openshift.org/ocp/4.17:ansible",
        "mirror": True,
    }
    mock_image_meta.pull_url.return_value = "brew-registry.example.com/openshift/ose-ansible:v4.17.0-1"

    mock_runtime.ordered_image_metas.return_value = [mock_image_meta]

    result = images_streams._get_upstreaming_entries(mock_runtime)

    assert len(result) == 1
    assert "ose-ansible" in result
    assert result["ose-ansible"]["image"] == "brew-registry.example.com/openshift/ose-ansible:v4.17.0-1"
    # Verify pull_url was called (Brew path)
    mock_image_meta.pull_url.assert_called_once()


def test_get_upstreaming_entries_with_konflux_build_system(
    mocker, mock_runtime_konflux, mock_image_meta, mock_konflux_build_record
):
    """Test getting upstreaming entries from image metas using Konflux build system"""
    # Customize image metadata for this test
    mock_image_meta.distgit_key = "ose-cli"
    mock_image_meta.config.content.source.ci_alignment.upstream_image = "registry.ci.openshift.org/ocp/4.17:cli"
    mock_image_meta.config.content.source.ci_alignment.primitive.return_value = {
        "upstream_image": "registry.ci.openshift.org/ocp/4.17:cli",
        "mirror": True,
    }

    # Mock pull_url to return the Konflux pullspec
    mock_image_meta.pull_url.return_value = "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123"
    mock_runtime_konflux.ordered_image_metas.return_value = [mock_image_meta]

    result = images_streams._get_upstreaming_entries(mock_runtime_konflux)

    assert len(result) == 1
    assert "ose-cli" in result
    assert result["ose-cli"]["image"] == "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123"

    # Verify pull_url was called
    mock_image_meta.pull_url.assert_called_once()


def test_get_upstreaming_entries_konflux_build_not_found(mocker, mock_runtime_konflux, mock_image_meta):
    """Test error when Konflux build is not found"""
    # Customize image metadata
    mock_image_meta.distgit_key = "ose-missing"
    mock_image_meta.config.content.source.ci_alignment.upstream_image = "registry.ci.openshift.org/ocp/4.17:missing"
    mock_image_meta.config.content.source.ci_alignment.primitive.return_value = {
        "upstream_image": "registry.ci.openshift.org/ocp/4.17:missing"
    }

    # Mock pull_url to raise IOError (as it would when no build is found)
    mock_image_meta.pull_url.side_effect = IOError("No Konflux build found for ose-missing in group openshift-4.17")
    mock_runtime_konflux.ordered_image_metas.return_value = [mock_image_meta]

    with pytest.raises(IOError, match="No Konflux build found for ose-missing"):
        images_streams._get_upstreaming_entries(mock_runtime_konflux)


def test_get_upstreaming_entries_with_final_user(mock_runtime, mock_image_meta):
    """Test that final_user is properly set from image metadata"""
    mock_image_meta.config.final_stage_user = "1001"
    mock_image_meta.pull_url.return_value = "brew-registry.example.com/test:latest"

    mock_runtime.ordered_image_metas.return_value = [mock_image_meta]

    result = images_streams._get_upstreaming_entries(mock_runtime)

    assert result["ose-test"].final_user == "1001"


# Tests for images:streams gen-buildconfigs command


def test_gen_buildconfigs_uses_get_upstreaming_entries():
    """Test that gen-buildconfigs uses _get_upstreaming_entries for both Brew and Konflux"""
    # This is a documentation test verifying the architecture
    # gen-buildconfigs calls _get_upstreaming_entries() which we already modified
    # to support both Brew and Konflux, so gen-buildconfigs automatically supports both

    # Verify the function exists and uses _get_upstreaming_entries
    import inspect

    # Access the underlying callback function from the Click Command
    callback = images_streams.images_streams_gen_buildconfigs.callback
    source = inspect.getsource(callback)

    # Verify it calls _get_upstreaming_entries
    assert "_get_upstreaming_entries(runtime, streams)" in source

    # Verify it uses the configuration fields from entries
    assert "config.transform" in source
    assert "config.upstream_image" in source
    assert "config.upstream_image_base" in source
