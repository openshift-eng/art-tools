from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from artcommonlib.model import Missing, Model
from artcommonlib.variants import BuildVariant
from doozerlib.backend.rebaser import KonfluxRebaser
from doozerlib.cli.images_okd import OkdRebaseCli

SCOPED_CACHE_PREFIX = "dnf --disablerepo='*' --enablerepo="
SOURCE_DOCKERFILE = r"""FROM quay.io/centos/centos:stream10
RUN source /etc/os-release && [ "${ID}" != "centos" ] && exit 0; \
    wget --no-verbose \
        --output-document=/etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-NFV \
        https://www.centos.org/keys/RPM-GPG-KEY-CentOS-SIG-NFV && \
    printf '%s\n' \
        '[centos-nfv-openvswitch]' \
        'name=CentOS Stream 10 - NFV Open vSwitch' \
        'baseurl=https://mirror.stream.centos.org/SIGs/10-stream/nfv/$basearch/openvswitch-2/' \
        'enabled=1' \
        'gpgcheck=1' \
        'repo_gpgcheck=1' \
        'gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-NFV' \
        > /etc/yum.repos.d/centos-nfv-openvswitch.repo && \
    dnf clean all && rm -rf /var/cache/*
RUN dnf --disablerepo='*' --enablerepo=delorean-component-clients makecache && \
    dnf --disablerepo='*' --enablerepo=delorean-component-common makecache && \
    dnf --disablerepo='*' --enablerepo=delorean-component-keystone makecache && \
    dnf --disablerepo='*' --enablerepo=delorean-master-testing makecache && \
    dnf clean all && rm -rf /var/cache/*
"""


class TestOkdBaseRebase(IsolatedAsyncioTestCase):
    async def test_noninteractive_cache_survives_variant_merge_and_rebase(self):
        """Exercise the NFV and scoped cache commands from openshift/images/base/Dockerfile.rhel10."""
        original_config = {
            'name': 'openshift/openshift-enterprise-base-rhel10',
            'content': {
                'source': {
                    'git': {'url': 'https://github.com/openshift/images'},
                    'dockerfile': 'Dockerfile.rhel10',
                }
            },
            'okd': {
                'content': {
                    'source': {
                        'modifications': [
                            {
                                'action': 'replace',
                                'match': SCOPED_CACHE_PREFIX,
                                'replacement': "dnf -y --disablerepo='*' --enablerepo=",
                            }
                        ]
                    }
                }
            },
        }
        for variant in (BuildVariant.OKD, BuildVariant.OCP):
            with self.subTest(variant=variant), TemporaryDirectory() as directory:
                dest_dir = Path(directory)
                upstream_path = dest_dir / 'Dockerfile.rhel10'
                upstream_path.write_text(SOURCE_DOCKERFILE)
                dockerfile_path = dest_dir / 'Dockerfile'
                dockerfile_path.write_text(upstream_path.read_text())
                runtime = MagicMock()
                runtime.group = 'okd-5.1' if variant is BuildVariant.OKD else 'openshift-5.1'
                runtime.get_major_minor_fields.return_value = (5, 1)
                runtime.group_config = Model({'vars': {}, 'product': 'openshift'})
                metadata = MagicMock()
                metadata.distgit_key = 'openshift-enterprise-base-rhel10'
                metadata.image_name_short = metadata.distgit_key
                metadata.get_jira_info.return_value = ('PROJECT', 'component')
                metadata.get_component_name.return_value = f'{metadata.distgit_key}-container'
                metadata.get_lockfile_backend.return_value = None
                metadata.config = Model(original_config)
                if variant is BuildVariant.OKD:
                    cli = OkdRebaseCli(runtime, 'v5.1.0', '1', 'example.org/images', '', False)
                    metadata.config = cli.get_okd_image_config(metadata)
                self.assertEqual(metadata.config.content.source.dockerfile, 'Dockerfile.rhel10')
                self.assertEqual(metadata.config.content.source.git.url, 'https://github.com/openshift/images')
                rebaser = KonfluxRebaser(runtime, dest_dir, MagicMock(), 'unsigned', variant=variant)
                rebaser.source_labels = {'old': {}, 'now': {}}

                if metadata.config.content.source.modifications:
                    await rebaser._run_modifications(metadata, dest_dir, dest_dir)
                with (
                    patch.object(rebaser, '_add_build_repos'),
                    patch.object(rebaser, '_modify_cachito_commands'),
                ):
                    await rebaser._update_dockerfile(
                        metadata, None, dockerfile_path, 'v5.1.0', '1.scos10', None, False, dest_dir
                    )

                rendered = dockerfile_path.read_text()
                cache_commands = [line.strip() for line in rendered.splitlines() if 'makecache' in line]
                self.assertEqual(len(cache_commands), 4)
                for command in cache_commands:
                    self.assertRegex(
                        command, r'dnf\s+-y\s+makecache' if variant is BuildVariant.OKD else r'dnf\s+makecache'
                    )
                    self.assertNotIn('--enablerepo', command)
                    self.assertNotIn('--disablerepo', command)
                self.assertIn("'gpgcheck=1'", rendered)
                self.assertIn("'repo_gpgcheck=1'", rendered)
                self.assertIn('https://www.centos.org/keys/RPM-GPG-KEY-CentOS-SIG-NFV', rendered)
                self.assertIn('gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-NFV', rendered)
                self.assertEqual(upstream_path.read_text(), SOURCE_DOCKERFILE)
                self.assertIs(Model(original_config).content.source.modifications, Missing)
