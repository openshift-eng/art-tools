import glob
import io
import json
import os
import re
import shutil
import threading
from pathlib import Path
from typing import Optional, Tuple, Union
from urllib.parse import urlparse

import yaml
from artcommonlib import exectools, pushd
from artcommonlib.brew import BuildStates
from dockerfile_parse import DockerfileParser
from koji import ClientSession
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib import brew, util
from doozerlib.runtime import Runtime


class OLMBundle(object):
    """This class is responsible for generating bundle containers out of previously built operators

    Every OLM Operator image should have a corresponding bundle container, which is mostly empty,
    carrying only the operator's manifests and some special Dockerfile labels, that allows the
    bundle container to publish those manifests on operator's behalf

    Even though each bundle container has its dedicated distgit repo, they are not meant to be
    independently built, due to their tight coupling to corresponding operators
    """

    def __init__(
        self,
        runtime: Runtime,
        operator_nvr_or_dict: Union[str, dict],
        dry_run: Optional[bool] = False,
        brew_session: Optional[ClientSession] = None,
    ):
        self.runtime = runtime
        self.dry_run = dry_run
        self.brew_session = brew_session or runtime.build_retrying_koji_client()
        self.operator_dict: Optional[dict] = None
        self.image_references = {}
        self.found_image_references = {}

        if isinstance(operator_nvr_or_dict, dict):
            self.operator_dict = operator_nvr_or_dict
        else:
            build = brew.get_build_objects([operator_nvr_or_dict], self.brew_session)[0]
            if not build:
                raise IOError(f"Build {self.operator_nvr} doesn't exist in Brew.")
            self.operator_dict = build

    @property
    def operator_nvr(self):
        return self.operator_dict['nvr']

    @property
    def operator_repo_name(self):
        source_url = urlparse(self.operator_dict['source'])
        # /git/containers/ose-cluster-kube-descheduler-operator => containers/ose-cluster-kube-descheduler-operator
        return f"containers/{source_url.path.rstrip('/').rsplit('/')[-1]}"

    @property
    def operator_build_commit(self):
        source_url = urlparse(self.operator_dict['source'])
        return source_url.fragment

    def find_bundle_image(self) -> str:
        """Check if a bundle already exists for the nvr that was used to init the OLMBundle object

        :return: NVR of latest found bundle build, or None if there is no bundle build nvr
                        corresponding to the nvr used to init the OLMBundle object
        """
        prefix = f"{self.bundle_brew_component}-{self.operator_dict['version']}.{self.operator_dict['release']}-"
        bundle_package_id = self.brew_session.getPackageID(self.bundle_brew_component, strict=True)
        builds = self.brew_session.listBuilds(
            packageID=bundle_package_id,
            pattern=prefix + "*",
            state=BuildStates.COMPLETE.value,
            queryOpts={'limit': 1, 'order': '-creation_event_id'},
            completeBefore=None,
        )
        if not builds:
            return None

        build_info = builds[0]
        pullspec = build_info['extra']['image']['index']['pull'][0]
        if not self.delivery_labels_match(pullspec):
            self.runtime.logger.info(
                f"Cannot use bundle nvr {build_info['nvr']} since it does not have expected image "
                f"delivery labels for assembly {self.runtime.assembly}"
            )
            return None
        return build_info['nvr']

    def rebase(self):
        """Update bundle distgit contents with manifests from given operator NVR
        Perform image SHA replacement on manifests before commit & push
        Annotations and Dockerfile labels are re-generated with info from operator's package YAML

        :return bool True if rebase succeeds, False if there was nothing new to commit
        """
        self.clone_operator()
        self.checkout_operator_to_build_commit()
        self.clone_bundle()
        self.clean_bundle_contents()
        self.get_operator_package_yaml_info()
        self.copy_operator_manifests_to_bundle()
        self.replace_image_references_by_sha_on_bundle_manifests()
        self.generate_bundle_annotations()
        self.generate_bundle_dockerfile()
        self.create_container_yaml()
        return self.commit_and_push_bundle(commit_msg="Update bundle manifests")

    def build(self) -> Tuple[Optional[int], Optional[int], Optional[dict]]:
        """Trigger a brew build of operator's bundle

        :return: (task_id, task_url, build_info) if build succeeds,
                 (task_id, task_url, None) if container-build task is created but build fails,
                 or (None, None, None) if unable to create a container-build task.
        """
        self.clone_bundle()
        task_id, task_url = self.trigger_bundle_container_build()
        if task_id:
            self.runtime.logger.info("Build running: %s", task_url)

        success = self.watch_bundle_container_build(task_id)
        if not success:
            return task_id, task_url, None

        if self.dry_run:
            return task_id, task_url, {'nvr': f"{self.bundle_brew_component}-v0.0.0-1"}

        taskResult = self.brew_session.getTaskResult(task_id)
        build_id = int(taskResult["koji_builds"][0])
        build_info = self.brew_session.getBuild(build_id)
        return task_id, task_url, build_info

    @property
    def bundle_image_name(self):
        prefix = '' if self.bundle_name.startswith('ose-') else 'ose-'
        return f'openshift/{prefix}{self.bundle_name}'

    def clone_operator(self):
        """Clone operator distgit repository to doozer working dir"""
        dg_dir = Path(self.operator_clone_path)
        tag = f'{self.operator_dict["version"]}-{self.operator_dict["release"]}'
        if dg_dir.exists():
            self.runtime.logger.info("Distgit directory already exists; skipping clone: %s", dg_dir)
            if self.runtime.upcycle:
                self.runtime.logger.warning("Refreshing source for '%s' due to --upcycle", dg_dir)
                exectools.cmd_assert(["git", "-C", str(dg_dir), "clean", "-fdx"])
                exectools.cmd_assert(
                    ["git", "-C", str(dg_dir), "fetch", "--depth", "1", "origin", "tag", tag], retries=3
                )
                exectools.cmd_assert(["git", "-C", str(dg_dir), "reset", "--hard", "FETCH_HEAD"])
            return
        dg_dir.parent.mkdir(parents=True, exist_ok=True)
        exectools.cmd_assert(
            'rhpkg {} clone --depth 1 --branch {} {} {}'.format(
                self.rhpkg_opts,
                tag,
                self.operator_repo_name,
                self.operator_clone_path,
            ),
            retries=3,
        )

    def checkout_operator_to_build_commit(self):
        """Checkout clone of operator repository to specific commit used to build given operator NVR"""
        with pushd.Dir(self.operator_clone_path):
            exectools.cmd_assert('git checkout {}'.format(self.operator_build_commit))

    def does_bundle_branch_exist(self):
        try:
            self.clone_bundle(retries=1)
            return True, ''
        except Exception as err:
            if len(err.args) > 1:
                _, _, clone_error = err.args[1]
                if f"Could not find remote branch {self.branch} to clone" in clone_error:
                    self.runtime.logger.warning(
                        "Could not find remote branch %s to clone, skipping bundle clone", self.branch
                    )
                    return False, f'{self.bundle_repo_name}/{self.branch}'
            raise

    def clone_bundle(self, retries: int = 3):
        """Clone corresponding bundle distgit repository of given operator NVR"""
        dg_dir = Path(self.bundle_clone_path)
        if dg_dir.exists():
            self.runtime.logger.info("Distgit directory already exists; skipping clone: %s", dg_dir)
            if self.runtime.upcycle:
                self.runtime.logger.warning("Refreshing source for '%s' due to --upcycle", dg_dir)
                exectools.cmd_assert(["git", "-C", str(dg_dir), "clean", "-fdx"])
                exectools.cmd_assert(
                    ["git", "-C", str(dg_dir), "fetch", "--depth", "1", "origin", self.branch], retries=3
                )
                exectools.cmd_assert(
                    [
                        "git",
                        "-C",
                        str(dg_dir),
                        "checkout",
                        "-B",
                        self.branch,
                        "--track",
                        f"origin/{self.branch}",
                        "--force",
                    ]
                )
            return
        dg_dir.parent.mkdir(parents=True, exist_ok=True)
        exectools.cmd_assert(
            'rhpkg {} clone --depth 1 --branch {} {} {}'.format(
                self.rhpkg_opts,
                self.branch,
                self.bundle_repo_name,
                self.bundle_clone_path,
            ),
            retries=retries,
        )

    def clean_bundle_contents(self):
        """Delete all files currently present in the bundle repository
        Generating bundle files is an idempotent operation, so it is much easier to clean up
        everything and re-create them instead of parsing and figuring out what changed

        At the end, only relevant diff, if any, will be committed.
        """
        exectools.cmd_assert(["git", "-C", self.bundle_clone_path, "rm", "--ignore-unmatch", "-rf", "."])

    def get_operator_package_yaml_info(self):
        """Get operator package name and channel from its package YAML
        This info will be used to generate bundle's Dockerfile labels and metadata/annotations.yaml
        """
        file_path = glob.glob('{}/*package.yaml'.format(self.operator_manifests_dir))[0]
        with io.open(file_path) as f:
            package_yaml = yaml.safe_load(f)

        self.package = package_yaml['packageName']
        self.channel = str(package_yaml['channels'][0]['name'])

    def copy_operator_manifests_to_bundle(self):
        """Copy all manifests from the operator distgit repository over to its corresponding bundle
        repository (except image-references file)
        We can be sure that the manifests contents are exactly what we expect, because our copy of
        operator repository is checked out to the specific commit used to build given operator NVR
        """
        dest = Path(self.bundle_manifests_dir)
        dest.mkdir(parents=True, exist_ok=True)
        for src in self.list_of_manifest_files_to_be_copied:
            shutil.copy2(src, dest, follow_symlinks=False)
        refs = dest / "image-references"
        if refs.exists():
            with open(refs, 'r') as f:
                image_refs_yml = yaml.safe_load(f.read())
            for entry in image_refs_yml.get('spec', {}).get('tags', []):
                self.image_references[entry["name"]] = entry
            refs.unlink()

    def replace_image_references_by_sha_on_bundle_manifests(self):
        """Iterate through all bundle manifests files, replacing any image reference tag by its
        corresponding SHA
        That is used to allow disconnected installs, where a cluster can't reach external registries
        in order to translate image tags into something "pullable"
        """
        for file in glob.glob('{}/*'.format(self.bundle_manifests_dir)):
            with io.open(file, 'r+', encoding='utf-8') as f:
                contents = self.find_and_replace_image_references_by_sha(f.read())
                f.seek(0)
                f.truncate()
                if "clusterserviceversion.yaml" in file:
                    if not self.valid_subscription_label:
                        raise ValueError("missing valid-subscription-label in operator config")
                    yml_content = yaml.safe_load(contents)
                    yml_content['metadata']['annotations']['operators.openshift.io/valid-subscription'] = (
                        self.valid_subscription_label
                    )
                    f.write(yaml.dump(yml_content))
                else:
                    f.write(contents)

        if len(self.found_image_references) != len(self.image_references):
            message = (
                "Mismatch between number of found image references and image-references file. "
                f"Found {len(self.found_image_references)}: {sorted(self.found_image_references.keys())}, "
                f"Expected {len(self.image_references)}: {sorted(self.image_references.keys())}. "
                "Operator build metadata is invalid, please investigate."
            )
            self.runtime.logger.warning(message)

    def generate_bundle_annotations(self):
        """Create an annotations YAML file for the bundle, using info extracted from operator's
        package YAML
        """
        annotations_file = '{}/metadata/annotations.yaml'.format(self.bundle_clone_path)
        exectools.cmd_assert('mkdir -p {}'.format(os.path.dirname(annotations_file)))

        with io.open(annotations_file, 'w', encoding='utf-8') as writer:
            writer.write(yaml.dump({'annotations': self.operator_framework_tags}))

    def generate_bundle_dockerfile(self):
        """Create a Dockerfile with instructions to build the bundle container and a set of LABELs
        that allow the bundle to publish its manifests on operator's behalf.
        """
        operator_df = DockerfileParser('{}/Dockerfile'.format(self.operator_clone_path))
        bundle_df = DockerfileParser('{}/Dockerfile'.format(self.bundle_clone_path))

        bundle_df.content = 'FROM scratch\nCOPY ./manifests /manifests\nCOPY ./metadata /metadata'
        bundle_df.labels = operator_df.labels
        bundle_df.labels['com.redhat.component'] = self.bundle_brew_component
        bundle_df.labels['com.redhat.delivery.appregistry'] = False
        bundle_df.labels['name'] = self.bundle_image_name
        bundle_df.labels['version'] = '{}.{}'.format(
            operator_df.labels['version'],
            operator_df.labels['release'],
        )
        bundle_df.labels = {
            **bundle_df.labels,
            **self.redhat_delivery_tags,
            **self.operator_framework_tags,
        }
        del bundle_df.labels['release']

    def create_container_yaml(self):
        """Use container.yaml to disable unnecessary multiarch"""
        filename = '{}/container.yaml'.format(self.bundle_clone_path)
        with io.open(filename, 'w', encoding='utf-8') as writer:
            writer.write('# metadata containers are not functional and do not need to be multiarch')
            writer.write('\n\n')
            writer.write(
                yaml.dump(
                    {
                        'platforms': {'only': ['x86_64']},
                        'operator_manifests': {'manifests_dir': 'manifests'},
                    }
                )
            )

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(60))
    def commit_and_push_bundle(self, commit_msg):
        """Try to commit and push bundle distgit repository if there were any content changes.

        :param string commit_msg: Commit message
        :return bool True if new changes were committed and pushed, False otherwise
        """
        with pushd.Dir(self.bundle_clone_path):
            exectools.cmd_assert(["git", "add", "-A"])
            rc, _, _ = exectools.cmd_gather(["git", "diff-index", "--quiet", "HEAD"])
            if rc == 0:
                self.runtime.logger.warning("Nothing new to commit.")
                return False
            exectools.cmd_assert('rhpkg {} commit -m "{}"'.format(self.rhpkg_opts, commit_msg))
            _, is_shallow, _ = exectools.cmd_gather(["git", "rev-parse", "--is-shallow-repository"])
            if is_shallow.strip() == "true":
                exectools.cmd_assert(["git", "fetch", "--unshallow"], retries=3)
            cmd = f'rhpkg {self.rhpkg_opts} push'
            if not self.dry_run:
                exectools.cmd_assert(cmd)
            else:
                self.runtime.logger.warning("[DRY RUN] Would have run %s", cmd)
            return True

    def trigger_bundle_container_build(self) -> Tuple[Optional[int], Optional[str]]:
        """Ask brew for a container-build of operator's bundle

        :return: (task_id, task_url) if brew task was successfully created, (None, None) otherwise
        """
        if self.dry_run:
            self.runtime.logger.warning("[DRY RUN] Would have triggered bundle build.")
            return 12345, "https://brewweb.example.com/brew/taskinfo?taskID=12345"
        with pushd.Dir(self.bundle_clone_path):
            rc, out, err = exectools.cmd_gather(
                'rhpkg {} container-build --nowait --target {}'.format(self.rhpkg_opts, self.target),
            )

        if rc != 0:
            msg = 'Unable to create brew task: rc={} out={} err={}'.format(rc, out, err)
            self.runtime.logger.warning(msg)
            return False

        task_url = re.search(r'Task info:\s(.+)', out).group(1)
        task_id = int(re.search(r'Created task:\s(\d+)', out).group(1))
        return task_id, task_url

    def watch_bundle_container_build(self, task_id):
        """Log brew task URL and eventual task states until task completion (or failure)

        :return bool True if brew task was successfully completed, False otherwise
        """
        if self.dry_run:
            self.runtime.logger.warning("[DRY RUN] Would have watched bundle container build task: %d", task_id)
            return True
        error = brew.watch_task(
            self.brew_session,
            self.runtime.logger.info,
            task_id,
            threading.Event(),
        )
        if error:
            self.runtime.logger.info(error)
            return False
        return True

    def find_and_replace_image_references_by_sha(self, contents):
        """Search image references (<registry>/<image>:<tag>) on given contents (usually YAML),
        replace them with corresponding (<registry>/<image>@<sha>) and collect such replacements to
        list them as "relatedImages" under "spec" section of contents (should it exist)

        :param string contents: File contents that potentially contains image references
        :return string: Same contents, with aforementioned modifications applied
        """
        found_images = {}

        def collect_replaced_image(match):
            source_image = f'{match.group(1)}:{match.group(2)}'
            sha = self.fetch_image_sha(source_image)
            image = '{}/{}@{}'.format(
                'registry.redhat.io',  # hardcoded until appregistry is dead
                match.group(1).replace('openshift/', 'openshift4/'),
                sha,
            )
            key = re.search(r'([^/]+)/(.+)', match.group(1)).group(2)
            self.runtime.logger.info(f"Replacing {self.operator_csv_config['registry']}/{source_image} with {image}")
            found_images[key] = image
            return image

        pattern = r'{}\/([^:]+):([^\'"\\\s]+)'.format(self.operator_csv_config['registry'])
        new_contents = re.sub(
            pattern,
            collect_replaced_image,
            contents,
            flags=re.MULTILINE,
        )

        self.found_image_references.update(found_images)
        return self.append_related_images_spec(new_contents, found_images)

    def fetch_image_sha(self, image):
        """Get corresponding SHA of given image (using `oc image info`)

        OCP 4.3+ supports "manifest-lists", which is a SHA that doesn't represent an actual image,
        but a list of images per architecture instead. OCP 4.3+ is smart enough to read that list
        and pick the correct architecture.

        Unfortunately, OCP 4.2 is multi-arch and does not support manifest-lists. It is still unclear
        how we want to handle this case on this new bundle workflow. Previously it was easy to simply
        generate manifests for all arches in a single run, since all manifests were living together
        under the same branch.

        Possible solutions:
        * Build multiple sets of operator bundles, one per architecture
        Caveats: More manual work, and maybe more advisories, since I'm not sure if Errata Tool will
        let us attach multiple builds of the same brew component to a single advisory.

        * Have a multi-arch build of the bundle container
        Caveats: Not sure if appregistry/IIB will know what to do with that, publishing each arch in
        a different channel

        For now, simply assuming x86_64 (aka amd64 in golang land)

        :param string image: Image reference (format: <registry>/<image>:<tag>)
        :return string: SHA of corresponding <tag> (format: sha256:a1b2c3d4...)
        """
        registry = self.runtime.group_config.urls.brew_image_host.rstrip('/')
        ns = self.runtime.group_config.urls.brew_image_namespace
        image = '{}/{}'.format(ns, image.replace('/', '-')) if ns else image

        pull_spec = '{}/{}'.format(registry, image)
        try:
            image_info = util.oc_image_info_for_arch__caching(pull_spec)
        except:
            self.runtime.logger.error(
                f'Unable to find image from CSV: {pull_spec}. Image may have failed to build after CSV rebase.'
            )
            raise

        if self.runtime.group_config.operator_image_ref_mode == 'manifest-list':
            return image_info['listDigest']

        # @TODO: decide how to handle 4.2 multi-arch. hardcoding amd64 for now
        return image_info['contentDigest']

    def append_related_images_spec(self, contents, images):
        """Create a new section under contents' "spec" called "relatedImages", listing all given
        images in the following format:

        spec:
          relatedImages:
            - name: image-a
              image: registry/image-a@sha256:....
            - name: image-b
              image: registry/image-b@sha256:....

        If list of images is empty or "spec" section is not found, return contents as-is

        :param string contents: File contents that potentially contains a "spec" section
        :param list images: List of image info dictionaries (format: [{"name": "...", "image": "..."}])
        :return string: Given contents with aforementioned modifications applied
        """
        if not images:
            return contents

        related_images = ['    - name: {}\n      image: {}'.format(name, image) for name, image in images.items()]
        related_images.sort()

        return re.sub(
            r'^spec:\n',
            'spec:\n  relatedImages:\n{}\n'.format('\n'.join(related_images)),
            contents,
            flags=re.MULTILINE,
        )

    @property
    def operator_name(self):
        return self.operator_repo_name.split('/')[-1]

    @property
    def operator_csv_config(self):
        return self.runtime.image_map[self.operator_name].config['update-csv']

    @property
    def operator_clone_path(self):
        return '{}/distgits/{}'.format(self.runtime.working_dir, self.operator_repo_name)

    @property
    def operator_manifests_dir(self):
        return '{}/{}'.format(
            self.operator_clone_path,
            self.operator_csv_config['manifests-dir'].rstrip('/'),
        )

    @property
    def operator_bundle_dir(self):
        return '{}/{}'.format(
            self.operator_manifests_dir,
            self.operator_csv_config['bundle-dir'].rstrip('/'),
        )

    @property
    def operator_brew_component(self):
        return self.runtime.image_map[self.operator_name].get_component_name()

    @property
    def bundle_name(self):
        return '{}-bundle'.format(self.operator_name)

    @property
    def bundle_repo_name(self):
        return '{}-bundle'.format(self.operator_repo_name)

    @property
    def bundle_clone_path(self):
        return '{}-bundle'.format(self.operator_clone_path)

    @property
    def bundle_manifests_dir(self):
        return '{}/manifests'.format(self.bundle_clone_path)

    @property
    def bundle_brew_component(self):
        config = self.runtime.image_map[self.operator_name].config

        if 'distgit' in config and 'bundle_component' in config['distgit']:
            return config['distgit']['bundle_component']

        return self.operator_brew_component.replace('-container', '-metadata-container')

    @property
    def branch(self):
        config = self.runtime.image_map[self.operator_name].config
        if 'distgit' in config and 'branch' in config['distgit']:
            return config['distgit']['branch']
        return self.runtime.group_config.branch.format(**self.runtime.group_config.vars)

    @property
    def rhpkg_opts(self):
        opts = self.runtime.rhpkg_config
        if hasattr(self.runtime, 'user') and self.runtime.user is not None:
            opts += ' --user {} '.format(self.runtime.user)
        return opts

    @property
    def list_of_manifest_files_to_be_copied(self):
        files = glob.glob('{}/*'.format(self.operator_bundle_dir))
        if not files:
            # 4.1 channel in package YAML is "preview" or "stable", but directory name is "4.1"
            files = glob.glob(
                '{}/{}/*'.format(
                    self.operator_manifests_dir,
                    '{MAJOR}.{MINOR}'.format(**self.runtime.group_config.vars),
                )
            )
        return files

    @property
    def operator_index_mode(self):
        mode = self.runtime.group_config.operator_index_mode or 'ga'  # default when missing
        if mode in {'pre-release', 'ga', 'ga-plus'}:
            # pre-release: label for pre-release operator index (unsupported)
            # ga: label for only this release's operator index
            # ga-plus: label for this release's operator index and future release indexes as well
            # [lmeyer 20240108] ref https://chat.google.com/room/AAAAZrx3KlI/6tf0phEdCF8
            # We may never use ga-plus, since the original motivation no longer seems important, and
            # it results in a problem: stage pushes for ga-plus v4.y fail when there is staged
            # v4.(y+1) content already with `skipVersion: v4.y` (because new v4.y content would be
            # immediately pruned). If we need `ga-plus` again, we can likely find a way around it.
            return mode
        self.runtime.logger.warning(f'{mode} is not a valid group_config.operator_index_mode')
        return 'ga'

    @property
    def redhat_delivery_tags(self):
        versions = 'v{MAJOR}.{MINOR}' if self.operator_index_mode == 'ga-plus' else '=v{MAJOR}.{MINOR}'

        labels = {
            'com.redhat.delivery.operator.bundle': 'true',
            'com.redhat.openshift.versions': versions.format(**self.runtime.group_config.vars),
        }
        if self.operator_index_mode == 'pre-release':
            labels['com.redhat.prerelease'] = 'true'
        return labels

    @property
    def operator_framework_tags(self):
        override_channel = self.channel
        override_default = self.channel
        stable_channel = 'stable'

        # see: issues.redhat.com/browse/ART-3107
        if self.runtime.group_config.operator_channel_stable in ['default', 'extra']:
            override_channel = ','.join({self.channel, stable_channel})
        if self.runtime.group_config.operator_channel_stable == 'default':
            override_default = stable_channel

        return {
            'operators.operatorframework.io.bundle.channel.default.v1': override_default,
            'operators.operatorframework.io.bundle.channels.v1': override_channel,
            'operators.operatorframework.io.bundle.manifests.v1': 'manifests/',
            'operators.operatorframework.io.bundle.mediatype.v1': 'registry+v1',
            'operators.operatorframework.io.bundle.metadata.v1': 'metadata/',
            'operators.operatorframework.io.bundle.package.v1': self.package,
        }

    @property
    def target(self):
        target_match = re.match(r'.*-rhel-(\d+)(?:-|$)', self.branch)
        if target_match:
            el_target = int(target_match.group(1))
            return self.runtime.get_default_candidate_brew_tag(el_target=el_target) or '{}-candidate'.format(
                self.branch
            )
        else:
            raise IOError(f'Unable to determine rhel version from branch: {self.branch}')

    @property
    def valid_subscription_label(self):
        return self.operator_csv_config['valid-subscription-label']

    def delivery_labels_match(self, bundle_image_pullspec):
        cmd = f"oc image info {bundle_image_pullspec} -o json"
        rc, out, err = exectools.cmd_gather(cmd)
        if rc != 0:
            raise ValueError(f"Error running {cmd}: {err}")
        image_info = json.loads(out)
        image_labels = image_info['config']['config']['Labels']
        for label, value in self.redhat_delivery_tags.items():
            if image_labels.get(label, '') != value:
                return False
        if self.operator_index_mode != 'pre-release':
            # It doesn't matter what the value of com.redhat.prerelease is
            # ET marks it as prerelease if the label is present
            if image_labels.get('com.redhat.prerelease', ''):
                return False
        return True
