import os
import yaml
import jinja2
import string
import traceback

from artcommonlib import exectools
from doozerlib.runtime import Runtime

GITHUB_TOKEN_SECRET_NAME = "github-access-token-ash"  # Secret name for GH token for openshift-priv
QUAY_REGISTRY_REPO_URL = "quay.io/rh_ee_asdas/konflux-test"
QUAY_REGISTRY_SECRET_NAME = "component-1"
KONFLUX_ROOT_WORKING_DIR = "/tmp/crap/konflux"


class KonfluxBuilder:
    def __init__(self, runtime: Runtime, distgit_name: str, namespace: str):
        self.runtime = runtime

        self.runtime.initialize(clone_distgits=False)
        self.namespace = namespace

        # An application resource name must start with a lower case alphabetical character,
        # be under 63 characters, and can only consist of lower case alphanumeric characters or ‘-’
        # Hence renaming openshift-4.16 to openshift-4-16 for example
        self.major_minor = runtime.group.split("-")[-1].replace(".", "-")
        self.application_name = runtime.group.replace(".", "-")  # Name of the Konflux application

        self.distgit_name = distgit_name
        self.image_meta = runtime.image_map[distgit_name]
        self.component_name = f"ocp-{self.major_minor}-{distgit_name}"  # Name of the Konflux component
        self.konflux_working_dir = f"{KONFLUX_ROOT_WORKING_DIR}/{distgit_name}"
        self.output_registry = f"{QUAY_REGISTRY_REPO_URL}:{self.component_name}"

        # https://github.com/openshift/coredns -> https://github.com/openshift-priv/coredns
        self.github_url = self.image_meta.config["content"]["source"]["git"]["web"].replace("openshift", "openshift-priv")

        self.initialize()

    @staticmethod
    def verify_secret(secret_name: str):
        command = f"oc get secret {secret_name}"
        exectools.cmd_assert(command)

    def initialize(self):
        command = f"oc project {self.namespace}"
        exectools.cmd_assert(command)

        # All secret names should be unique in the namespace
        # Check if secret for openshift-priv access is present
        self.verify_secret(QUAY_REGISTRY_SECRET_NAME)

        # Check if secret for quay registry access is present
        self.verify_secret(GITHUB_TOKEN_SECRET_NAME)

    @staticmethod
    def generate_file(file_path: str, data: dict, output_path: str):
        # Load the YAML template
        with open(file_path, "r") as file:
            template_content = file.read()

        # Create a Jinja2 template from the file content
        template = jinja2.Template(template_content)

        # Render the template with data
        rendered_content = template.render(data)

        # Load the rendered content as YAML
        yaml_data = yaml.safe_load(rendered_content)

        output_directory = os.path.dirname(output_path)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Dump the YAML data to a file
        with open(output_path, 'w') as file:
            yaml.safe_dump(yaml_data, file)

    def get_resource_uid(self, resource_name, name):
        command = f"oc get {resource_name} {name} -o yaml"
        self.runtime.logger.info(f"Running command: {command}")
        out, _ = exectools.cmd_assert(command)
        return yaml.safe_load(out)["metadata"]["uid"]

    def apply_generated_file(self, data, file_path, output_path, mode="apply"):
        """
        Apply the generated files to the cluster
        """
        self.generate_file(file_path=file_path, data=data, output_path=output_path)
        command = f"oc {mode} -f {output_path}"
        self.runtime.logger.info(f"Running command: {command}")
        exectools.cmd_assert(command)

    def _start_build(self):
        data = {
            "application": {
                "name": self.application_name
            },
            "namespace": self.namespace,

        }
        self.apply_generated_file(data, "doozer/static/konflux/application.yaml",
                                  f"{self.konflux_working_dir}/application.yaml")

        application_uid = self.get_resource_uid(resource_name="Application", name=self.application_name)
        data = {
            "registry_details": string.Template("""'{"image":"$registry_url","visibility":"$registry_visibility","secret":"$registry_secret"}'""").substitute(registry_url=QUAY_REGISTRY_REPO_URL, registry_visibility="private", registry_secret=QUAY_REGISTRY_SECRET_NAME),
            "namespace": self.namespace,
            "application": {
                "name": self.application_name,
                "uid": application_uid
            },
            "component": {
                "name": self.component_name
            },
            "git": {
                "token_name": GITHUB_TOKEN_SECRET_NAME,
                "context": "./",
                "url": self.github_url,
                "branch": f"art-<{self.runtime.group}>-assembly-<test>-dgk-<{self.distgit_name}>",
                "dockerfile_path": "Dockerfile"
            }
        }
        self.apply_generated_file(data, "doozer/static/konflux/component.yaml", f"{self.konflux_working_dir}/component.yaml")

        component_uid = self.get_resource_uid(resource_name="Component", name=self.component_name)
        data = {
            "namespace": self.namespace,
            "application": {
                "name": self.application_name
            },
            "component": {
                "name": self.component_name,
                "uid": component_uid
            },
            "git": {
                "token_name": GITHUB_TOKEN_SECRET_NAME,
                "context": ".",
                "url": self.github_url,
                "branch": f"art-<{self.runtime.group}>-assembly-<test>-dgk-<{self.distgit_name}>",
                "dockerfile_path": "Dockerfile"
            },
            "konflux": {
                "bundle_sha": "quay.io/redhat-appstudio-tekton-catalog/pipeline-docker-build:13ecd03ec9f7de811f837a5460c41105231c911a"
            },
            "output_registry_url": self.output_registry
        }

        self.apply_generated_file(data, "doozer/static/konflux/pipeline_run.yaml", f"{self.konflux_working_dir}/pipeline_run.yaml", mode="create")
        self.runtime.logger.info(f"Will push to registry {self.output_registry} on success")

        return ""  # Return pipeline run id

    async def build(self, image, retries: int = 3):
        dg = image.distgit_repo()
        logger = dg.logger
        for attempt in range(retries):
            logger.info("Build attempt %s/%s", attempt + 1, retries)
            try:
                await exectools.to_thread(self._start_build())
            except Exception as err:
                raise Exception(f"Error building image {image.name}: {str(err)}: {traceback.format_exc()}")

        return True



