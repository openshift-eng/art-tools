import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import tomli
from artcommonlib import model, runtime
from artcommonlib.exectools import cmd_gather_async

from pyartcd import constants, jenkins, util
from pyartcd.jira_client import JIRAClient
from pyartcd.mail import MailService
from pyartcd.slack import SlackClient


class Runtime:
    def __init__(self, config: Dict[str, Any], working_dir: Path, dry_run: bool):
        self.config = config
        self.working_dir = working_dir
        self.doozer_working = os.path.abspath(f"{self.working_dir / 'doozer_working'}")
        self.dry_run = dry_run
        self.logger = logging.getLogger("pyartcd")

        # checks working_dir
        if not self.working_dir.is_dir():
            raise IOError(f"Working directory {self.working_dir.absolute()} doesn't exist.")

    @classmethod
    def from_config_file(cls, config_filename: Path, working_dir: Path, dry_run: bool):
        with open(config_filename, "rb") as config_file:
            config_dict = tomli.load(config_file)
        return Runtime(config=config_dict, working_dir=working_dir, dry_run=dry_run)

    def new_jira_client(self, jira_token: Optional[str] = None):
        if not jira_token:
            jira_token = os.environ.get("JIRA_TOKEN")
            if not jira_token:
                raise ValueError("JIRA_TOKEN environment variable is not set")
        return JIRAClient.from_url(self.config["jira"]["url"], token_auth=jira_token)

    def new_slack_client(self, token: Optional[str] = None):
        if not token and not self.dry_run:
            token = os.environ.get("SLACK_BOT_TOKEN")
            if not token and not self.dry_run:
                raise ValueError("SLACK_BOT_TOKEN environment variable is not set")
        return SlackClient(
            token,
            dry_run=self.dry_run,
            job_name=jenkins.get_job_name(),
            build_url=jenkins.get_build_url(),
            build_id=jenkins.get_build_id(),
        )

    def new_mail_client(self):
        return MailService.from_config(self.config)

    async def cleanup_sources(self, source_dir_name: str):
        """
        Removes the git sources cloned by Doozer using rsync
        """

        source_path = f"{self.doozer_working}/{source_dir_name}"
        self.logger.info("About to remove source dir %s...", source_path)
        empty_to_overwrite_path = f"{self.doozer_working}/empty_to_overwrite/"

        # Create the empty directory and delete the source dir with rsync --delete
        Path(empty_to_overwrite_path).mkdir(parents=True, exist_ok=True)
        cmd = ["rsync", "-a", "--delete", f"{empty_to_overwrite_path}", source_path]
        self.logger.info("Executing rsync delete operation: %s", " ".join(cmd))
        start_time = datetime.now()
        await cmd_gather_async(cmd)
        self.logger.info("Deleted %s in %s seconds", source_path, (datetime.now() - start_time).seconds)


class GroupRuntime(runtime.GroupRuntime):
    @classmethod
    async def create(cls, *args, **kwargs):
        """Async instantiation that populates group_config"""
        self = cls(*args, **kwargs)

        self._group_config = model.Model(
            await util.load_group_config(
                self.group,
                self.assembly,
                None,
                self.doozer_data_path,
                self.doozer_data_gitref,
            )
        )
        return self

    def __init__(
        self,
        config: Dict[str, Any],
        working_dir: Path,
        group: str,
        assembly: str = "test",
        doozer_data_path: str = constants.OCP_BUILD_DATA_URL,
        doozer_data_gitref: str = "",
    ):
        self.config = config
        self.working_dir = working_dir
        self.group = group
        self.assembly = assembly
        self.doozer_data_path = doozer_data_path
        self.doozer_data_gitref = doozer_data_gitref
        self._group_config = model.Missing  # stub

    @property
    def group_config(self):
        return self._group_config
