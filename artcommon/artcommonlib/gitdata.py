import io
import os
import shutil
import string
import urllib.parse

import ruamel.yaml
import ruamel.yaml.util
import yaml
from artcommonlib import exectools
from artcommonlib.constants import GIT_NO_PROMPTS
from artcommonlib.logutil import get_logger
from artcommonlib.pushd import Dir
from future.utils import as_native_str

SCHEMES = ["ssh", "ssh+git", "http", "https"]


class SafeFormatter(string.Formatter):
    """
    A string formatter that safely substitutes variables, leaving unfound ones as-is.

    Example:
        formatter = SafeFormatter()
        result = formatter.format("Hello {name}, version {version}, missing {unknown}",
                                  name="World", version="1.0")
        # Result: "Hello World, version 1.0, missing {unknown}"
    """

    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            try:
                return kwargs[key]
            except KeyError:
                return "{" + key + "}"
        else:
            return string.Formatter.get_value(key, args, kwargs)


class GitDataException(Exception):
    """A broad exception for errors during GitData operations"""

    pass


class GitDataBranchException(GitDataException):
    pass


class GitDataPathException(GitDataException):
    pass


class DataObj(object):
    def __init__(self, key, path, data):
        self.key = key
        self.path = path
        self.base_dir = os.path.dirname(self.path)
        self.filename = self.path.replace(self.base_dir, "").strip("/")
        self.data = data
        self.indent = 2
        self.block_seq_indent = None

    @as_native_str()
    def __repr__(self):
        result = {
            "key": self.key,
            "path": self.path,
            "data": self.data,
        }
        return str(result)

    def reload(self):
        with open(self.path, "r") as f:
            # Reload with ruamel.yaml and guess the indent.
            self.data, self.indent, self.block_seq_indent = ruamel.yaml.util.load_yaml_guess_indent(
                f, preserve_quotes=True
            )

    def save(self):
        with open(self.path, "w") as f:
            # pyyaml doesn't preserve the order of keys or comments when loading and saving yamls.
            # Save with ruamel.yaml instead to keep the format as much as possible.
            ruamel.yaml.round_trip_dump(self.data, f, indent=self.indent, block_seq_indent=self.block_seq_indent)


class GitData(object):
    def __init__(
        self,
        data_path=None,
        clone_dir="./",
        commitish="master",
        sub_dir=None,
        exts=["yaml", "yml", "json"],
        reclone=False,
        logger=None,
    ):
        """
        Load structured data from a git source.
        :param str data_path: Git url (git/http/https) or local directory path
        :param str clone_dir: Location to clone data into
        :param str commitish: Repo branch (tag or sha also allowed) to checkout
        :param str sub_dir: Sub dir in data to treat as root
        :param list exts: List of valid extensions to search for in data, with out period
        :param reclone: If a clone is already present, remove it and reclone latest.
        :param logger: Python logging object to use
        :raises GitDataException:
        """
        self.logger = logger
        if logger is None:
            self.logger = get_logger(__name__)

        self.clone_dir = clone_dir
        self.branch = commitish

        self.remote_path = None
        self.sub_dir = sub_dir
        self.exts = ["." + e.lower() for e in exts]
        self.commit_hash = None
        self.origin_url = None
        self.reclone = reclone
        if data_path:
            self.clone_data(data_path)

    def clone_data(self, data_path):
        """
        Clones data for given data_path:
        :param str data_path: Git url (git/http/https) or local directory path
        """

        # Remove trailing slash to prevent GitDataBranchException:
        self.data_path = data_path.rstrip("/")

        data_url = urllib.parse.urlparse(self.data_path)
        if data_url.scheme in SCHEMES or (data_url.scheme == "" and ":" in data_url.path):
            data_name = os.path.splitext(os.path.basename(data_url.path))[0]
            data_destination = os.path.join(self.clone_dir, data_name)
            clone_data = True

            if self.reclone and os.path.isdir(data_destination):
                shutil.rmtree(data_destination)

            if os.path.isdir(data_destination):
                self.logger.info("Data clone directory already exists, checking commit sha")
                with Dir(data_destination):
                    # check the current status of what's local
                    rc, out, err = exectools.cmd_gather("git status -sb")
                    if rc:
                        raise GitDataException("Error getting data repo status: {}".format(err))

                    lines = out.strip().split("\n")
                    synced = "ahead" not in lines[0] and "behind" not in lines[0] and len(lines) == 1

                    # check if there are unpushed
                    # verify local branch
                    rc, out, err = exectools.cmd_gather("git rev-parse --abbrev-ref HEAD")
                    if rc:
                        raise GitDataException("Error checking local branch name: {}".format(err))
                    branch = out.strip()
                    if branch != self.branch:
                        if not synced:
                            msg = (
                                "Local branch is `{}`, but requested `{}` and you have uncommitted/pushed changes\n"
                                "You must either clear your local data or manually checkout the correct branch."
                            ).format(branch, self.branch)
                            raise GitDataBranchException(msg)
                    else:
                        # Check if local is synced with remote
                        rc, out, err = exectools.cmd_gather(["git", "ls-remote", self.data_path, self.branch])
                        if rc:
                            raise GitDataException("Unable to check remote sha: {}".format(err))
                        remote = out.strip().split("\t")[0]
                        try:
                            exectools.cmd_assert("git branch --contains {}".format(remote))
                            self.logger.info("{} is already cloned and latest".format(self.data_path))
                            clone_data = False
                        except:
                            if not synced:
                                msg = (
                                    "Local data is out of sync with remote and you have unpushed commits: {}\n"
                                    "You must either clear your local data\n"
                                    "or manually rebase from latest remote to continue"
                                ).format(data_destination)
                                raise GitDataException(msg)

            if clone_data:
                if os.path.isdir(data_destination):  # delete if already there
                    shutil.rmtree(data_destination)
                self.logger.info("Cloning config data from {}".format(self.data_path))
                if not os.path.isdir(data_destination):
                    # Clone all branches as we must sometimes reference master /OWNERS for maintainer information
                    cmd = "git clone --no-single-branch {} {}".format(self.data_path, data_destination)
                    exectools.cmd_assert(cmd, set_env=GIT_NO_PROMPTS)
                    exectools.cmd_assert(f"git -C {data_destination} checkout {self.branch}", set_env=GIT_NO_PROMPTS)

            self.remote_path = self.data_path
            self.data_path = data_destination
        elif data_url.scheme in ["", "file"]:
            self.remote_path = None
            self.data_path = os.path.abspath(self.data_path)  # just in case relative path was given
        else:
            raise ValueError(
                "Invalid data_path: {} - invalid scheme: {}".format(self.data_path, data_url.scheme),
            )

        if self.sub_dir:
            self.data_dir = os.path.join(self.data_path, self.sub_dir)
        else:
            self.data_dir = self.data_path

        self.origin_url, _ = exectools.cmd_assert(f"git -C {self.data_path} remote get-url origin", strip=True)
        self.commit_hash, _ = exectools.cmd_assert(f"git -C {self.data_path} rev-parse HEAD", strip=True)
        ref, _ = exectools.cmd_assert(f"git -C {self.data_path} rev-parse --abbrev-ref HEAD", strip=True)
        ref_s = f"({ref})" if ref != "HEAD" else ""
        self.logger.info(f"On commit: {self.commit_hash} {ref_s}")

        if not os.path.isdir(self.data_dir):
            raise GitDataPathException("{} is not a valid sub-directory in the data".format(self.sub_dir))

    def load_yaml_file(self, file_path, strict=True):
        full_path = os.path.join(self.data_dir, file_path)
        if not os.path.isfile(full_path):
            if strict:
                raise ValueError(f"Could not find file at {full_path}")
            else:
                return None

        with io.open(full_path, "r", encoding="utf-8") as f:
            raw_text = f.read()

        try:
            data = yaml.full_load(raw_text)
        except Exception as e:
            raise ValueError(f"error parsing file {full_path}: {e}")

        return data

    def load_data(self, path="", key=None, keys=None, exclude=None, filter_funcs=None, replace_vars=None):
        full_path = os.path.join(self.data_dir, path.replace("\\", "/"))
        if path and not os.path.isdir(full_path):
            raise GitDataPathException('Cannot find "{}" under "{}"'.format(path, self.data_dir))

        if filter_funcs is not None and not isinstance(filter_funcs, list):
            filter_funcs = [filter_funcs]

        if exclude is not None and not isinstance(exclude, list):
            exclude = [exclude]

        if key and keys:
            raise GitDataException("Must use key or keys, but not both!")

        if key:
            keys = [key]

        if keys:
            if not isinstance(keys, list):
                keys = [keys]
            files = []
            for k in keys:
                for ext in self.exts:
                    path = k + ext
                    if os.path.isfile(os.path.join(full_path, k + ext)):
                        files.append(path)
                        break  # found for this key, move on
        else:
            files = os.listdir(full_path)

        result = {}

        for name in files:
            base_name, ext = os.path.splitext(name)
            if ext.lower() in self.exts:
                data_file = os.path.join(full_path, name)
                if os.path.isfile(data_file):
                    with io.open(data_file, "r", encoding="utf-8") as f:
                        raw_text = f.read()
                        if replace_vars:
                            # Use safe substitution - replace found vars, leave unfound ones as-is
                            try:
                                formatter = SafeFormatter()
                                raw_text = formatter.format(raw_text, **replace_vars)
                            except Exception as e:
                                self.logger.warning(
                                    "Error applying template substitution to {}: {}".format(data_file, e)
                                )
                        try:
                            data = yaml.full_load(raw_text)
                        except Exception as e:
                            raise ValueError(f"error parsing file {data_file}: {e}")
                        use = True
                        if exclude and base_name in exclude:
                            use = False

                        if use and filter_funcs:
                            for func in filter_funcs:
                                use &= func(base_name, data)
                                if not use:
                                    break

                        if use:
                            result[base_name] = DataObj(base_name, data_file, data)

        if key and key in result:
            result = result[key]

        return result

    def commit(self, msg):
        """
        Commit outstanding data changes
        """
        self.logger.info("Commit config: {}".format(msg))
        with Dir(self.data_path):
            exectools.cmd_assert("git add .")
            exectools.cmd_assert('git commit --allow-empty -m "{}"'.format(msg))

    def push(self):
        """
        Push changes back to data repo.
        Will of course fail if user does not have write access.
        """
        self.logger.info("Pushing config...")
        with Dir(self.data_path):
            exectools.cmd_assert("git push")
