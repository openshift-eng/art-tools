from typing import Dict, List, Optional, TextIO


def parse_record_log(file: TextIO) -> Dict[str, List[Dict[str, Optional[str]]]]:
    """
    Parse record.log from Doozer into a dict.
    The dict will be keyed by the type of operation performed.
    The values will be a list of dicts. Each of these dicts will contain the attributes for a single recorded operation of the top
    level key's type.
    """
    result = {}
    for line in file:
        fields = line.rstrip().split("|")
        type = fields[0]
        record = {
            entry_split[0]: entry_split[1] if len(entry_split) > 1 else None
            for entry_split in map(lambda entry: entry.split("=", 1), fields[1:])
            if entry_split[0]
        }
        result.setdefault(type, []).append(record)
    return result


def get_branch_protection_notify(record_log: dict) -> list[dict]:
    """
    Returns a list of branch_protection_missing records from the record log.
    Each entry contains distgit, source_url, branch, and owners.
    """
    return record_log.get("branch_protection_missing", [])


def get_failed_rpms(record_log: dict, full_record: bool = False) -> dict:
    """
    Returns a map of failed rpms
    distgit => task_url
    """

    rpms = record_log.get("build_rpm", [])
    failed_map = {}

    for rpm in rpms:
        distgit = rpm["distgit_key"]
        if rpm["status"] != "0":
            failed_map[distgit] = rpm if full_record else rpm.get("task_url", "No task URL available")
        else:
            # build may have succeeded later. If so, remove.
            if distgit in failed_map:
                del failed_map[distgit]

    return failed_map
