from schema import And, Optional, Or, Regex, Schema, SchemaError

from validator.schema.modification_schema import modification

GIT_SSH_URL_REGEX = r"((git@[\w\.]+))([\w\.@\:/\-~]+)(\.git)(/)?"
GIT_WEB_URL_REGEX = r"https://github.com\/[a-z-]+/[a-z-]+"

valid_modes = [
    "auto",
    "disabled",
    "wip",
]

RPM_CONTENT_SCHEMA = {
    Optional("build"): {
        "use_source_tito_config": bool,
        "tito_target": And(str, len),
        "push_release_commit": bool,
    },
    Optional("source"): {
        Optional("alias"): And(str, len),
        Optional("git"): {
            "branch": {
                Optional("fallback"): And(str, len),
                Optional("stage"): And(str, len),
                "target": And(str, len),
            },
            "url": And(str, len, Regex(GIT_SSH_URL_REGEX)),
            "web": And(str, len, Regex(GIT_WEB_URL_REGEX)),
        },
        Optional("specfile"): Regex(r".+\.spec$"),
        Optional("modifications"): [modification],
    },
}

rpm_schema = Schema(
    {
        "content": RPM_CONTENT_SCHEMA,
        Optional("distgit"): {
            Optional("branch"): And(str, len),
        },
        Optional("enabled_repos"): [
            And(str, len),
        ],
        Optional("mode"): Or(*valid_modes),
        "name": And(str, len),
        "owners": [
            And(str, len),
        ],
        Optional("maintainer"): {
            Optional("product"): And(str, len),
            "component": And(str, len),
            Optional("subcomponent"): And(str, len),
        },
        Optional("targets"): [
            And(str, len),
        ],
        Optional("hotfix_targets"): [
            And(str, len),
        ],
        Optional("external_scanners"): {
            Optional("sast_scanning"): {
                Optional("jira_integration"): {
                    "enabled": And(bool),
                },
            },
        },
    }
)


def validate(_, data):
    try:
        rpm_schema.validate(data)
    except SchemaError as err:
        return "{}".format(err)
