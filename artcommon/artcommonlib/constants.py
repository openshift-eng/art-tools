# constants shared across multiple sub-projects

RHCOS_RELEASES_BASE_URL = "https://releases-rhcos-art.apps.ocp-virt.prod.psi.redhat.com/storage/releases"
BREW_HUB = "https://brewhub.engineering.redhat.com/brewhub"
BREW_DOWNLOAD_URL = "https://download.devel.redhat.com/brewroot"
RELEASE_SCHEDULES = "https://pp.engineering.redhat.com/api/v7/releases"

# Environment variables to disable Git stdin prompts for username, password, etc
GIT_NO_PROMPTS = {
    "GIT_SSH_COMMAND": "ssh -oBatchMode=yes",
    "GIT_TERMINAL_PROMPT": "0",
}
