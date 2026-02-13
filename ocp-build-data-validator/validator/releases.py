from typing import List, Optional

from . import support


def validate(data: dict) -> Optional[str]:
    self_referential_upgrades = get_self_referential_upgrades(data)
    if self_referential_upgrades:
        return f"The following releases contain references to themselves in their respective 'upgrades': {', '.join(self_referential_upgrades)}"


def get_self_referential_upgrades(releases_config: dict) -> List[str]:
    """
    Get a list of releases which contain themselves in their 'upgrades'
    """
    self_referential_releases = []
    for release, release_config in releases_config["releases"].items():
        full_release = release

        if release.startswith("ec") or release.startswith("rc"):
            # Construct full release name
            group_config = support.load_group_config_for("releases.yml")
            full_release = f"{group_config['vars']['MAJOR']}.{group_config['vars']['MINOR']}.0-{release}"

        upgrades_releases_str = release_config.get("assembly", {}).get("group", {}).get("upgrades", "")
        for upgrade_release in upgrades_releases_str.split(","):
            if upgrade_release.strip() == full_release:
                self_referential_releases.append(release)

    return self_referential_releases
