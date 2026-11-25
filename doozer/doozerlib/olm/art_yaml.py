from collections.abc import Mapping, Sequence

from ruamel.yaml import YAML


def render_art_yaml(
    template: str, version: str, release: str, overrides: Sequence[dict], *, group_config: Mapping[str, object]
) -> dict:
    """Render art.yaml with per-image overrides to its build-derived template variables.

    Overrides require the group switch to be enabled in openshift-4.23.
    Search-only overrides apply to `updates[].update_list[].search` and take precedence
    over general overrides there. Each variable, including FULL_VER, is independent.
    """
    version = version.removeprefix('v')
    major, minor, subminor = version.split('.')[:3]
    date_time = release.split('.')[0]
    variables = {
        'MAJOR': major,
        'MINOR': minor,
        'SUBMINOR': subminor,
        'RELEASE': release,
        'DATE_TIME': date_time,
        'FULL_VER': f'{version}-{date_time}',
    }
    search_overrides = {}
    # Merged group metadata can include group.ext.yml, which group.yml validation does not cover.
    if group_config.get('art-yaml-overrides-enabled') is True and group_config.get('name') == 'openshift-4.23':
        for override in overrides:
            scope = search_overrides if override.get('search_only', False) else variables
            scope[override['name']] = override['value']

    yaml = YAML(typ='safe')
    rendered = yaml.load(template.format_map(variables))
    if not search_overrides or not isinstance(rendered, dict) or not isinstance(rendered.get('updates'), list):
        return rendered

    # Keep the normal render for filenames and replacements, including unquoted
    # placeholders. Read only the search strings from the original YAML.
    original = yaml.load(template)
    search_variables = variables | search_overrides

    # Copy rules and update blocks that YAML aliases may share with other sections.
    updates = []
    for update, original_update in zip(rendered['updates'], original['updates'], strict=True):
        rules = update.get('update_list')
        if isinstance(rules, list):
            scoped_rules = []
            for rule, original_rule in zip(rules, original_update['update_list'], strict=True):
                search = original_rule['search'].format_map(search_variables)
                scoped_rules.append({**rule, 'search': search})
            update = {**update, 'update_list': scoped_rules}
        updates.append(update)
    rendered['updates'] = updates
    return rendered
