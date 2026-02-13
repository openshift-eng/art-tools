from ruamel.yaml import YAML


def validate(contents):
    yaml = YAML(typ="safe")
    try:
        return (yaml.load(contents), None)
    except Exception as err:
        return (None, "{}".format(err))
