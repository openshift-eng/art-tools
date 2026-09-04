from schema import And, Optional, Or, Schema


def modification(file):
    valid_modification_actions = [
        'command',
        'replace',
        'add',
    ]

    return Schema(
        {
            'action': Or(*valid_modification_actions),
            Optional('command'): [str],
            Optional('match'): And(str, len),
            Optional('replacement'): Or(None, str),
            Optional('source'): And(str, len),
            Optional('path'): And(str, len),
            Optional('why'): And(str, len),  # consider making this required once it becomes customary
            Optional('overwriting'): bool,
        }
    ).validate(file)
