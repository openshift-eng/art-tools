from typing import Optional


def validate(data: dict) -> Optional[str]:
    errors = []

    alias_collision_error = get_alias_collisions(data)
    if alias_collision_error:
        errors.append(alias_collision_error)

    if errors:
        return "; ".join(errors)


def get_alias_collisions(streams_data: dict) -> Optional[str]:
    """
    Check for collisions between top-level stream names and alias values.

    A collision occurs when:
    - An alias value matches a top-level stream name
    - The same alias value appears in multiple streams
    """
    stream_names = set(streams_data.keys())
    alias_to_stream = {}  # Maps each alias value to the stream that defines it
    collisions = []

    for stream_name, stream_config in streams_data.items():
        aliases = stream_config.get("aliases", [])
        if not aliases:
            continue

        for alias in aliases:
            # Check if alias collides with a top-level stream name
            if alias in stream_names:
                collisions.append(f"Alias '{alias}' in stream '{stream_name}' collides with a top-level stream name")

            # Check if alias is defined by another stream
            if alias in alias_to_stream and alias_to_stream[alias] != stream_name:
                collisions.append(
                    f"Alias '{alias}' is defined in both stream '{alias_to_stream[alias]}' and stream '{stream_name}'"
                )
            elif alias not in alias_to_stream:
                alias_to_stream[alias] = stream_name

    if collisions:
        return "Stream alias collisions found: " + "; ".join(collisions)
