"""Tolerance for firmware-corrupted bucket keys.

The v3 firmware appends a ` .$version`/` ._sync` fragment to a bucket's
composite key when it parses certain PUT-response shapes, then echoes the
mutated key back in every subsequent request. A canonical bucket key
(`<type>.<serial>`) never contains a space, so everything from the first
space onward is corruption: stripping it resolves the request to the real
bucket and keeps corrupted variants from being stored as phantom buckets.
"""


def is_corrupted_key(object_key: str) -> bool:
    """True if object_key carries a firmware corruption suffix."""
    return " " in object_key


def canonical_object_key(object_key: str) -> str:
    """Return the canonical bucket key, stripping any corruption suffix."""
    return object_key.split(" ", 1)[0] if object_key else object_key
