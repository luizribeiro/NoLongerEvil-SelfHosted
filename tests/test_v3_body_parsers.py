"""Tests for v3-firmware (Format 3) body parsing in parse_subscribe_body
and parse_put_body.

v3-firmware devices (Display-2.14 / firmware 4.3.3) send:
- subscribe: {"keys": [{"key": "device.SERIAL"}, {"key": "shared.SERIAL"}]}
- put:      {"shared": {"SERIAL": {fields}}, "device": {"SERIAL": {fields}}}

These tests cover the new format-3 branches without disturbing the existing
format-1 and format-2 paths.
"""

from nolongerevil.routes.nest.transport import parse_put_body, parse_subscribe_body

SERIAL = "02AA01AB501203EQ"


# ---------------------------------------------------------------------------
# parse_subscribe_body — Format 3 (v3 keys-only)
# ---------------------------------------------------------------------------


def test_parse_subscribe_v3_keys_translates_to_objects() -> None:
    body = {"keys": [{"key": f"device.{SERIAL}"}, {"key": f"shared.{SERIAL}"}]}
    session, _chunked, objects = parse_subscribe_body(body)
    assert session == ""
    assert [o["object_key"] for o in objects] == [f"device.{SERIAL}", f"shared.{SERIAL}"]


def test_parse_subscribe_v3_keys_forces_chunked() -> None:
    """v3 long-poll semantics: server MUST chunk-stream even though the
    request doesn't carry chunked=true."""
    body = {"keys": [{"key": f"device.{SERIAL}"}]}
    _session, chunked, _objects = parse_subscribe_body(body)
    assert chunked is True


def test_parse_subscribe_v3_keys_drops_malformed_entries() -> None:
    body = {
        "keys": [
            {"key": f"device.{SERIAL}"},
            {"not_a_key": "ignored"},
            "not_a_dict",
            {"key": 42},
        ]
    }
    _session, _chunked, objects = parse_subscribe_body(body)
    assert [o["object_key"] for o in objects] == [f"device.{SERIAL}"]


def test_parse_subscribe_objects_array_still_works() -> None:
    body = {
        "chunked": False,
        "objects": [{"object_key": f"device.{SERIAL}", "object_revision": 12}],
    }
    _session, chunked, objects = parse_subscribe_body(body)
    assert chunked is False
    assert objects[0]["object_revision"] == 12


def test_parse_subscribe_named_fields_still_works() -> None:
    body = {
        "chunked": True,
        "device": {"object_key": f"device.{SERIAL}", "object_revision": 5},
        "shared": {"object_key": f"shared.{SERIAL}", "object_revision": 3},
    }
    _session, chunked, objects = parse_subscribe_body(body)
    assert chunked is True
    keys = sorted(o["object_key"] for o in objects)
    assert keys == [f"device.{SERIAL}", f"shared.{SERIAL}"]


# ---------------------------------------------------------------------------
# parse_put_body — Format 3 (v3 nested-by-bucket-type-then-serial)
# ---------------------------------------------------------------------------


def test_parse_put_v3_nested_extracts_inline_fields() -> None:
    body = {
        "shared": {SERIAL: {"target_temperature": 21.5, "current_humidity": 45}},
        "device": {SERIAL: {"away": False}},
    }
    _session, objects = parse_put_body(body)
    by_key = {o["object_key"]: o for o in objects}
    assert by_key[f"shared.{SERIAL}"]["value"] == {
        "target_temperature": 21.5,
        "current_humidity": 45,
    }
    assert by_key[f"device.{SERIAL}"]["value"] == {"away": False}


def test_parse_put_v3_nested_strips_metadata_fields_from_value() -> None:
    body = {
        "shared": {
            SERIAL: {
                "object_key": f"shared.{SERIAL}",  # metadata field
                "base_object_revision": 7,
                "if_object_revision": 7,
                "target_temperature": 22.0,
            }
        }
    }
    _session, objects = parse_put_body(body)
    obj = objects[0]
    assert obj["object_key"] == f"shared.{SERIAL}"
    assert obj["base_object_revision"] == 7
    assert obj["if_object_revision"] == 7
    assert obj["value"] == {"target_temperature": 22.0}


def test_parse_put_v3_nested_ignores_unknown_bucket_types() -> None:
    body = {
        "shared": {SERIAL: {"target_temperature": 21.5}},
        "not_a_bucket_type": {SERIAL: {"junk": True}},
    }
    _session, objects = parse_put_body(body)
    assert [o["object_key"] for o in objects] == [f"shared.{SERIAL}"]


def test_parse_put_objects_array_still_works() -> None:
    body = {
        "session": "abc",
        "objects": [{"object_key": f"shared.{SERIAL}", "value": {"target_temperature": 21}}],
    }
    session, objects = parse_put_body(body)
    assert session == "abc"
    assert objects[0]["value"]["target_temperature"] == 21


def test_parse_put_bucket_keyed_still_works() -> None:
    body = {
        "session": "abc",
        f"shared.{SERIAL}": {
            "object_key": f"shared.{SERIAL}",
            "target_temperature": 21.5,
        },
    }
    _session, objects = parse_put_body(body)
    assert objects[0]["object_key"] == f"shared.{SERIAL}"
    assert objects[0]["value"] == {"target_temperature": 21.5}


def test_parse_put_v3_nested_does_not_trigger_when_objects_present() -> None:
    """If 'objects' is present, ignore the nested format even if buckets are
    also present at the top level."""
    body = {
        "objects": [{"object_key": f"shared.{SERIAL}", "value": {"target_temperature": 19}}],
        "shared": {SERIAL: {"target_temperature": 99}},
    }
    _session, objects = parse_put_body(body)
    assert len(objects) == 1
    assert objects[0]["value"]["target_temperature"] == 19
