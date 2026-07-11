"""Golden / characterization tests for PluginsManager's config-merge machinery:
merge_dicts / _recursive_merge / _merge_lists / _detect_identity_key.

This is the RICH merge used to compose plugin YAML: it returns a new dict, tracks
key provenance via `$plugin` tags and a history map, and merges lists with
identity-aware semantics ($remove / $after / $before) — distinct from the simple
utils.deep_merge (see test_deep_merge.py).

These pin the *current* behaviour (quirks included) so refactors are safe. A
failure means the merge semantics changed — decide intentionally.
"""
import pytest

from coframe.plugins import PluginsManager


@pytest.fixture
def pm():
    return PluginsManager()


# --------------------------------------------------------------------------- #
# _detect_identity_key
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("items, expected", [
    ([{"id": 1}], "id"),
    ([{"name": "n"}], "name"),
    ([{"field": "f"}], "field"),
    ([{"group": "g"}], "group"),
    ([{"id": 1, "name": "n"}], "id"),          # priority: id before name
    ([{"name": "n", "field": "f"}], "name"),   # priority: name before field
    ([{"foo": 1}], None),                       # dict without a recognised key
    (["a", "b"], None),                         # pure scalar list
    ([], None),                                 # empty list
    ([42, {"id": 1}], "id"),                    # first *dict* item decides
])
def test_detect_identity_key(pm, items, expected):
    assert pm._detect_identity_key(items) == expected


# --------------------------------------------------------------------------- #
# _merge_lists — plain (no identity key) → append with equality-dedup
# --------------------------------------------------------------------------- #

def test_plain_string_list_dedup(pm):
    assert pm._merge_lists(["x", "y"], ["y", "z"], "p", "k") == ["x", "y", "z"]


def test_plain_int_list_dedup(pm):
    assert pm._merge_lists([1, 2], [2, 3], "p", "k") == [1, 2, 3]


def test_plain_unkeyed_dict_list_dedup_by_equality(pm):
    # No identity key → plain path; {'foo':1} deduped by full equality, no $plugin added.
    out = pm._merge_lists([{"foo": 1}], [{"foo": 1}, {"bar": 2}], "p", "k")
    assert out == [{"foo": 1}, {"bar": 2}]


# --------------------------------------------------------------------------- #
# _merge_lists — smart (identity key present)
# --------------------------------------------------------------------------- #

def test_smart_same_identity_overwrites_props_and_tags_plugin(pm):
    out = pm._merge_lists([{"id": "a", "v": 1}], [{"id": "a", "v": 9}], "p2", "k")
    assert out == [{"id": "a", "v": 9, "$plugin": "p2"}]


def test_smart_new_identity_appends_and_base_untouched(pm):
    # Untouched base items are NOT re-tagged; only the incoming item gets $plugin.
    out = pm._merge_lists([{"id": "a", "v": 1}], [{"id": "b", "v": 2}], "p2", "k")
    assert out == [{"id": "a", "v": 1}, {"id": "b", "v": 2, "$plugin": "p2"}]


def test_smart_remove_drops_item(pm):
    out = pm._merge_lists(
        [{"id": "a"}, {"id": "b"}], [{"id": "a", "$remove": True}], "p2", "k")
    assert out == [{"id": "b"}]


def test_smart_after_inserts_following_anchor(pm):
    out = pm._merge_lists(
        [{"id": "a"}, {"id": "b"}, {"id": "c"}],
        [{"id": "x", "$after": "a"}], "p", "k")
    assert [i["id"] for i in out] == ["a", "x", "b", "c"]
    assert out[1] == {"id": "x", "$plugin": "p"}   # directive stripped, plugin tagged


def test_smart_before_inserts_preceding_anchor(pm):
    out = pm._merge_lists(
        [{"id": "a"}, {"id": "b"}, {"id": "c"}],
        [{"id": "y", "$before": "b"}], "p", "k")
    assert [i["id"] for i in out] == ["a", "y", "b", "c"]


def test_smart_after_takes_precedence_over_before(pm):
    out = pm._merge_lists(
        [{"id": "a"}, {"id": "b"}, {"id": "c"}],
        [{"id": "z", "$after": "a", "$before": "c"}], "p", "k")
    assert [i["id"] for i in out] == ["a", "z", "b", "c"]   # $after wins


def test_smart_missing_anchor_appends_at_end(pm):
    out = pm._merge_lists(
        [{"id": "a"}], [{"id": "x", "$after": "nope"}], "p", "k")
    assert [i["id"] for i in out] == ["a", "x"]


def test_smart_merge_by_name_when_no_id(pm):
    out = pm._merge_lists([{"name": "a", "v": 1}], [{"name": "a", "v": 2}], "p", "k")
    assert out == [{"name": "a", "v": 2, "$plugin": "p"}]


# --------------------------------------------------------------------------- #
# merge_dicts / _recursive_merge — provenance & structure
# --------------------------------------------------------------------------- #

def test_new_scalar_key_has_no_plugin_tag(pm):
    assert pm.merge_dicts({"a": 1}, "p1") == {"a": 1}


def test_new_dict_key_gets_plugin_tag(pm):
    assert pm.merge_dicts({"d": {"x": 1}}, "p1") == {"d": {"x": 1, "$plugin": "p1"}}


def test_provenance_stays_with_original_definer(pm):
    pm.merge_dicts({"d": {"x": 1}}, "p1")
    data = pm.merge_dicts({"d": {"y": 2}}, "p2")
    assert data == {"d": {"x": 1, "$plugin": "p1", "y": 2}}   # $plugin stays p1


def test_overlapping_scalar_last_writer_wins(pm):
    pm.merge_dicts({"a": 1}, "p1")
    assert pm.merge_dicts({"a": 2}, "p2") == {"a": 2}


def test_incompatible_types_raise_typeerror(pm):
    pm.merge_dicts({"a": 1}, "p1")
    with pytest.raises(TypeError):
        pm.merge_dicts({"a": [1]}, "p2")


def test_new_dict_list_key_tags_each_item(pm):
    data = pm.merge_dicts({"items": [{"id": "a"}]}, "p1")
    assert data["items"] == [{"id": "a", "$plugin": "p1"}]


def test_list_merge_through_merge_dicts(pm):
    pm.merge_dicts({"items": [{"id": "a", "v": 1}]}, "p1")
    data = pm.merge_dicts({"items": [{"id": "a", "v": 9}, {"id": "b", "v": 2}]}, "p2")
    assert data["items"] == [
        {"id": "a", "v": 9, "$plugin": "p2"},
        {"id": "b", "v": 2, "$plugin": "p2"},
    ]


# --------------------------------------------------------------------------- #
# history tracking
# --------------------------------------------------------------------------- #

def test_history_tracks_definers_per_key_path(pm):
    pm.merge_dicts({"d": {"x": 1}}, "p1")
    pm.merge_dicts({"d": {"y": 2}}, "p2")
    assert pm.history["d"] == ["p1", "p2"]
    assert pm.history["d.x"] == ["p1"]
    assert pm.history["d.y"] == ["p2"]


# --------------------------------------------------------------------------- #
# custom merge handlers
# --------------------------------------------------------------------------- #

def test_exact_merge_handler_overrides_list_merge(pm):
    pm.register_merge_handler("items", lambda base, new, plugin: ["CUSTOM"])
    pm.merge_dicts({"items": [{"id": "a"}]}, "p1")
    data = pm.merge_dicts({"items": [{"id": "b"}]}, "p2")
    assert data["items"] == ["CUSTOM"]


def test_wildcard_merge_handler_matches(pm):
    pm.register_merge_handler("tables.*.columns", lambda base, new, plugin: ["W"])
    pm.merge_dicts({"tables": {"Book": {"columns": [{"id": "a"}]}}}, "p1")
    data = pm.merge_dicts({"tables": {"Book": {"columns": [{"id": "b"}]}}}, "p2")
    assert data["tables"]["Book"]["columns"] == ["W"]
