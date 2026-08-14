"""When the generated model.py is stale.

`model.py` is generated from the plugin YAML, and regenerated when the YAML is
newer than it. The rule missed its other input: **the generator itself**. A
change to `source.py` or `db.py` produces a different model from the very same
YAML, so a `git pull` that alters the generation used to leave the old model in
place — with the new schema already applied to the database. Errors from that
mismatch point everywhere except at the cause.
"""
import os

from coframe.plugins import PluginsManager, _core_timestamp


def model_file(tmp_path, age: float):
    """A generated model whose mtime is `age` seconds newer than the core."""
    path = tmp_path / 'model.py'
    path.write_text('# generated\n')
    stamp = _core_timestamp() + age
    os.utime(path, (stamp, stamp))
    return str(path)


def test_the_core_counts_among_the_inputs():
    """A manager with no plugins at all still has a timestamp: the package."""
    manager = PluginsManager()

    assert _core_timestamp() > 0
    assert manager.get_timestamp() == _core_timestamp()


def test_a_model_older_than_the_generator_is_stale(tmp_path):
    manager = PluginsManager()

    assert manager.should_regenerate(model_file(tmp_path, age=-10))


def test_a_model_newer_than_everything_is_kept(tmp_path):
    manager = PluginsManager()

    assert not manager.should_regenerate(model_file(tmp_path, age=+10))


def test_a_missing_model_is_always_generated(tmp_path):
    assert PluginsManager().should_regenerate(str(tmp_path / 'absent.py'))
