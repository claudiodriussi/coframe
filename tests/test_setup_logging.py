"""Listening is the application's choice, made once.

The library speaks to the `coframe` logger and never installs a handler; an
application that wants the lines calls `setup_logging` at startup. Stdout
always (journald under systemd, the terminal by hand), a rotating file when
asked, and a second call replaces the first instead of doubling every line.
"""
import logging

import coframe.server_utils as srv


def _ours():
    return [h for h in logging.getLogger().handlers if getattr(h, '_coframe', False)]


def _teardown():
    root = logging.getLogger()
    for h in _ours():
        root.removeHandler(h)
        h.close()


def test_stdout_alone_by_default():
    try:
        srv.setup_logging('INFO')
        assert len(_ours()) == 1
        assert isinstance(_ours()[0], logging.StreamHandler)
        assert logging.getLogger().level == logging.INFO
    finally:
        _teardown()


def test_a_file_is_added_when_asked_and_carries_the_time(tmp_path):
    log = tmp_path / 'app.log'
    try:
        srv.setup_logging('DEBUG', str(log))
        logging.getLogger('coframe').info('hello')
        for h in _ours():
            h.flush()
        text = log.read_text()
        assert 'INFO coframe: hello' in text
        assert text[:4].isdigit()  # the timestamp: a file has no journald under it
        assert logging.getLogger().level == logging.DEBUG
    finally:
        _teardown()


def test_calling_twice_replaces_instead_of_doubling(tmp_path):
    try:
        srv.setup_logging('INFO')
        srv.setup_logging('WARNING', str(tmp_path / 'b.log'))
        assert len(_ours()) == 2
        assert logging.getLogger().level == logging.WARNING
    finally:
        _teardown()


def test_an_unknown_level_falls_back_to_info():
    try:
        srv.setup_logging('LOUD')
        assert logging.getLogger().level == logging.INFO
    finally:
        _teardown()
