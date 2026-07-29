"""Tests for the command dispatcher — CommandProcessor.send().

Every request of every server goes through here, and until now nothing covered
it: the existing tests call endpoint functions directly. These fix the observable
behaviour — what a caller passes in and what comes back — so the machinery
underneath can be changed without changing what the servers see.

The context tests earn their place: the dispatcher used to run each command in a
brand-new thread, so thread-local state started empty by construction. Serving
requests from a reused pool thread removes that guarantee, and only an explicit
set on every dispatch keeps one user's context out of the next user's request.
"""
import pytest

from coframe.db import BaseApp
from coframe.endpoints import CommandProcessor, endpoint, _ENDPOINTS


@pytest.fixture
def processor():
    """A processor with a few endpoints registered, cleaned up afterwards."""
    registered = []

    def register(name, func):
        _ENDPOINTS[name] = func
        registered.append(name)

    register('echo', lambda params: {'seen': params})
    register('boom', _raise)
    register('shaped', lambda params: {'status': 'error', 'message': 'nope', 'code': 422})
    register('context', lambda params: BaseApp.get_context())

    cp = CommandProcessor()
    cp.endpoints = dict(_ENDPOINTS)
    yield cp

    for name in registered:
        _ENDPOINTS.pop(name, None)
    BaseApp.set_context({})


def _raise(params):
    raise KeyError('missing thing')


def test_result_wraps_the_return_value(processor):
    result = processor.send({'operation': 'echo', 'parameters': {'a': 1}})

    assert result['status'] == 'success'
    assert result['code'] == 200
    assert result['data'] == {'seen': {'a': 1}}


def test_a_shaped_dict_is_passed_through(processor):
    """An endpoint may return its own status/code instead of a plain payload."""
    result = processor.send({'operation': 'shaped'})

    assert result['status'] == 'error'
    assert result['code'] == 422
    assert result['message'] == 'nope'


def test_unknown_operation_is_a_404(processor):
    result = processor.send({'operation': 'nowhere'})

    assert result['status'] == 'error'
    assert result['code'] == 404
    assert 'nowhere' in result['message']


def test_an_exception_becomes_a_500_with_its_type(processor):
    result = processor.send({'operation': 'boom'})

    assert result['status'] == 'error'
    assert result['code'] == 500
    assert result['error_type'] == 'KeyError'
    assert 'missing thing' in result['message']


def test_request_id_is_echoed_and_generated(processor):
    given = processor.send({'operation': 'echo', 'request_id': 'abc-123'})
    assert given['request_id'] == 'abc-123'

    auto = processor.send({'operation': 'echo'})
    assert auto['request_id'] and auto['request_id'] != 'abc-123'


def test_the_context_is_visible_to_the_endpoint(processor):
    result = processor.send({'operation': 'context', 'context': {'id': 7, 'username': 'ada'}})

    assert result['data'] == {'id': 7, 'username': 'ada'}


def test_a_command_without_context_does_not_inherit_the_previous_one(processor):
    """The guarantee that a pooled, reused thread makes load-bearing."""
    processor.send({'operation': 'context', 'context': {'id': 7, 'username': 'ada'}})

    result = processor.send({'operation': 'context'})

    assert result['data'] == {}, "context leaked from the previous command"


def test_results_are_not_retained(processor):
    """Nothing may accumulate per request: a dispatcher is not a cache."""
    for _ in range(50):
        processor.send({'operation': 'echo'})

    leftovers = [name for name in ('results', 'pending_commands') if getattr(processor, name, None)]
    assert not leftovers, f"the dispatcher kept state in {leftovers}"


def test_endpoint_decorator_registers_a_dispatchable_operation(processor):
    """Note the decorator registers the function and returns a wrapper, so this
    asserts on behaviour rather than on identity."""
    @endpoint('decorated')
    def _decorated(params):
        return 'ok'

    try:
        processor.endpoints = dict(_ENDPOINTS)
        assert processor.send({'operation': 'decorated'})['data'] == 'ok'
    finally:
        _ENDPOINTS.pop('decorated', None)
