"""Registering coframe's routes — on its own application, and inside someone else's.

`register_flask` and `register_fastapi` take whatever target they are given, so
the same call serves the two deployments that until now were one: coframe as
the owner of the process, and coframe as a guest of an application that already
exists. The tests below are mostly about the second one, because it is the one
nothing exercised: what has to be proven is not that the routes answer, but
that **nothing outside them moved** — the host's routes, its hooks, and its
JSON contract, which in the deployment that prompted this belongs to an API
already in production on handheld terminals.

No database and no plugin tree here: the registration layer only ever touches
`coframe_app.cp` and `plugins.config`, and driving it with stubs is what keeps
these tests about routing instead of about everything else.
"""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import jwt
import pytest

import coframe.server_utils as srv
from coframe.endpoints import CommandProcessor, _ENDPOINTS

SECRET = 'test-secret-key'

CONFIG = {
    'name': 'routes-test',
    'version': '1.0',
    'api': {'prefix': 'coframe', 'endpoint_prefix': 'endpoint'},
    'authentication': {'context_fields': ['id', 'username']},
}


@pytest.fixture
def coframe_app():
    """A stub application: a command processor with a few endpoints on it."""
    registered = []

    def register(name, func):
        _ENDPOINTS[name] = func
        registered.append(name)

    register('echo', lambda params: {'seen': params})
    register('a_date', lambda params: {'when': date(2026, 8, 24),
                                       'at': datetime(2026, 8, 24, 10, 30)})
    # Keys of mixed type: a descriptor carries them, and sorting them raises.
    register('mixed_keys', lambda params: {1: 'one', 'two': 2})
    register('boom', _raise)

    cp = CommandProcessor()
    cp.endpoints = dict(_ENDPOINTS)
    yield SimpleNamespace(cp=cp)

    for name in registered:
        _ENDPOINTS.pop(name, None)


def _raise(params):
    raise KeyError('missing thing')


@pytest.fixture
def plugins():
    return SimpleNamespace(config=dict(CONFIG))


def token(last_refresh=None, **claims):
    """A valid bearer token; `last_refresh=0` is one due for a refresh."""
    now = datetime.now(timezone.utc)
    payload = {
        'id': 1,
        'username': 'tester',
        'exp': now + timedelta(hours=1),
        'last_refresh': now.timestamp() if last_refresh is None else last_refresh,
    }
    payload.update(claims)
    return jwt.encode(payload, SECRET, algorithm='HS256')


def bearer(**kwargs):
    return {'Authorization': f'Bearer {token(**kwargs)}'}


# ── Flask, owning the process ─────────────────────────────────────────────────

@pytest.fixture
def flask_client(coframe_app, plugins):
    """coframe registered straight onto the application, as a server does."""
    from flask import Flask

    app = Flask(__name__)
    srv.register_flask(app, coframe_app, plugins, SECRET)
    return app.test_client()


def test_the_dispatcher_carries_an_operation(flask_client):
    res = flask_client.post('/coframe/endpoint/echo', json={'a': 1},
                            headers=bearer())

    assert res.status_code == 200
    assert res.get_json() == {'status': 'success', 'data': {'seen': {'a': 1}},
                              'status_code': 200}


def test_info_lives_under_the_prefix(flask_client):
    """Not at the root: there it would collide with a host that has one."""
    assert flask_client.get('/coframe/info').status_code == 200
    assert flask_client.get('/info').status_code == 404


def test_no_token_is_refused(flask_client):
    res = flask_client.post('/coframe/endpoint/echo', json={})

    assert res.status_code == 401
    assert res.get_json()['status'] == 'error'


def test_an_invalid_token_is_refused(flask_client):
    res = flask_client.post('/coframe/endpoint/echo', json={},
                            headers={'Authorization': 'Bearer not-a-token'})

    assert res.status_code == 401


def test_a_failing_endpoint_answers_with_its_status(flask_client):
    res = flask_client.post('/coframe/endpoint/boom', json={}, headers=bearer())

    assert res.status_code >= 400
    assert res.get_json()['status'] == 'error'


def test_a_stale_token_comes_back_refreshed(flask_client):
    res = flask_client.post('/coframe/endpoint/echo', json={},
                            headers=bearer(last_refresh=0))

    assert res.status_code == 200
    assert 'X-New-Token' in res.headers
    assert jwt.decode(res.headers['X-New-Token'], SECRET,
                      algorithms=['HS256'])['username'] == 'tester'


def test_a_fresh_token_is_left_alone(flask_client):
    res = flask_client.post('/coframe/endpoint/echo', json={}, headers=bearer())

    assert 'X-New-Token' not in res.headers


def test_dates_go_out_as_iso(flask_client):
    """ISO 8601, the form the endpoints accept back on write — and without
    replacing the application's JSON provider to get it."""
    res = flask_client.post('/coframe/endpoint/a_date', json={}, headers=bearer())

    assert res.get_json()['data'] == {'when': '2026-08-24', 'at': '2026-08-24T10:30:00'}


def test_keys_of_mixed_type_do_not_raise(flask_client):
    res = flask_client.post('/coframe/endpoint/mixed_keys', json={}, headers=bearer())

    assert res.status_code == 200
    assert res.get_json()['data'] == {'1': 'one', 'two': 2}


# ── Flask, as a guest ─────────────────────────────────────────────────────────

@pytest.fixture
def host_app(coframe_app, plugins):
    """An application that already exists, with coframe mounted inside it.

    The host's route serves a date through *its* JSON provider, which is the
    contract coframe must not touch.
    """
    from flask import Blueprint, Flask, jsonify

    app = Flask(__name__)

    @app.route('/api/v1/status')
    def status():
        return jsonify({'ok': True, 'stamp': date(2026, 8, 24)})

    @app.route('/')
    def home():
        return 'the host'

    bp = Blueprint('coframe', __name__)
    srv.register_flask(bp, coframe_app, plugins, SECRET)
    app.register_blueprint(bp)

    return app


def test_the_guest_answers_where_it_was_mounted(host_app):
    res = host_app.test_client().post('/coframe/endpoint/echo', json={'a': 1},
                                      headers=bearer())

    assert res.status_code == 200
    assert res.get_json()['data'] == {'seen': {'a': 1}}


def test_the_host_keeps_its_own_routes(host_app):
    client = host_app.test_client()

    assert client.get('/').data == b'the host'
    assert client.get('/api/v1/status').get_json()['ok'] is True


def test_the_host_json_contract_is_untouched(host_app):
    """Flask serializes dates as RFC 1123. coframe wants ISO, and gets it on
    its own responses only: a host whose clients parse RFC 1123 keeps it."""
    res = host_app.test_client().get('/api/v1/status')

    assert res.get_json()['stamp'] == 'Mon, 24 Aug 2026 00:00:00 GMT'


def test_the_refresh_hook_does_not_reach_the_host(host_app):
    """`after_request` registered on the blueprint runs for the blueprint's
    requests. A stale token on a coframe route refreshes; the host's routes
    never grow a header they did not ask for."""
    client = host_app.test_client()

    assert 'X-New-Token' in client.post('/coframe/endpoint/echo', json={},
                                        headers=bearer(last_refresh=0)).headers
    assert 'X-New-Token' not in client.get('/api/v1/status').headers


def test_the_host_decides_where_the_blueprint_hangs(coframe_app, plugins):
    """With `prefix=''` the routes are relative, and the mount point is the
    host's to choose."""
    from flask import Blueprint, Flask

    app = Flask(__name__)
    bp = Blueprint('coframe', __name__, url_prefix='/admin/cf')
    srv.register_flask(bp, coframe_app, plugins, SECRET, prefix='')
    app.register_blueprint(bp)

    client = app.test_client()

    assert client.post('/admin/cf/endpoint/echo', json={}, headers=bearer()).status_code == 200
    assert client.post('/coframe/endpoint/echo', json={}, headers=bearer()).status_code == 404


def test_registration_hands_back_the_middleware(coframe_app, plugins):
    """The same identity enters by other doors — a server-rendered page logging
    a person in — and it must not build a second middleware to do it."""
    from flask import Flask

    auth = srv.register_flask(Flask(__name__), coframe_app, plugins, SECRET)

    assert isinstance(auth, srv.AuthMiddleware)
    assert auth.secret_key == SECRET


# ── FastAPI, both ways ────────────────────────────────────────────────────────

@pytest.fixture
def fastapi_client(coframe_app, plugins):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    srv.register_fastapi(app, coframe_app, plugins, SECRET)
    return TestClient(app)


def test_fastapi_dispatches_the_same_way(fastapi_client):
    res = fastapi_client.post('/coframe/endpoint/echo', json={'a': 1},
                              headers=bearer())

    assert res.status_code == 200
    assert res.json()['data'] == {'seen': {'a': 1}}


def test_fastapi_refuses_without_a_token(fastapi_client):
    assert fastapi_client.post('/coframe/endpoint/echo', json={}).status_code == 401


def test_fastapi_refreshes_a_stale_token(fastapi_client):
    """No application middleware carries the header: the dependency writes it
    on the response, which is what lets a router be a valid target."""
    res = fastapi_client.post('/coframe/endpoint/echo', json={},
                              headers=bearer(last_refresh=0))

    assert 'X-New-Token' in res.headers


def test_fastapi_dates_go_out_as_iso(fastapi_client):
    res = fastapi_client.post('/coframe/endpoint/a_date', json={}, headers=bearer())

    assert res.json()['data'] == {'when': '2026-08-24', 'at': '2026-08-24T10:30:00'}


def test_fastapi_as_a_guest_on_a_router(coframe_app, plugins):
    from fastapi import APIRouter, FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()

    @app.get('/api/v1/status')
    def status():
        return {'ok': True}

    router = APIRouter()
    srv.register_fastapi(router, coframe_app, plugins, SECRET)
    app.include_router(router)

    client = TestClient(app)

    assert client.get('/api/v1/status').json() == {'ok': True}
    assert client.post('/coframe/endpoint/echo', json={'a': 1},
                       headers=bearer()).json()['data'] == {'seen': {'a': 1}}


def test_both_frameworks_answer_the_same(flask_client, fastapi_client):
    """The one property the pair exists to hold: what a client sees does not
    depend on which of the two is serving."""
    flask_res = flask_client.post('/coframe/endpoint/echo', json={'a': 1},
                                  headers=bearer())
    fastapi_res = fastapi_client.post('/coframe/endpoint/echo', json={'a': 1},
                                      headers=bearer())

    assert flask_res.get_json() == fastapi_res.json()
