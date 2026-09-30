"""
coframe.clientui — where the compiled client is mounted, and who logs people in.

Coframe has two natures, and an application says which one it has in the
`client:` section of its config.yaml:

    client:
      role: app | admin   # app:   coframe IS the application and owns "/"
                          # admin: coframe is the admin of a host application,
                          #        mounted under /admin/; "/" belongs to the host
      login: /login       # the host's login page; absent = the client's own login
      base: /backoffice   # only to mount somewhere else than the role says

The login is a second axis, not a consequence of the role: a host may have no
users of its own (a public showcase), and then its admin logs people in itself.

The compiled client always lives in `<app>/clientui/`: the directory says what it
is, the configuration says where it is mounted. `static/` stays the host's, with
the route its framework gives it.

Pure: no web framework is imported here, so `check`, `coframe dev` and the
servers read the same rule.
"""
from typing import Any, Dict, NamedTuple, Optional

ROLES = ('app', 'admin')
DEFAULT_ROLE = 'app'
DEFAULT_BASE = {'app': '', 'admin': '/admin'}

# The directory of an application that holds its compiled client.
CLIENT_DIR = 'clientui'


class ClientSettings(NamedTuple):
    role: str
    base: str             # '' for the root, otherwise '/path' without a trailing slash
    login: Optional[str]  # the host's login page, or None when the client logs in


def client_settings(config: Dict[str, Any]) -> ClientSettings:
    """
    The `client:` section of an application's config.yaml, checked.

    Raises:
        ValueError: on a role outside ROLES, or a path that does not start
            with '/' — the client would be mounted where nobody looks
    """
    section = config.get('client') or {}
    role = section.get('role', DEFAULT_ROLE)
    if role not in ROLES:
        raise ValueError(f"client.role '{role}' is not one of {', '.join(ROLES)}")

    base = section.get('base', DEFAULT_BASE[role])
    base = '' if base in (None, '', '/') else str(base)
    if base and not base.startswith('/'):
        raise ValueError(f"client.base '{base}' must start with '/'")
    base = base.rstrip('/')

    login = section.get('login') or None
    if login is not None and not str(login).startswith('/'):
        raise ValueError(f"client.login '{login}' must be a path starting with '/'")

    return ClientSettings(role, base, login)
