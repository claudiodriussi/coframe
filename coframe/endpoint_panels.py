from typing import Any, Dict
import coframe.utils
from coframe.endpoints import endpoint
# Note: context is set globally via BaseApp.set_context() before each call —
# endpoint functions receive only (data).


def _strip_meta(obj: Any) -> Any:
    """Remove $plugin metadata keys (internal, not needed by client)."""
    if isinstance(obj, dict):
        return {k: _strip_meta(v) for k, v in obj.items() if k != '$plugin'}
    if isinstance(obj, list):
        return [_strip_meta(item) for item in obj]
    return obj


@endpoint('get_page')
def get_page(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a resolved page descriptor by id.

    Resolves all $ref fields and strips internal $plugin metadata
    before sending to the client.

    Parameters:
        id: Page id (e.g. "book_list")

    Returns:
        { status, data: <resolved page descriptor>, code }
    """
    panel_id = data.get('id')
    if not panel_id:
        return {'status': 'error', 'message': 'id is required', 'code': 400}

    app = coframe.utils.get_app()
    panel = app.pm.get(f'pages.{panel_id}')

    if panel is None:
        return {'status': 'error', 'message': f"Panel not found: '{panel_id}'", 'code': 404}

    resolved = app.pm.resolve_refs(panel)
    return {'status': 'success', 'data': _strip_meta(resolved), 'code': 200}
