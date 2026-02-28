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


@endpoint('get_panel')
def get_panel(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a resolved panel descriptor by id.

    Resolves all $ref fields and strips internal $plugin metadata
    before sending to the client.

    Parameters:
        id: Panel id — short form ("book_list") searches all namespaces;
            qualified form ("panels.book_list") accesses the namespace directly.

    Returns:
        { status, data: <resolved panel descriptor>, code }
    """
    panel_id = data.get('id')
    if not panel_id:
        return {'status': 'error', 'message': 'id is required', 'code': 400}

    app = coframe.utils.get_app()
    panel = None
    parts = panel_id.split('.')

    if len(parts) == 2:
        # Qualified "plugin.id" → direct access
        panel = app.pm.get(f'panels.{panel_id}')
    else:
        # Short id → search across all namespaces in load order
        panels_root = app.pm.get('panels') or {}
        for ns in panels_root:
            if ns.startswith('$'):
                continue  # skip $plugin and other metadata keys
            candidate = app.pm.get(f'panels.{ns}.{panel_id}')
            if candidate is not None:
                panel = candidate
                break

    if panel is None:
        return {'status': 'error', 'message': f"Panel not found: '{panel_id}'", 'code': 404}

    resolved = app.pm.resolve_refs(panel)
    return {'status': 'success', 'data': _strip_meta(resolved), 'code': 200}
