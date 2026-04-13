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


def _auto_list_page(table_name: str, table: Any) -> Dict[str, Any]:
    """
    Auto-generate a minimal list page descriptor for a table.

    Convention: requested as '{table_name}_list' (e.g. 'author_list').
    Includes all non-secret effective_columns as table columns.
    FK columns are shown as raw id fields (no join auto-resolve).

    M2M tables (composite PK) get a read-only toolbar — no add/delete
    since those require both FK sides and a dedicated form.
    """
    columns = []
    for col in table.effective_columns:
        if col.attributes.get('secret'):
            continue
        entry: Dict[str, Any] = {'field': col.name}
        label = col.attributes.get('label')
        if label:
            entry['title'] = label
        columns.append(entry)

    # M2M tables have a composite PK — restrict to read-only actions
    m2m = table.attributes.get('many_to_many')
    toolbar = ['search', 'filter', 'export'] if m2m else ['add', 'search', 'filter', 'export']

    return {
        'title': table_name,
        '_auto': True,
        'content': {
            'type': 'table',
            'source': {'model': table_name},
            'columns': columns,
            'actions': {
                'toolbar': toolbar,
            },
        },
    }


def _resolve_auto_page(app: Any, panel_id: str) -> Dict[str, Any] | None:
    """
    Try to auto-generate a page from a conventional id.

    Supported patterns:
      {table_name}_list  →  auto list view for that table

    Table name matching is case-insensitive (e.g. 'author_list' → 'Author').
    Returns None if no matching table is found.
    """
    if panel_id.endswith('_list'):
        base = panel_id[:-5]  # strip '_list'
        for t_name, t_obj in app.tables.items():
            if t_name.lower() == base.lower():
                return _auto_list_page(t_name, t_obj)
    return None


@endpoint('get_page')
def get_page(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a resolved page descriptor by id.

    Resolves all $ref fields and strips internal $plugin metadata
    before sending to the client.

    Resolution order:
      1. Explicit pages dict (YAML plugin pages)
      2. Auto-generated fallback:
           {table}_list  →  minimal table view for that DB table

    Parameters:
        id: Page id (e.g. "book_list", "Author_list")

    Returns:
        { status, data: <resolved page descriptor>, code }
    """
    panel_id = data.get('id')
    if not panel_id:
        return {'status': 'error', 'message': 'id is required', 'code': 400}

    app = coframe.utils.get_app()
    panel = app.pm.get(f'pages.{panel_id}')

    if panel is not None:
        resolved = app.pm.resolve_refs(panel)
        return {'status': 'success', 'data': _strip_meta(resolved), 'code': 200}

    # Fallback: auto-generation from table schema
    auto = _resolve_auto_page(app, panel_id)
    if auto is not None:
        return {'status': 'success', 'data': auto, 'code': 200}

    return {'status': 'error', 'message': f"Panel not found: '{panel_id}'", 'code': 404}
