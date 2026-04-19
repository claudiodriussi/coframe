from typing import Any, Dict
import coframe.utils
from coframe.endpoints import endpoint
# Note: context is set globally via BaseApp.set_context() before each call —
# endpoint functions receive only (data).


_JSON_SCALARS = (str, int, float, bool, type(None))

def _strip_meta(obj: Any) -> Any:
    """Remove $plugin metadata keys and non-JSON-serializable objects."""
    if isinstance(obj, dict):
        return {k: _strip_meta(v) for k, v in obj.items() if k != '$plugin'}
    if isinstance(obj, list):
        return [_strip_meta(item) for item in obj]
    if isinstance(obj, _JSON_SCALARS):
        return obj
    # Drop anything else (DbTable, DbColumn, etc.) that can't be JSON-serialized
    return None


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


def _auto_form_page(table_name: str, table: Any) -> Dict[str, Any]:
    """
    Auto-generate a form page descriptor for a table.

    Convention: '{table_name}_form' (e.g. 'book_form').
    PK columns are included as read-only. Secret and virtual columns are skipped.
    All column attributes are forwarded to the client (which applies what it knows).
    The only exception is foreign_key['table'] which is a non-serializable DbTable object.
    """
    fields = []
    for col in table.effective_columns:
        attrs = col.attributes

        if attrs.get('secret'):
            continue
        if attrs.get('virtual'):
            continue

        entry: Dict[str, Any] = {'name': col.name}

        # Pass all attributes through; strip non-serializable objects
        for k, v in attrs.items():
            if k == 'foreign_key':
                entry[k] = {fk_k: fk_v for fk_k, fk_v in v.items() if fk_k != 'table'}
            elif isinstance(v, _JSON_SCALARS) or isinstance(v, (list, dict)):
                entry[k] = v

        # Derived client hints not present in raw attributes
        if attrs.get('primary_key'):
            entry['readonly'] = True
        if attrs.get('nullable') is False and attrs.get('default') is None:
            entry['required'] = True

        fields.append(entry)

    return {
        'title': table_name,
        '_auto': True,
        'content': {
            'type': 'form',
            'source': {'model': table_name},
            'fields': fields,
            'policy': {'editable': True},
            'actions': {'toolbar': ['save', 'cancel']},
        },
    }


def _resolve_auto_page(app: Any, panel_id: str) -> Dict[str, Any] | None:
    """
    Try to auto-generate a page from a conventional id.

    Supported patterns:
      {table_name}_list  →  auto list view for that table
      {table_name}_form  →  auto form descriptor for that table

    Table name matching is case-insensitive (e.g. 'author_list' → 'Author').
    Returns None if no matching table is found.
    """
    for suffix, builder in (('_list', _auto_list_page), ('_form', _auto_form_page)):
        if panel_id.endswith(suffix):
            base = panel_id[:-len(suffix)]
            for t_name, t_obj in app.tables.items():
                if t_name.lower() == base.lower():
                    return builder(t_name, t_obj)
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
           {table}_form  →  auto form descriptor for that table

    Parameters:
        id: Page id (e.g. "book_list", "book_form", "Author_list")

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
