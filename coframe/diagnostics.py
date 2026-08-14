"""
coframe.diagnostics — post-load validation and effective-state dump.

Two entry points (run_checks is pure; dump_app reads the plugin source files
to embed them; neither uses argparse):

  run_checks(app)  -> List[issue]
      Validate the merged plugin data after everything is loaded
      (calc_db done): unresolved $ref, views pointing at unknown
      tables/columns, stack_push targets that don't exist, orphan
      views. Merge-time issues collected by PluginsManager (value
      overlaps, positional anchors not found) are included first.

  dump_app(app)    -> Dict
      Complete JSON-serializable snapshot of the effective application
      state: plugins, issues, tables, types, every descriptor section
      (pages, views, menus, menu_items, … with $ref resolved), endpoints,
      merge history, and source_files (raw pre-merge YAML per plugin/file).
      $plugin attribution keys are kept so an external viewer (web app,
      tkinter, jq, ...) can show which plugin contributed each piece.
      DB/type sections (tables/types/schemas) are special-cased; all other
      sections are treated uniformly.

Issue format (shared with PluginsManager.add_issue):
  {severity: 'error'|'warning'|'info', code, path, message, plugin}
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

# Only plain identifiers ('title') and dotted fields ('Publisher.name') are
# checkable against the schema; anything else (SQL expressions, aliases,
# functions) is passed through to the querybuilder untouched.
_CHECKABLE_FIELD = re.compile(r'^[A-Za-z_]\w*(\.[A-Za-z_]\w*)?$')

# Sections that feed model/type generation, not UI descriptors. They have their
# own dedicated dumpers/checks (tables via _table_dict, types via the schema
# registry) and must NOT be walked as generic descriptor trees. Every OTHER
# top-level section (pages, views, menus, menu_items, future chrome/quickbar/...)
# is a descriptor and is validated/dumped uniformly — see docs/pending/menu.md §10.
_DB_SECTIONS = frozenset({'tables', 'types', 'schemas'})


def _descriptor_sections(pm: Any) -> List[str]:
    """Top-level descriptor sections: everything mergeable that isn't DB/type."""
    return [k for k, v in pm.data.items()
            if not k.startswith('$') and k not in _DB_SECTIONS and isinstance(v, dict)]


def make_issue(severity: str, code: str, path: str, message: str,
               plugin: Optional[str] = None) -> Dict[str, Any]:
    """Build an issue dict in the shared diagnostics format."""
    return {'severity': severity, 'code': code, 'path': path,
            'message': message, 'plugin': plugin}


# ── Checks ─────────────────────────────────────────────────────────────────────

def run_checks(app: Any) -> List[Dict[str, Any]]:
    """
    Validate the merged descriptor data of a fully loaded app.

    Args:
        app: Initialized coframe app (setup_schema() is sufficient —
             needs app.pm and app.tables, no DB engine)

    Returns:
        List of issues, merge-time ones first, then descriptor checks.
    """
    pm = app.pm
    issues: List[Dict[str, Any]] = list(getattr(pm, 'issues', []))
    referenced: Set[str] = set()

    # Generic descriptor validation: walk every descriptor section uniformly
    # (pages, views, menus, menu_items, and any future section). The per-node
    # checks ($ref, view source, stack_push target) fire wherever they appear.
    for section in _descriptor_sections(pm):
        data = pm.data.get(section) or {}
        for item_id, item in data.items():
            if item_id.startswith('$'):
                continue
            _walk(app, item, f'{section}.{item_id}', None, issues, referenced)

    # Orphan views: defined but never targeted by a $ref
    views = pm.data.get('views') or {}
    for view_id, view in views.items():
        if view_id.startswith('$'):
            continue
        if f'views.{view_id}' not in referenced:
            plugin = view.get('$plugin') if isinstance(view, dict) else None
            issues.append(make_issue(
                'info', 'view-orphan', f'views.{view_id}',
                'view is not referenced by any page', plugin))

    return issues


def _walk(app: Any, obj: Any, path: str, plugin: Optional[str],
          issues: List[Dict[str, Any]], referenced: Set[str]) -> None:
    """Recursively check a descriptor subtree, tracking $plugin attribution."""
    if isinstance(obj, dict):
        plugin = obj.get('$plugin', plugin)

        ref = obj.get('$ref')
        if isinstance(ref, str):
            referenced.add(ref)
            if app.pm.get(ref) is None:
                issues.append(make_issue(
                    'error', 'ref-unresolved', path,
                    f"$ref '{ref}' does not resolve", plugin))

        if isinstance(obj.get('source'), dict):
            _check_view(app, obj, path, plugin, issues)

        if obj.get('action') == 'stack_push':
            _check_push_target(app, obj, path, plugin, issues)

        for key, value in obj.items():
            if key.startswith('$'):
                continue
            _walk(app, value, f'{path}.{key}', plugin, issues, referenced)

    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            label: Any = i
            if isinstance(item, dict):
                label = item.get('id') or item.get('name') or item.get('field') or i
            _walk(app, item, f'{path}[{label}]', plugin, issues, referenced)


def _check_view(app: Any, view: Dict[str, Any], path: str,
                plugin: Optional[str], issues: List[Dict[str, Any]]) -> None:
    """Check a view-like dict (has source.model): model + columns/fields."""
    model = view['source'].get('model')
    if not isinstance(model, str):
        return

    table = app.tables.get(model)
    if table is None:
        issues.append(make_issue(
            'error', 'model-missing', f'{path}.source.model',
            f"table '{model}' is not defined", plugin))
        return

    colnames = {c.name for c in table.effective_columns}

    for col in view.get('columns') or []:
        if isinstance(col, dict) and isinstance(col.get('field'), str):
            field = col['field']
            _check_field(app, field, colnames, model,
                         f'{path}.columns[{field}]', plugin, issues)

    _check_fields(view.get('fields') or [], colnames, model,
                  f'{path}.fields', plugin, issues)


def _check_field(app: Any, field: str, colnames: Set[str], model: str,
                 path: str, plugin: Optional[str],
                 issues: List[Dict[str, Any]]) -> None:
    """Check a table column reference; dotted fields resolve on the joined table."""
    if field.startswith('$') or not _CHECKABLE_FIELD.match(field):
        return

    if '.' in field:
        t_name, col_name = field.split('.', 1)
        target = app.tables.get(t_name)
        if target is None:
            issues.append(make_issue(
                'warning', 'field-unknown', path,
                f"joined field '{field}': table '{t_name}' is not defined", plugin))
        elif col_name not in {c.name for c in target.effective_columns}:
            issues.append(make_issue(
                'warning', 'field-unknown', path,
                f"joined field '{field}': column '{col_name}' not in '{t_name}'", plugin))
        return

    if field not in colnames:
        issues.append(make_issue(
            'warning', 'field-unknown', path,
            f"field '{field}' not found in table '{model}'", plugin))


def _check_fields(fields: List[Any], colnames: Set[str], model: str,
                  path: str, plugin: Optional[str],
                  issues: List[Dict[str, Any]]) -> None:
    """Check form fields, recursing into groups ({group, fields})."""
    for f in fields:
        if not isinstance(f, dict):
            continue
        if isinstance(f.get('fields'), list):
            _check_fields(f['fields'], colnames, model,
                          f"{path}[{f.get('group', '?')}]", plugin, issues)
            continue
        name = f.get('name')
        if (isinstance(name, str) and not name.startswith('$')
                and '.' not in name and name not in colnames):
            issues.append(make_issue(
                'warning', 'field-unknown', f'{path}[{name}]',
                f"field '{name}' not found in table '{model}'", plugin))


def _check_push_target(app: Any, action: Dict[str, Any], path: str,
                       plugin: Optional[str],
                       issues: List[Dict[str, Any]]) -> None:
    """A stack_push target must be an explicit page or auto-generatable."""
    target = action.get('panel')
    if not isinstance(target, str):
        return
    pages = app.pm.data.get('pages') or {}
    if target in pages:
        return

    from coframe.pages import resolve_auto_page
    if resolve_auto_page(app, target) is None:
        issues.append(make_issue(
            'error', 'panel-missing', f'{path}.panel',
            f"stack_push target '{target}' is not a page and cannot be auto-generated",
            plugin))


# ── Full dump ──────────────────────────────────────────────────────────────────

def dump_app(app: Any) -> Dict[str, Any]:
    """
    Build the complete effective-state snapshot of a loaded app.

    Everything is JSON-serializable (write with json.dumps(..., default=str)
    to be safe with dates or other stray objects).
    """
    from coframe.cli import _table_dict
    from coframe.endpoints import _ENDPOINTS

    pm = app.pm

    # Every descriptor section, dumped uniformly with $ref resolved and $plugin
    # attribution kept (pages, views, menus, menu_items, future sections). DB/type
    # sections are excluded — they have dedicated dumpers below.
    sections: Dict[str, Dict[str, Any]] = {}
    for name in _descriptor_sections(pm):
        out: Dict[str, Any] = {}
        for item_id, item in pm.data[name].items():
            if item_id.startswith('$'):
                continue
            try:
                out[item_id] = pm.resolve_refs(item)
            except ValueError as e:  # circular $ref
                out[item_id] = {'$error': str(e)}
        sections[name] = out

    # Keys contributed by more than one plugin — the interesting slice of history
    contested = {k: sorted(set(v)) for k, v in pm.history.items()
                 if len(set(v)) > 1}

    # Raw pre-merge YAML per plugin/file. Lossless where the merged snapshot is
    # not: comments, ordering, $after/$remove directives before they are consumed,
    # types before mixin flattening. Provenance is free — each file is attributed
    # by its plugin + path. The inspector searches/diffs these instead of walking
    # the filesystem, and never has to re-run the merge (it stays a viewer).
    # `content` = parsed (in-memory, structured search); `text` = raw (comments,
    # textual search), best-effort.
    source_files: Dict[str, Any] = {}
    for name in pm.sorted:
        p = pm.plugins[name]
        files: Dict[str, Any] = {}
        entries = [(p.plugin_dir / 'config.yaml', p.config)]
        entries += list(zip(p.data_files, p.data))
        for path, parsed in entries:
            try:
                text = path.read_text(encoding='utf-8')
            except OSError:
                text = None
            files[path.name] = {'content': parsed, 'text': text}
        source_files[name] = {'dir': str(p.plugin_dir), 'files': files}

    return {
        # Contract version for external consumers (inspector, tooling).
        # Bump on any structural change to the sections below.
        # v2: descriptor sections are generic (pages/views + menus/menu_items/…)
        #     and + source_files (raw pre-merge YAML per plugin/file).
        'dump_version': 2,
        'generated': datetime.now().isoformat(timespec='seconds'),
        'app': {k: pm.config.get(k) for k in ('name', 'version', 'description')},
        'plugins': [
            {
                'name': name,
                'version': pm.plugins[name].config.get('version'),
                'depends_on': pm.plugins[name].config.get('depends_on'),
                'dir': str(pm.plugins[name].plugin_dir),
                'data_files': [f.name for f in pm.plugins[name].data_files],
                'sources': [f.name for f in pm.plugins[name].sources],
            }
            for name in pm.sorted
        ],
        'issues': run_checks(app),
        'tables': {name: _table_dict(t) for name, t in app.tables.items()},
        'types': app.get_type_schema(include_builtin=False),
        **sections,
        'endpoints': {
            name: {
                'module': getattr(fn, '__module__', ''),
                'doc': (getattr(fn, '__doc__', None) or '').strip().split('\n')[0],
            }
            for name, fn in sorted(_ENDPOINTS.items())
        },
        'contested_keys': contested,
        'merge_history': dict(sorted(pm.history.items())),
        'source_files': source_files,
    }
