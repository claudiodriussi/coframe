"""
coframe.cli — introspection and management utilities.

Two layers:

  dump_*()          Pure functions: receive an app, return (yaml_text, label).
                    No file I/O, no argparse — fully testable in isolation.

  make_parser()     Build and return the ArgumentParser for the coframe CLI.
                    Add new subparsers here as new commands are implemented.

  run_cli()         Dispatch parsed args to the right dump_* function and
                    write output to file or stdout.  Accepts an output_dir
                    so callers can point it at their project's data folder.

Commands needing a live database (db-check, db-sync) are listed in DB_COMMANDS:
the caller builds the app with an engine for those, and with the schema alone
for the rest.

Planned sections:
  dump_*     — read-only introspection (pages, tables, types, plugins, endpoints)
  db_*       — schema alignment (coframe.schema_sync)
  [future]   — dump-endpoints: catalog of registered endpoints with metadata
  [future]   — backup / restore
  [future]   — db engine migration

Future standalone entry-point (pyproject.toml scripts):
  coframe --config config.yaml dump-page author_list
"""

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import yaml


# ── Helpers ────────────────────────────────────────────────────────────────────

def _to_yaml(data: Any) -> str:
    """Serialize a dict to a human-readable YAML string."""
    return yaml.dump(
        data,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        indent=2,
    )


# ── dump_page ──────────────────────────────────────────────────────────────────

def dump_page(app: Any, page_id: str, auto: bool = False, raw: bool = False) -> Tuple[str, str]:
    """
    Generate a YAML snippet for a page descriptor.

    Resolution order (unless --auto):
      1. Explicit pages dict (YAML plugin pages)
      2. Auto-generated fallback from table schema

    Args:
        app:     Initialized coframe app (setup_schema() is sufficient — no DB engine needed)
        page_id: Page identifier (e.g. 'author_list', 'book_with_reviews')
        auto:    Force auto-generation from table schema, ignoring any explicit YAML
        raw:     Skip $ref resolution — output the raw descriptor as declared in YAML

    Returns:
        (yaml_text, source_label)
        yaml_text    — YAML string wrapped in {page_id: descriptor}, ready to paste
        source_label — human-readable description of the resolution path taken

    Raises:
        ValueError: if the page cannot be found or auto-generated
    """
    from coframe.endpoint_panels import _resolve_auto_page, _strip_meta

    descriptor = None
    source_label = ''

    if auto:
        descriptor = _resolve_auto_page(app, page_id)
        if descriptor is None:
            raise ValueError(f"Cannot auto-generate '{page_id}' — no matching table found")
        source_label = 'auto-generated'
    else:
        raw_panel = app.pm.get(f'pages.{page_id}')
        if raw_panel is not None:
            if raw:
                descriptor = _strip_meta(raw_panel)
                source_label = 'explicit, $ref not expanded'
            else:
                descriptor = _strip_meta(app.pm.resolve_refs(raw_panel))
                source_label = 'explicit, $ref expanded'
        else:
            descriptor = _resolve_auto_page(app, page_id)
            if descriptor is None:
                raise ValueError(f"Page '{page_id}' not found and cannot be auto-generated")
            source_label = 'auto-generated — no explicit YAML found'

    # Remove internal marker before serializing
    descriptor.pop('_auto', None)

    yaml_text = _to_yaml({page_id: descriptor})
    return yaml_text, source_label


# ── dump_table ─────────────────────────────────────────────────────────────────

# Column attributes exposed in the dump (superset of what get_table_schema sends
# to the client — here we want the full developer view).
_COL_ATTRS = (
    'type', 'label', 'help',
    'primary_key', 'autoincrement',
    'nullable', 'unique', 'index', 'default',
    'virtual', 'secret', 'editable',
    'widget',
)


def _column_dict(col: Any) -> Dict[str, Any]:
    """Serialize a DbColumn to a dict for developer inspection."""
    d: Dict[str, Any] = {'name': col.name}

    for attr in _COL_ATTRS:
        if attr in col.attributes:
            d[attr] = col.attributes[attr]
        elif attr == 'type' and col.db_type:
            d['type'] = col.db_type.name

    # Foreign key — show as "Table.field" string instead of resolved objects
    fk = col.attributes.get('foreign_key')
    if fk and isinstance(fk, dict) and 'table' in fk:
        d['foreign_key'] = f"{fk['table'].name}.{fk['id']}"

    return d


def _table_dict(table: Any) -> Dict[str, Any]:
    """Serialize a DbTable to a dict for developer inspection."""
    # PK fields (same logic as get_table_schema)
    m2m = table.attributes.get('many_to_many')
    if m2m:
        pk_fields = [m2m['target1']['column'], m2m['target2']['column']]
    else:
        pk_fields = [
            col.name for col in table.effective_columns
            if col.attributes.get('primary_key')
        ]

    d: Dict[str, Any] = {'pk_fields': pk_fields}

    # Table-level metadata
    for attr in ('label', 'help', 'tags', 'mixins'):
        val = table.attributes.get(attr)
        if val:
            d[attr] = val

    # M2M targets summary
    if m2m:
        d['many_to_many'] = {
            'target1': f"{m2m['target1']['table'].name}.{m2m['target1']['id']} → {m2m['target1']['column']}",
            'target2': f"{m2m['target2']['table'].name}.{m2m['target2']['id']} → {m2m['target2']['column']}",
        }

    d['columns'] = [_column_dict(col) for col in table.effective_columns]

    # Plugins that contributed to this table
    d['defined_in'] = [p.name for p in table.plugins]

    return d


def dump_table(app: Any, table_names: Optional[List[str]] = None) -> Tuple[str, str]:
    """
    Generate a YAML snapshot of one or more tables after all plugin merges.

    Shows the full effective schema: real columns + mixin columns + virtual
    columns, with all resolved attributes (nullable, unique, FK target, …).

    Args:
        app:         Initialized coframe app (setup_schema() is sufficient)
        table_names: List of table names to dump; None = all tables

    Returns:
        (yaml_text, label)

    Raises:
        ValueError: if a requested table name is not found
    """
    if table_names:
        unknown = [n for n in table_names if n not in app.tables]
        if unknown:
            raise ValueError(f"Unknown table(s): {', '.join(unknown)}")
        tables = {n: app.tables[n] for n in table_names}
        label = ', '.join(table_names)
    else:
        tables = dict(app.tables)
        label = 'all tables'

    output = {name: _table_dict(table) for name, table in tables.items()}
    return _to_yaml(output), label


# ── dump_types ─────────────────────────────────────────────────────────────────

# Attributes skipped in the tree nodes (structural, not useful as reference)
_TYPE_SKIP = {'base', 'columns', 'autoincrement'}


def _type_delta(type_attrs: Dict, parent_attrs: Optional[Dict]) -> Dict:
    """
    Return only attributes that this type adds or overrides vs its parent.
    Skips structural keys and anything identical to the parent.
    """
    result = {}
    for key, val in type_attrs.items():
        if key in _TYPE_SKIP:
            continue
        if parent_attrs is None or key not in parent_attrs or parent_attrs[key] != val:
            result[key] = val
    return result


def _build_scalar_tree(
    types: Dict[str, Any],
    parent_name: Optional[str],
    include_builtin: bool,
) -> Dict:
    """
    Recursively build the scalar type tree rooted at parent_name.
    Each node contains its delta attributes + child type names as nested keys.
    """
    node: Dict = {}
    parent_obj = types.get(parent_name) if parent_name else None
    parent_attrs = parent_obj.attributes if parent_obj else None

    # Find direct non-builtin children of parent_name
    children = [
        (name, t) for name, t in types.items()
        if not t.columns                              # scalar only
        and not (t.plugin == "")                     # non-builtin only
        and (t.attributes.get('base') == parent_name  # direct child
             or (parent_name is None and 'base' not in t.attributes))
    ]

    for name, t in sorted(children, key=lambda x: x[0]):
        delta = _type_delta(t.attributes, parent_attrs)
        subtree = _build_scalar_tree(types, name, include_builtin)
        entry: Dict = {**delta, **subtree}
        node[name] = entry

    return node


def dump_types(app: Any, include_builtin: bool = False) -> Tuple[str, str]:
    """
    Generate a YAML snapshot of the type registry as two sections:

      types:           — scalar types, shown as an inheritance tree.
                         Each node contains only the attributes it ADDS or
                         OVERRIDES vs its parent; child type names appear as
                         nested keys (PascalCase vs snake_case, no collision).

      compound_types:  — types with sub-columns (Address, TimeStamp, Archivable, …).
                         Shown flat with their full column list.

    Args:
        app:             Initialized coframe app
        include_builtin: If True, also include builtin SQLAlchemy types as
                         explicit root nodes even when they have no custom children.

    Returns:
        (yaml_text, label)
    """
    all_types = app.types  # Dict[str, DbType]

    # ── Compound types (have sub-columns) ─────────────────────────────────────
    compound: Dict = {}
    for name, t in sorted(all_types.items()):
        if not t.columns:
            continue
        if t.plugin == "" and not include_builtin:
            continue
        entry: Dict = {}
        label = t.attributes.get('label')
        if label:
            entry['label'] = label
        help_text = t.attributes.get('help')
        if help_text:
            entry['help'] = help_text.strip()
        _col_skip = {'$plugin', 'plugin', 'base'}
        entry['columns'] = [
            {k: v for k, v in col.attributes.items()
             if k not in _col_skip and v is not None}
            for col in t.columns
        ]
        compound[name] = entry

    # ── Scalar type tree ───────────────────────────────────────────────────────
    # Find builtin types that have at least one non-builtin child (tree roots).
    # Non-builtin types with no base at all also become roots (top-level).
    scalar_types = {n: t for n, t in all_types.items() if not t.columns}

    # Collect all parent names that non-builtin scalars point to
    builtin_parents_used: set = set()
    for t in scalar_types.values():
        if t.plugin == "":
            continue
        parent = t.attributes.get('base')
        if parent and scalar_types.get(parent) and scalar_types[parent].plugin == "":
            builtin_parents_used.add(parent)

    # Build tree: top-level keys are builtin parents (if they have children)
    # plus non-builtin orphan roots (no base).
    tree: Dict = {}

    for builtin_parent in sorted(builtin_parents_used):
        subtree = _build_scalar_tree(scalar_types, builtin_parent, include_builtin)
        if subtree:
            tree[builtin_parent] = subtree

    # Non-builtin types with no parent
    orphan_subtree = _build_scalar_tree(scalar_types, None, include_builtin)
    tree.update(orphan_subtree)

    output: Dict = {}
    if tree:
        output['types'] = tree
    if compound:
        output['compound_types'] = compound

    return _to_yaml(output), 'type registry'


# ── db-check / db-sync ─────────────────────────────────────────────────────────

# Commands that need app.initialize_db() to have run — the others work on the
# merged schema alone.
DB_COMMANDS = {'db-check', 'db-sync'}


def db_check(app: Any) -> Tuple[str, bool]:
    """
    Compare the database with the schema the plugins describe.

    Returns:
        (report, aligned) — read-only, nothing is written.
    """
    from coframe.db import Base
    from coframe.schema_sync import diff_schema, format_diff

    diff = diff_schema(app.engine, Base.metadata)
    return format_diff(diff), diff.is_aligned


def db_sync(app: Any, dry_run: bool = False) -> Tuple[str, bool]:
    """
    Apply the changes the database can take safely: new tables and columns,
    new indexes, widened types, relaxed NOT NULLs.  Everything else is
    reported and left alone — see coframe.schema_sync for why.

    Returns:
        (report, aligned_after) — with dry_run, the DDL that would run.
    """
    from coframe.db import Base
    from coframe.schema_sync import apply_diff, diff_schema, format_diff, plan_sql

    diff = diff_schema(app.engine, Base.metadata)

    if diff.is_aligned:
        return 'Database aligned with the schema.', True

    lines = [format_diff(diff, sync_command=None)]

    if dry_run:
        sql = plan_sql(app.engine, diff)
        if sql:
            lines += ['', '--- SQL that would run ---', sql.rstrip()]
        return '\n'.join(lines), False

    if diff.safe:
        executed = apply_diff(app.engine, diff, logger=app.pm.logger)
        lines += ['', f'Applied {len(diff.safe)} change(s):']
        lines += [f'  {statement}' for statement in executed]

    return '\n'.join(lines), not diff.refused


# ── CLI parser ─────────────────────────────────────────────────────────────────

def make_parser() -> argparse.ArgumentParser:
    """
    Build and return the coframe CLI argument parser.

    Add new subparsers here as new commands are implemented.
    The parser is intentionally separate from run_cli() so callers
    can inspect or extend it before parsing.
    """
    parser = argparse.ArgumentParser(
        prog='coframe',
        description='Coframe CLI — introspection and management',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  dump-page author_list                 auto-gen list (no explicit YAML)
  dump-page book_with_reviews           explicit page, $ref expanded
  dump-page book_with_reviews --raw     explicit page, $ref kept
  dump-page book_list --auto            force auto-gen (ignore explicit YAML)
  dump-table Author                     Author schema after plugin merge
  dump-table                            all tables
  dump-types                            type inheritance tree + compound types
  dump-types --include-builtin          also show SQLAlchemy built-in roots
  check                                 validate merged descriptors (refs, models, fields)
  check --dump                          also write full JSON dump to <output_dir>/appdump.json
  db-check                              compare the database with the schema (exit 1 if it differs)
  db-sync --dry-run                     show the DDL an alignment would run
  db-sync                               apply it (adds only — never drops, never narrows)
        """,
    )

    sub = parser.add_subparsers(dest='command', metavar='command')

    # ── dump-page ──────────────────────────────────────────────────────────────
    p = sub.add_parser(
        'dump-page',
        help='Generate YAML for a page descriptor (auto-gen or explicit)',
    )
    p.add_argument('page_id', help='Page id (e.g. author_list, book_with_reviews)')
    p.add_argument('--auto', action='store_true',
                   help='Force auto-generation from table schema (ignore explicit YAML)')
    p.add_argument('--raw', action='store_true',
                   help='No $ref expansion — output raw descriptor as declared in YAML')
    p.add_argument('-o', '--output', metavar='PATH',
                   help='Output file path (- = stdout; default: <output_dir>/pages/<id>.yaml)')

    # ── dump-table ─────────────────────────────────────────────────────────────
    p = sub.add_parser(
        'dump-table',
        help='Dump table schema after plugin merge (effective columns, PK, mixins, …)',
    )
    p.add_argument('table', nargs='*', metavar='TABLE',
                   help='Table name(s) to dump (omit for all tables)')
    p.add_argument('-o', '--output', metavar='PATH',
                   help='Output file path (- = stdout; default: <output_dir>/tables/<name>.yaml)')

    # ── dump-types ─────────────────────────────────────────────────────────────
    p = sub.add_parser(
        'dump-types',
        help='Dump type registry as inheritance tree + compound types',
    )
    p.add_argument('--include-builtin', action='store_true',
                   help='Include SQLAlchemy built-in types as explicit root nodes')
    p.add_argument('-o', '--output', metavar='PATH',
                   help='Output file path (- = stdout; default: <output_dir>/types.yaml)')

    # ── check ──────────────────────────────────────────────────────────────────
    p = sub.add_parser(
        'check',
        help='Validate merged descriptors; report unresolved refs, unknown models/fields, orphans',
    )
    p.add_argument('--dump', nargs='?', const='', metavar='PATH',
                   help='Also write the full effective-state JSON dump '
                        '(default path: <output_dir>/appdump.json)')

    # ── db-check ───────────────────────────────────────────────────────────────
    p = sub.add_parser(
        'db-check',
        help='Compare the database with the schema described by the plugins (read-only)',
    )

    # ── db-sync ────────────────────────────────────────────────────────────────
    p = sub.add_parser(
        'db-sync',
        help='Align the database: add tables/columns/indexes, widen types. Never drops.',
    )
    p.add_argument('--dry-run', action='store_true',
                   help='Print the DDL that would run, without touching the database')

    return parser


# ── CLI dispatcher ─────────────────────────────────────────────────────────────

def print_issues(issues: List[Dict[str, Any]]) -> int:
    """
    Print issues grouped by severity (errors first). Returns the error count.
    """
    by_sev: Dict[str, List[Dict[str, Any]]] = {'error': [], 'warning': [], 'info': []}
    for issue in issues:
        by_sev.setdefault(issue['severity'], []).append(issue)

    for sev in ('error', 'warning', 'info'):
        group = by_sev[sev]
        if not group:
            continue
        print(f'\n{sev.upper()} ({len(group)}):')
        for i in sorted(group, key=lambda x: (x['code'], x['path'])):
            plugin = f"  [{i['plugin']}]" if i.get('plugin') else ''
            print(f"  {i['code']:<20} {i['path']} — {i['message']}{plugin}")

    total = sum(len(g) for g in by_sev.values())
    if total == 0:
        print('No issues found.')
    else:
        print(f"\n{len(by_sev['error'])} errors, {len(by_sev['warning'])} warnings, "
              f"{len(by_sev['info'])} info.")
    return len(by_sev['error'])


def _write(yaml_text: str, label: str, out_arg: str, default_path: Path) -> None:
    """Write yaml_text to stdout or a file, printing a status line."""
    if out_arg == '-':
        print(f'# [{label}]\n')
        print(yaml_text)
    else:
        out_path = Path(out_arg) if out_arg else default_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(f'# [{label}]\n{yaml_text}', encoding='utf-8')
        print(f'Written [{label}]: {out_path}')


def run_cli(app: Any, args: argparse.Namespace, output_dir: Path = Path('.')) -> None:
    """
    Dispatch parsed CLI args to the appropriate dump_* function.

    Args:
        app:        Initialized coframe app (setup_schema() is sufficient)
        args:       Parsed argparse.Namespace
        output_dir: Base directory for default output paths.
                    devtest uses Path('data'), a standalone CLI would use Path('.')
                    or a user-supplied --output-dir.
    """
    out_arg = getattr(args, 'output', None) or ''

    if args.command == 'dump-page':
        try:
            yaml_text, label = dump_page(app, args.page_id, auto=args.auto, raw=args.raw)
        except ValueError as e:
            print(f'Error: {e}', file=sys.stderr)
            sys.exit(1)
        _write(yaml_text, label, out_arg,
               output_dir / 'pages' / f'{args.page_id}.yaml')

    elif args.command == 'dump-table':
        names = args.table or None
        try:
            yaml_text, label = dump_table(app, names)
        except ValueError as e:
            print(f'Error: {e}', file=sys.stderr)
            sys.exit(1)
        default = (output_dir / 'tables' / f'{names[0]}.yaml'
                   if names and len(names) == 1
                   else output_dir / 'tables' / 'all.yaml')
        _write(yaml_text, label, out_arg, default)

    elif args.command == 'dump-types':
        yaml_text, label = dump_types(app, include_builtin=args.include_builtin)
        _write(yaml_text, label, out_arg, output_dir / 'types.yaml')

    elif args.command == 'check':
        import json
        from coframe.diagnostics import run_checks, dump_app

        issues = run_checks(app)
        n_errors = print_issues(issues)

        if args.dump is not None:
            dump = dump_app(app)
            out_path = Path(args.dump) if args.dump else output_dir / 'appdump.json'
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                json.dumps(dump, indent=2, ensure_ascii=False, default=str),
                encoding='utf-8')
            print(f'Full dump written: {out_path}')

        if n_errors:
            sys.exit(1)

    elif args.command == 'db-check':
        report, aligned = db_check(app)
        print(report)
        if not aligned:
            sys.exit(1)

    elif args.command == 'db-sync':
        report, aligned = db_sync(app, dry_run=args.dry_run)
        print(report)
        if not aligned:
            sys.exit(1)

    else:
        make_parser().print_help()
        sys.exit(1)
