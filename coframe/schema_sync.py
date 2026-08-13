"""
Schema alignment — keep the database in the shape the plugins describe.

State-based, not migration-based: there is no revision history and no version
table.  The desired state is the merged YAML schema (i.e. the metadata of the
generated model), the database is compared against it on the spot, and the
difference is either applied or reported.  Two consequences worth knowing:

  * adding or removing a plugin needs no bookkeeping — the next diff simply
    computes a different target;
  * nothing here transforms data.  A rename cannot be told apart from a drop
    plus an add, so renames, splits and merges stay manual.

What may be applied automatically is a closed list (see `_classify`): create a
table, add a column, add an index, widen a type, relax a NOT NULL.  Everything
else — every drop, every narrowing, every constraint change — is reported with
the SQL it would take, and left to a human.  The guard is structural rather
than a flag: no code path here removes anything.  That is what makes the
command safe to run against a database that is only partly ours — a subset of
plugins loaded, or tables owned by a host application, which the diff ignores
entirely.

Alembic supplies the comparison engine and the operations (including the
move-and-copy that SQLite needs to alter a column); its versioning half is
deliberately unused, and the dependency is imported lazily so that an app that
never syncs need not install it.

Entry points:
    diff_schema()     compare database against metadata -> SchemaDiff
    format_diff()     human-readable report
    plan_sql()        the DDL a sync would run, rendered without touching the DB
    apply_diff()      run the safe subset, return the SQL executed
    check_on_startup() the deploy-time guard wired into DB.initialize_db()
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy as sa
from sqlalchemy.engine import Engine

# Verdicts
SAFE = 'safe'          # inside the closed list — db-sync applies it
REFUSED = 'refused'    # a human decides: reported with the SQL it would take
IGNORED = 'ignored'    # noise we deliberately do not track (server defaults, comments)


class SchemaOutOfDateError(RuntimeError):
    """The database does not match the schema the plugins describe."""


class AlembicMissingError(RuntimeError):
    """Alembic is needed for schema comparison but is not installed."""


def _alembic():
    """Import the three Alembic entry points, with a usable error if absent."""
    try:
        from alembic.autogenerate import compare_metadata
        from alembic.migration import MigrationContext
        from alembic.operations import Operations
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise AlembicMissingError(
            "Schema comparison needs Alembic: pip install alembic"
        ) from exc
    return compare_metadata, MigrationContext, Operations


# ── The diff ───────────────────────────────────────────────────────────────────

@dataclass
class Change:
    """One difference between the database and the schema."""
    kind: str                     # add_table, add_column, widen_type, …
    verdict: str                  # SAFE | REFUSED | IGNORED
    target: str                   # 'book' or 'book.isbn'
    description: str              # what it is, in one line
    reason: str = ''              # why it was refused
    sql_hint: str = ''            # the statement a human would run instead
    payload: Dict[str, Any] = field(default_factory=dict)  # what apply_diff needs

    def __str__(self) -> str:
        return f'{self.target}: {self.description}'


@dataclass
class SchemaDiff:
    """The full comparison, split by verdict."""
    changes: List[Change] = field(default_factory=list)

    @property
    def safe(self) -> List[Change]:
        return [c for c in self.changes if c.verdict == SAFE]

    @property
    def refused(self) -> List[Change]:
        return [c for c in self.changes if c.verdict == REFUSED]

    @property
    def ignored(self) -> List[Change]:
        return [c for c in self.changes if c.verdict == IGNORED]

    @property
    def is_aligned(self) -> bool:
        """True when nothing but ignored noise separates database and schema."""
        return not self.safe and not self.refused


# ── Type comparison ────────────────────────────────────────────────────────────

_INT_RANK = {'SMALLINT': 1, 'INTEGER': 2, 'INT': 2, 'BIGINT': 3}


def _int_rank(type_: Any) -> int:
    name = type_.__class__.__name__.upper()
    if name in _INT_RANK:
        return _INT_RANK[name]
    if isinstance(type_, sa.BigInteger):
        return 3
    if isinstance(type_, sa.SmallInteger):
        return 1
    return 2


def _is_widening(old: Any, new: Any) -> bool:
    """
    True when the new type can hold every value the old one could.

    Only the cases worth automating are recognised; anything unrecognised is
    reported rather than applied, so a false negative costs a manual statement
    while a false positive would cost data.
    """
    # Enum is a String subclass but its value set is not a length — never guess.
    if isinstance(old, sa.Enum) or isinstance(new, sa.Enum):
        return False

    if isinstance(old, sa.String) and isinstance(new, sa.String):
        if new.length is None:      # unbounded (Text) holds anything
            return True
        if old.length is None:      # bounded cannot hold an unbounded column
            return False
        return new.length >= old.length

    if isinstance(old, sa.Integer) and isinstance(new, sa.Integer):
        return _int_rank(new) >= _int_rank(old)

    if isinstance(old, sa.Numeric) and isinstance(new, sa.Numeric):
        if isinstance(old, sa.Float) != isinstance(new, sa.Float):
            return False
        if new.precision is None:
            return True
        if old.precision is None:
            return False
        old_scale = old.scale or 0
        new_scale = new.scale or 0
        # Both the integer part and the decimal part must keep their room.
        return (new.precision - new_scale) >= (old.precision - old_scale) and new_scale >= old_scale

    return False


# ── Column defaults, used to fill existing rows ────────────────────────────────

_UNRESOLVED = object()


def _default_value(column: Any) -> Any:
    """
    The value existing rows should get for a newly added NOT NULL column.

    YAML `default:` becomes a Python-side SQLAlchemy default, which only fires
    on INSERT — rows already in the table need it applied explicitly.  A
    callable default (`datetime.now`, `$op_date`) is called once, so every
    existing row gets the same value: the moment of the sync, which is the
    honest answer when the real one was never recorded.
    """
    default = column.default
    if default is None:
        return _UNRESOLVED
    if getattr(default, 'is_callable', False):
        for args in ((None,), ()):
            try:
                return default.arg(*args)
            except TypeError:
                continue
        return _UNRESOLVED
    if getattr(default, 'is_scalar', False) or not hasattr(default, 'arg'):
        return getattr(default, 'arg', default)
    return _UNRESOLVED


def _quote(value: Any) -> str:
    """Render a value for a SQL hint (display only — never executed)."""
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


# ── Classification ─────────────────────────────────────────────────────────────

def _table_row_count(conn: Any, table_name: str) -> int:
    return conn.execute(
        sa.select(sa.func.count()).select_from(sa.table(table_name))
    ).scalar_one()


def _classify_add_column(conn: Any, table_name: str, column: Any) -> Change:
    """
    A new column is safe unless it is NOT NULL and existing rows have no value
    to put in it.
    """
    target = f'{table_name}.{column.name}'
    type_str = str(column.type)

    if column.nullable:
        return Change(kind='add_column', verdict=SAFE, target=target,
                      description=f'new column {type_str}',
                      payload={'table': table_name, 'column': column})

    if column.server_default is not None:
        return Change(kind='add_column', verdict=SAFE, target=target,
                      description=f'new column {type_str} NOT NULL, server default',
                      payload={'table': table_name, 'column': column})

    value = _default_value(column)
    if value is not _UNRESOLVED:
        return Change(kind='add_column', verdict=SAFE, target=target,
                      description=f'new column {type_str} NOT NULL, '
                                  f'existing rows set to {value!r}',
                      payload={'table': table_name, 'column': column, 'backfill': value})

    if _table_row_count(conn, table_name) == 0:
        return Change(kind='add_column', verdict=SAFE, target=target,
                      description=f'new column {type_str} NOT NULL (table is empty)',
                      payload={'table': table_name, 'column': column})

    return Change(
        kind='add_column', verdict=REFUSED, target=target,
        description=f'new column {type_str} NOT NULL',
        reason='NOT NULL without a default, and the table has rows: '
               'existing rows have no value to receive',
        sql_hint=f'ALTER TABLE {table_name} ADD COLUMN {column.name} {type_str};\n'
                 f'UPDATE {table_name} SET {column.name} = <value>;',
    )


def _classify(conn: Any, diff: Any) -> Optional[Change]:
    """Turn one Alembic diff tuple into a Change, or None to drop it silently."""
    kind = diff[0]

    if kind == 'add_table':
        table = diff[1]
        return Change(kind='add_table', verdict=SAFE, target=table.name,
                      description=f'new table ({len(table.columns)} columns)',
                      payload={'table_obj': table})

    if kind == 'add_column':
        return _classify_add_column(conn, diff[2], diff[3])

    if kind == 'add_index':
        index = diff[1]
        return Change(kind='add_index', verdict=SAFE, target=index.table.name,
                      description=f'new index {index.name} '
                                  f'({", ".join(c.name for c in index.columns)})',
                      payload={'index': index})

    if kind == 'modify_type':
        _, _, table_name, col_name, existing, old_type, new_type = diff
        target = f'{table_name}.{col_name}'
        if _is_widening(old_type, new_type):
            return Change(kind='widen_type', verdict=SAFE, target=target,
                          description=f'type {old_type} -> {new_type}',
                          payload={'table': table_name, 'column': col_name,
                                   'old_type': old_type, 'new_type': new_type,
                                   'existing': existing})
        return Change(
            kind='modify_type', verdict=REFUSED, target=target,
            description=f'type {old_type} -> {new_type}',
            reason='not a widening: existing values may not fit the new type',
            sql_hint=f'ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE {new_type};',
        )

    if kind == 'modify_nullable':
        _, _, table_name, col_name, existing, old_null, new_null = diff
        target = f'{table_name}.{col_name}'
        if old_null is False and new_null is True:
            return Change(kind='relax_nullable', verdict=SAFE, target=target,
                          description='NOT NULL -> nullable',
                          payload={'table': table_name, 'column': col_name,
                                   'existing': existing, 'nullable': True})
        return Change(
            kind='modify_nullable', verdict=REFUSED, target=target,
            description='nullable -> NOT NULL',
            reason='existing rows may hold NULL',
            sql_hint=f'UPDATE {table_name} SET {col_name} = <value> '
                     f'WHERE {col_name} IS NULL;\n'
                     f'ALTER TABLE {table_name} ALTER COLUMN {col_name} SET NOT NULL;',
        )

    # Server defaults and comments are reflected too unevenly across dialects to
    # be worth chasing; they never affect what the application can store.
    if kind in ('modify_default', 'modify_comment', 'add_table_comment',
                'remove_table_comment'):
        return Change(kind=kind, verdict=IGNORED, target=str(diff[2] if len(diff) > 2 else diff[1]),
                      description='default/comment difference — not tracked')

    if kind == 'remove_column':
        _, _, table_name, column = diff
        return Change(
            kind='remove_column', verdict=REFUSED, target=f'{table_name}.{column.name}',
            description='column exists in the database but not in the schema',
            reason='dropping a column destroys data and usually needs UI changes too',
            sql_hint=f'ALTER TABLE {table_name} DROP COLUMN {column.name};',
        )

    if kind == 'remove_index':
        index = diff[1]
        return Change(
            kind='remove_index', verdict=REFUSED, target=index.table.name,
            description=f'index {index.name} exists in the database but not in the schema',
            reason='an index may have been added on purpose outside the schema',
            sql_hint=f'DROP INDEX {index.name};',
        )

    # Constraints, foreign keys, anything a future Alembic reports: refuse by
    # default.  An unknown difference is exactly the case not to guess about.
    target = ''
    for item in diff[1:]:
        name = getattr(item, 'name', None)
        if isinstance(name, str):
            target = name
            break
    return Change(
        kind=kind, verdict=REFUSED, target=target or '?',
        description=f'{kind.replace("_", " ")}',
        reason='constraint and key changes are not applied automatically',
    )


# ── Public API ─────────────────────────────────────────────────────────────────

def _primary_key_changes(engine: Engine, metadata: sa.MetaData) -> List[Change]:
    """
    Report a table whose key in the database is not the key in the schema.

    The comparison engine does not look at constraints, so a changed key is
    invisible to it — it only sees the shadow it casts, a column appearing and
    an index appearing. Where that shadow is applicable (an empty table, a
    column with a default) a sync would add the column and stop, leaving a
    database that reports itself aligned while the key is somewhere else: the
    silent divergence this whole module exists to prevent.

    Nothing here is ever applied. Changing a key means rebuilding the table —
    new shape, copy the rows, swap — which is a data transformation, and the
    closed list stops before those on purpose.
    """
    changes: List[Change] = []
    inspector = sa.inspect(engine)
    existing = set(inspector.get_table_names())

    for table in metadata.tables.values():
        if table.name not in existing:
            continue
        in_db = tuple(inspector.get_pk_constraint(table.name).get('constrained_columns') or ())
        in_schema = tuple(col.name for col in table.primary_key.columns)
        if in_db == in_schema:
            continue
        changes.append(Change(
            kind='modify_primary_key', verdict=REFUSED, target=table.name,
            description=f'primary key ({", ".join(in_db) or "none"}) '
                        f'-> ({", ".join(in_schema) or "none"})',
            reason='a key change rebuilds the table: the comparison engine cannot see it, '
                   'and no sync applies it',
            sql_hint=f'CREATE TABLE {table.name}__new (…);\n'
                     f'INSERT INTO {table.name}__new SELECT … FROM {table.name};\n'
                     f'DROP TABLE {table.name};\n'
                     f'ALTER TABLE {table.name}__new RENAME TO {table.name};',
        ))
    return changes


def diff_schema(engine: Engine, metadata: sa.MetaData) -> SchemaDiff:
    """
    Compare the database behind `engine` with `metadata`.

    Tables the schema does not declare are not looked at at all: they may
    belong to a host application, to another tenant, or to a plugin that is not
    loaded, and nothing distinguishes those from a table someone meant to
    remove — so a whole table missing from the schema is silence, by design.
    Inside a declared table the question is different: the table is ours, so a
    column the schema no longer declares is a real difference and gets
    reported (never applied).
    """
    compare_metadata, MigrationContext, _ = _alembic()

    known = {t.name for t in metadata.tables.values()}

    def include_object(obj, name, type_, reflected, compare_to):
        return name in known if type_ == 'table' else True

    changes: List[Change] = []
    with engine.connect() as conn:
        context = MigrationContext.configure(
            conn,
            opts={'compare_type': True, 'include_object': include_object},
        )
        for raw in compare_metadata(context, metadata):
            # Column-level differences arrive grouped in a list.
            for item in (raw if isinstance(raw, list) else [raw]):
                change = _classify(conn, item)
                if change is not None:
                    changes.append(change)

    changes.extend(_primary_key_changes(engine, metadata))
    return SchemaDiff(changes=_ordered(changes))


def _ordered(changes: List[Change]) -> List[Change]:
    """Report them in the order a sync carries them out (see `_run`)."""
    rank = {'add_table': 0, 'add_column': 1, 'widen_type': 2,
            'relax_nullable': 2, 'add_index': 3}
    return sorted(changes, key=lambda c: rank.get(c.kind, 4))


def format_diff(diff: SchemaDiff, sync_command: Optional[str] = 'db-sync') -> str:
    """
    Render the comparison for a terminal.  `sync_command` names the command
    that would apply it; pass None when the caller is that command.
    """
    if diff.is_aligned:
        return 'Database aligned with the schema.'

    lines: List[str] = []

    if diff.safe:
        lines.append(f'Applicable automatically ({len(diff.safe)}):')
        for change in diff.safe:
            lines.append(f'  + {change.target:<40} {change.description}')

    if diff.refused:
        if lines:
            lines.append('')
        lines.append(f'Needs a decision ({len(diff.refused)}) — not applied:')
        for change in diff.refused:
            lines.append(f'  ! {change.target:<40} {change.description}')
            lines.append(f'      {change.reason}')
            for statement in change.sql_hint.splitlines():
                lines.append(f'      {statement}')

    if diff.safe and sync_command:
        lines.append('')
        lines.append(f'Run `{sync_command}` to apply the {len(diff.safe)} change(s) above.')

    return '\n'.join(lines)


def _batch_needed(engine: Engine) -> bool:
    """SQLite has no ALTER COLUMN: altering one means rebuilding the table."""
    return engine.dialect.name == 'sqlite'


def _copy_of(column: Any, *, nullable: Optional[bool] = None) -> Any:
    """A detached copy of a metadata column, so the live metadata is not touched."""
    copy = column._copy() if hasattr(column, '_copy') else column.copy()
    if nullable is not None:
        copy.nullable = nullable
    return copy


class _Shape:
    """
    The tables as a rebuild will find them, while rendering offline.

    Online, a rebuild reflects the table itself and sees whatever the earlier
    steps did to it.  Offline nothing is executed, so the same knowledge has to
    be carried by hand: the table is reflected once and then kept up to date
    with every step rendered, or a plan touching one table twice would describe
    the second step against a table that no longer exists in that form.
    """

    def __init__(self, conn: Any) -> None:
        self.conn = conn
        self.tables: Dict[str, sa.Table] = {}

    def of(self, table: str) -> sa.Table:
        if table not in self.tables:
            self.tables[table] = sa.Table(table, sa.MetaData(), autoload_with=self.conn)
        return self.tables[table]

    def column_added(self, table: str, column: Any) -> None:
        self.of(table).append_column(column)

    def column_altered(self, table: str, column: str, type_: Any,
                       nullable: Optional[bool]) -> None:
        target = self.of(table).c[column]
        if type_ is not None:
            target.type = type_
        if nullable is not None:
            target.nullable = nullable


def _literal(statement: Any, engine: Engine) -> Any:
    """
    Render bound values inline, for the plan only: a dry run showing
    `SET copies=?` would hide the very thing there is to review.  What actually
    runs keeps its parameters bound, where the driver handles the typing.
    """
    try:
        return str(statement.compile(dialect=engine.dialect,
                                     compile_kwargs={'literal_binds': True}))
    except Exception:
        return statement


def _run(op: Any, conn: Any, engine: Engine, changes: List[Change],
         reflect: bool) -> None:
    """
    Emit the operations for `changes` through the Alembic operations object.

    Everything goes through `op`, never through the connection directly: that
    is what lets the very same code render the plan without executing it.  In
    offline mode `op.get_bind()` is a buffer wearing the shape of a connection,
    so a statement sent to it is written down instead of run.

    `reflect` is False when rendering offline, where a rebuild cannot look at
    the database and is handed the table to copy instead.

    The phases are what keep a table from being rebuilt more than once: new
    columns and their backfills first, then every alteration that table needs
    in a single pass, then the indexes — which may sit on a column added a
    moment ago.
    """
    shape = None if reflect else _Shape(conn)
    alterations: Dict[str, List[Dict[str, Any]]] = {}
    indexes: List[Any] = []

    def alter(table: str, **kwargs: Any) -> None:
        alterations.setdefault(table, []).append(kwargs)

    for change in changes:
        kind = change.kind
        payload = change.payload

        if kind == 'add_table':
            payload['table_obj'].create(op.get_bind(), checkfirst=False)

        elif kind == 'add_column':
            table, column = payload['table'], payload['column']
            if 'backfill' in payload:
                # Three steps, because a Python-side default never reaches rows
                # that already exist: widen the door, fill, then lock.  The
                # locking is left to the alteration phase, where it costs the
                # same rebuild as everything else this table needs.
                op.add_column(table, _copy_of(column, nullable=True))
                target = sa.table(table, sa.column(column.name, column.type))
                update = target.update().values(**{column.name: payload['backfill']})
                op.execute(_literal(update, engine) if shape else update)
                if shape:
                    shape.column_added(table, _copy_of(column, nullable=True))
                alter(table, column=column.name, existing_type=column.type,
                      nullable=False)
            else:
                op.add_column(table, _copy_of(column))
                if shape:
                    shape.column_added(table, _copy_of(column))

        elif kind == 'widen_type':
            alter(payload['table'], column=payload['column'],
                  existing_type=payload['old_type'], type_=payload['new_type'],
                  nullable=payload['existing'].get('existing_nullable'))

        elif kind == 'relax_nullable':
            alter(payload['table'], column=payload['column'], nullable=True)

        elif kind == 'add_index':
            indexes.append(payload['index'])

    for table, columns in alterations.items():
        _alter_table(op, shape, engine, table, columns)

    # Last: a rebuild only restores the indexes the table had when it was
    # copied, so one created earlier in the run would go down with it.
    for index in indexes:
        op.create_index(index.name, index.table.name,
                        [c.name for c in index.columns], unique=index.unique)


def _alter_table(op: Any, shape: Optional['_Shape'], engine: Engine, table: str,
                 columns: List[Dict[str, Any]]) -> None:
    """
    Alter every column of one table in a single pass.

    Where the dialect can alter a column in place that is just a loop; on
    SQLite it is one table rebuild instead of one per column, which on a large
    table is the difference that matters.
    """
    if not _batch_needed(engine):
        for spec in columns:
            op.alter_column(table, spec.pop('column'), **spec)
        return

    # Rebuilding copies the table as it stands, so columns the schema does not
    # know about — and the data in them — come across untouched.
    batch_kwargs: Dict[str, Any] = {}
    if shape is not None:
        batch_kwargs['copy_from'] = shape.of(table)
    with op.batch_alter_table(table, **batch_kwargs) as batch:
        for spec in columns:
            batch.alter_column(spec['column'],
                               **{k: v for k, v in spec.items() if k != 'column'})
    if shape is not None:
        for spec in columns:
            shape.column_altered(table, spec['column'],
                                 spec.get('type_'), spec.get('nullable'))


def plan_sql(engine: Engine, diff: SchemaDiff) -> str:
    """
    The DDL a sync would run, rendered without touching the database.

    Alembic's offline mode writes the statements to a buffer instead of the
    connection; nothing is executed, and the connection is only used to reflect
    the tables a rebuild copies from.
    """
    import io

    _, MigrationContext, Operations = _alembic()

    if not diff.safe:
        return ''

    buffer = io.StringIO()
    with engine.connect() as conn:
        context = MigrationContext.configure(
            conn, opts={'as_sql': True, 'output_buffer': buffer})
        op = Operations(context)
        _run(op, conn, engine, diff.safe, reflect=False)
    return buffer.getvalue()


def apply_diff(engine: Engine, diff: SchemaDiff,
               logger: Optional[logging.Logger] = None) -> List[str]:
    """
    Run the safe subset of `diff`.  Returns the SQL statements executed.

    Refused changes are not touched — they are not even represented here as
    operations.  What ran is logged, since a state-based sync keeps no history
    of its own.
    """
    _, MigrationContext, Operations = _alembic()

    if not diff.safe:
        return []

    executed: List[str] = []

    @sa.event.listens_for(engine, 'before_cursor_execute')
    def _capture(conn, cursor, statement, parameters, context, executemany):
        executed.append(' '.join(statement.split()))

    try:
        with engine.begin() as conn:
            context = MigrationContext.configure(conn)
            op = Operations(context)
            _run(op, conn, engine, diff.safe, reflect=True)
    finally:
        sa.event.remove(engine, 'before_cursor_execute', _capture)

    ddl = [s for s in executed
           if s.upper().startswith(('CREATE', 'ALTER', 'DROP', 'UPDATE', 'INSERT INTO'))]
    if logger:
        for change in diff.safe:
            logger.info('schema sync: %s — %s', change.target, change.description)
        for statement in ddl:
            logger.info('schema sync SQL: %s', statement)
    return ddl


def check_on_startup(engine: Engine, metadata: sa.MetaData, policy: str,
                     logger: Optional[logging.Logger] = None,
                     sync_command: str = 'db-sync') -> Optional[SchemaDiff]:
    """
    Compare at boot and act according to `policy`: 'error' (default) stops the
    server, 'warn' logs, 'off' skips the comparison altogether.

    Failing at startup is the cheap outcome: the expensive one is an
    application serving requests against a database it no longer matches.
    """
    if policy in (None, '', 'off', False):
        return None
    if policy not in ('error', 'warn'):
        raise ValueError(
            f"Unknown migrations.on_startup policy: {policy!r} (error | warn | off)")

    diff = diff_schema(engine, metadata)
    if diff.is_aligned:
        return diff

    report = format_diff(diff, sync_command=sync_command)
    if policy == 'error':
        raise SchemaOutOfDateError(
            'Database does not match the schema described by the plugins.\n\n'
            + report)
    (logger or logging.getLogger(__name__)).warning(
        'Database does not match the schema described by the plugins.\n%s', report)
    return diff
