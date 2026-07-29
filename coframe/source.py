import inspect
import importlib.util
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Tuple
from coframe.db import DB, DbColumn, DbTable


class ModelImportManager:
    """
    Manages imports for SQLAlchemy model generation.
    Collects and organizes different types of imports.
    """

    def __init__(self, db: DB) -> None:
        """
        Initialize import manager.

        Args:
            db: Database schema manager instance
        """
        self.db = db
        self.standard_imports: Set[str] = set()  # Python standard imports
        self.column_imports: Set[str] = set()  # SQLAlchemy Column types
        self.orm_imports: Set[str] = {"Mapped", "mapped_column"}  # SQLAlchemy ORM features

        # Add configured imports from plugins
        self._add_configured_imports()

    def _add_configured_imports(self) -> None:
        """Add imports configured in plugins."""
        # Add global imports from main config
        for imp in self.db.pm.config.get('source_imports', []):
            self.standard_imports.add(imp)

        # Add imports from individual plugins
        for plugin_name, plugin_data in self.db.pm.plugins.items():
            for imp in plugin_data.config.get('source_imports', []):
                self.standard_imports.add(imp)

    def add_python_type_import(self, python_type: str) -> None:
        """
        Add import for Python type.

        Args:
            python_type: Name of Python type to import
        """
        if python_type == 'datetime':
            self.standard_imports.add("from datetime import datetime")
        elif python_type == 'date':
            self.standard_imports.add("from datetime import date")
        elif python_type == 'time':
            self.standard_imports.add("from datetime import time")
        elif python_type == 'Decimal':
            self.standard_imports.add("from decimal import Decimal")

    def add_relationship_imports(self) -> None:
        """Add imports needed for relationship definitions."""
        self.orm_imports.add("relationship")
        self.standard_imports.add("from typing import List")

    def generate_import_statements(self) -> str:
        """
        Generate formatted import statements.

        Returns:
            String with all import statements
        """
        import_statements = []

        # Add standard imports
        if self.standard_imports:
            import_statements.append('\n'.join(sorted(self.standard_imports)))
            import_statements.append('')

        # Add SQLAlchemy Column imports
        if self.column_imports:
            import_statements.append(f"from sqlalchemy import {', '.join(sorted(self.column_imports))}")

        # Add SQLAlchemy ORM imports
        if self.orm_imports:
            import_statements.append(f"from sqlalchemy.orm import {', '.join(sorted(self.orm_imports))}")

        # Add declared_attr for dynamic __tablename__
        import_statements.append('from sqlalchemy.ext.declarative import declared_attr')

        # Add Base import and coframe utilities
        import_statements.append('from coframe.db import Base, BaseApp')
        import_statements.append('from coframe.utils import resolve_table_name')
        import_statements.append('import coframe')

        return '\n'.join(import_statements)


class PluginClassFinder:
    """
    Finds and imports Python classes from plugin source files
    that match table names for inheritance.
    """

    def __init__(self, plugins_manager):
        """
        Initialize the class finder with the plugins manager.

        Args:
            plugins_manager: The plugins manager instance
        """
        self.pm = plugins_manager
        self.class_map: Dict[str, List[Tuple[str, str]]] = {}  # Table name -> [(module_path, class_name)]
        self.imported_modules: Dict[str, object] = {}  # Module path -> module object

    def scan_plugin_sources(self) -> None:
        """
        Scan all plugin source files for classes that match table names.
        """
        for plugin_name, plugin in self.pm.plugins.items():
            for source_file in plugin.sources:
                self._process_source_file(source_file, plugin)

    def _process_source_file(self, source_file: Path, plugin) -> None:
        """
        Process a single source file to find relevant classes.

        Args:
            source_file: Path to the source file
            plugin: Plugin object that owns the source file
        """

        # Construct the module path relative to the plugin's root.
        # load_plugins() puts each plugin *root* on sys.path, so every plugin is
        # importable by its own folder name — regardless of where that root lives
        # on disk (this is what makes out-of-tree roots like ../commons work).
        # Deriving the module from the full filesystem path instead would produce
        # invalid names for out-of-tree roots (e.g. '..commons.common.model').
        module_path = f"{source_file.parent.name}.{source_file.stem}"

        try:
            # Import the module
            if module_path not in self.imported_modules:
                self.imported_modules[module_path] = importlib.import_module(module_path)

            module = self.imported_modules[module_path]

            # Scan for classes
            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Check if class is defined in this module (not imported)
                if obj.__module__ == module_path:
                    # Add to class map for potential table matching
                    if name not in self.class_map:
                        self.class_map[name] = []
                    self.class_map[name].append((module_path, name))

        except (ImportError, AttributeError) as e:
            print(f"Error importing module {module_path}: {e}")

    def get_class_inheritance(self, table_name: str) -> List[str]:
        """
        Get the inheritance paths for a table.

        Args:
            table_name: Name of the table to find inheritance for

        Returns:
            List of fully qualified class paths for inheritance
        """
        result = []
        if table_name in self.class_map:
            for module_path, class_name in self.class_map[table_name]:
                result.append(f"{module_path}.{class_name}")
        return result


@dataclass
class ForeignKeyRelation:
    """
    One foreign key, collected while columns are generated and turned into a pair
    of relationship() attributes only once every table has been seen.

    The deferral is what makes disambiguation possible: how a relationship must be
    named — and whether the join needs spelling out — depends on the *other* foreign
    keys in the model, which are not known while a single column is being written.
    """
    table: DbTable            # table that owns the FK column
    column: str               # the FK column
    fk_table: DbTable         # referenced table
    fk_id: str                # referenced column
    soft: bool                # no DB-level constraint (see § 4.5)
    relation: Optional[str]   # explicit forward name from YAML, if given
    backref: Optional[str]    # explicit back name from YAML, if given

    @property
    def self_referential(self) -> bool:
        return self.table.name == self.fk_table.name


class RelationshipManager:
    """
    Manages relationships between tables for model generation.

    Every foreign key produces two Python attributes: the *forward* one on the table
    that holds the column (`Loan.book`, many-to-one) and the *back* one on the table
    referenced (`Book.loans`, one-to-many). Neither exists in SQL — only the column
    and its constraint do — so their names are free, and are chosen here.
    """

    def __init__(self) -> None:
        """Initialize relationship manager."""
        self.direct_relations: Dict[str, List[str]] = {}  # Table name -> list of relation statements
        self.back_relations: Dict[str, List[str]] = {}  # Table name -> list of backref statements
        self.foreign_keys: List[ForeignKeyRelation] = []
        self._resolved = False

    def add_foreign_key_relation(self, table: DbTable, column_name: str, fk_table: DbTable,
                                 fk_id: str = 'id', soft: bool = False,
                                 relation: Optional[str] = None,
                                 backref: Optional[str] = None) -> None:
        """
        Record a foreign key. The code is emitted later, by resolve().

        Args:
            table: Table owning the foreign key column
            column_name: Current column name
            fk_table: Foreign table object
            fk_id: Referenced column on the foreign table (default 'id')
            soft: If True the column has no ForeignKey constraint — the join must
                  be stated explicitly (primaryjoin) since SQLAlchemy can no longer
                  infer it from a constraint.
            relation: Explicit name for the forward attribute (YAML `relation:`)
            backref: Explicit name for the back attribute (YAML `backref:`)
        """
        if self._resolved:
            # Only reachable if a FK column is generated outside the table pass —
            # a mixin type, today. Silently dropping it would leave a model with a
            # column and no way to navigate it.
            raise ValueError(
                f"Foreign key '{table.name}.{column_name}' was declared after relationships "
                f"were resolved: foreign keys are not supported on mixins."
            )
        self.foreign_keys.append(ForeignKeyRelation(
            table=table, column=column_name, fk_table=fk_table, fk_id=fk_id,
            soft=soft, relation=relation, backref=backref,
        ))

    def resolve(self) -> None:
        """
        Turn the collected foreign keys into relationship() statements.

        Runs once, after every table has been processed, because both the naming and
        the join spec depend on the model as a whole.
        """
        if self._resolved:
            return
        self._resolved = True

        # More than one FK from the same table to the same target means the back
        # side cannot keep the plain table name: both would land on the same
        # attribute of the target class.
        pair_count: Dict[Tuple[str, str], int] = {}
        for fk in self.foreign_keys:
            key = (fk.table.name, fk.fk_table.name)
            pair_count[key] = pair_count.get(key, 0) + 1

        named = []
        for fk in self.foreign_keys:
            forward = fk.relation or self._relation_name(fk)
            ambiguous = pair_count[(fk.table.name, fk.fk_table.name)] > 1
            back = fk.backref or self._back_name(fk, forward, ambiguous)
            named.append((fk, forward, back))

        self._check_names(named)

        for fk, forward, back in named:
            self._emit(fk, forward, back)

    def _relation_name(self, fk: ForeignKeyRelation) -> str:
        """
        Name of the forward attribute: the column without its key suffix.

        `book_id` -> `book`, `merged_into_id` -> `merged_into`, and any other suffix
        works the same way (`codice_esterno_fk` -> `codice_esterno`) because the cut
        is at the last underscore, not at a known list of suffixes.

        A column with nothing to cut — a legacy `codcli` — falls back to the name of
        the referenced table, which cannot clash with the column precisely because
        the column does not follow the convention. `relation:` overrides both.
        """
        name, separator, _ = fk.column.rpartition('_')
        return name if separator and name else fk.fk_table.name.lower()

    def _back_name(self, fk: ForeignKeyRelation, forward: str, ambiguous: bool) -> str:
        """
        Name of the back attribute: the source table, suffixed with the forward name
        when the same pair of tables is linked more than once (`partners_parent`,
        `partners_merged_into`). `backref:` overrides it — and should, since a good
        name is rarely mechanical (`children`, `duplicates`).
        """
        name = fk.table.table_name.lower()
        return f"{name}_{forward}" if ambiguous else name

    def _check_names(self, named: List[Tuple[ForeignKeyRelation, str, str]]) -> None:
        """
        Refuse a model in which two attributes of the same class would share a name.

        Without this the second definition simply overwrites the first in the class
        body — a relationship silently replacing a column, or one of two foreign keys
        disappearing — which is the failure mode this whole pass exists to remove.
        """
        # class name -> attribute name -> what claims it
        claims: Dict[str, Dict[str, str]] = {}

        def claim(owner: DbTable, attr: str, source: str) -> None:
            owned = claims.setdefault(owner.name, {})
            if attr in owned:
                raise ValueError(
                    f"Relationship name clash on '{owner.name}': '{attr}' is claimed by "
                    f"{owned[attr]} and by {source}. Name one of them explicitly with "
                    f"`relation:` (forward) or `backref:` (reverse) in the foreign key."
                )
            owned[attr] = source

        for fk, forward, back in named:
            claim(fk.table, forward, f"the foreign key '{fk.table.name}.{fk.column}'")
            claim(fk.fk_table, back, f"the reverse of '{fk.table.name}.{fk.column}'")

        # A relationship may also collide with a real column of the same class.
        involved: Dict[str, DbTable] = {}
        for fk, _, _ in named:
            involved[fk.table.name] = fk.table
            involved[fk.fk_table.name] = fk.fk_table

        for table_name, table in involved.items():
            owned = claims.get(table_name, {})
            for column in table.effective_columns:
                if column.name in owned:
                    raise ValueError(
                        f"Relationship name clash on '{table_name}': '{column.name}' is both a "
                        f"column and {owned[column.name]}. Name the relationship explicitly with "
                        f"`relation:` or `backref:` in the foreign key."
                    )

    def _emit(self, fk: ForeignKeyRelation, forward: str, back: str) -> None:
        """Write the two relationship() statements for one foreign key."""
        indent = " " * 4
        local = f"{fk.table.name}.{fk.column}"

        # `foreign_keys` is emitted unconditionally. SQLAlchemy can infer it whenever
        # a single FK path links the two tables, but a *second* path — added later,
        # possibly by another plugin merging a column into either table, in either
        # direction — makes the inference ambiguous and breaks relationships that no
        # one touched. Stating it keeps each table's code independent of the rest.
        common = f", foreign_keys='{local}'"

        # Without a constraint SQLAlchemy has no join condition to infer; spell it out.
        if fk.soft:
            common += f", primaryjoin='{local} == {fk.fk_table.name}.{fk.fk_id}'"

        # Self-reference: both sides are the same class, so nothing tells SQLAlchemy
        # which one is the "one" side. remote_side does.
        remote = f", remote_side='{fk.fk_table.name}.{fk.fk_id}'" if fk.self_referential else ""

        relation = self.add_relation_key(fk.table.name, self.direct_relations)
        relation.append(
            f"{indent}{forward}: Mapped['{fk.fk_table.name}'] = "
            f"relationship('{fk.fk_table.name}'{common}{remote}, back_populates='{back}')\n"
        )

        relation = self.add_relation_key(fk.fk_table.name, self.back_relations)
        relation.append(
            f"{indent}{back}: Mapped[List['{fk.table.name}']] = "
            f"relationship('{fk.table.name}'{common}, back_populates='{forward}')\n"
        )

    def add_relation_key(self, name: str, relation: Dict[str, List[str]] = {}) -> List[str]:
        if name not in relation:
            relation[name] = []
        return relation[name]


class ColumnGenerator:
    """
    Generates column definitions for SQLAlchemy models.
    Handles column types, attributes, and relationships.
    """

    def __init__(self, db: DB, imports: ModelImportManager, relationships: RelationshipManager) -> None:
        """
        Initialize column generator.

        Args:
            db: Database schema manager
            imports: Import manager
            relationships: Relationship manager
        """
        self.db = db
        self.imports = imports
        self.relationships = relationships

    def generate_column(self, column: DbColumn, table: DbTable) -> str:
        """
        Generate SQLAlchemy column definition.

        Args:
            column: Column definition
            table: Parent table\n

        Returns:
            String with the column definition
        """
        indent = " " * 4

        # Handle many-to-many relationships
        if 'm2m_class' in column.attributes:
            m2m_class = column.attributes['m2m_class']
            self.relationships.add_many2many_relation(m2m_class, table, column)
            return ""

        # Get column type information
        t = column.db_type
        py_type = t.python_type.__name__
        sa_type = t.name

        # Use the most specific type in inheritance chain
        if t.inheritance:
            sa_type = t.inheritance[-1]

        # Add type to imports
        self.imports.column_imports.add(sa_type)

        # Add Python type import if needed
        self.imports.add_python_type_import(py_type)

        # Process type arguments
        type_args = [f"{a}={column.attr_type[a]}" for a in column.attr_type]
        if type_args:
            sa_type = f"{sa_type}({', '.join(type_args)})"

        # Process foreign key
        foreign = ""
        if 'foreign_key' in column.attributes:
            foreign = self._process_foreign_key(column, table)

        # Process column arguments (translate $-token system defaults)
        field_args = [
            f"{a}={self._render_field_value(a, column.attr_field[a])}"
            for a in column.attr_field
        ]
        args_str = ', '.join([""] + field_args) if field_args else ""

        # Construct final column definition
        return f"{indent}{column.name}: Mapped[{py_type}] = mapped_column({sa_type}{foreign}{args_str})\n"

    def _render_field_value(self, attr: str, value: Any) -> str:
        """
        Render a mapped_column() field-argument value.

        Values are emitted verbatim (e.g. `default=datetime.now`), except a
        `$`-token `default` — a system default like `$op_date` — which is
        translated into a reference to the registered core callable
        (coframe.defaults) and the needed import is added. This is the seam that
        makes op_date (and any app-registered system default) usable declaratively
        from model YAML.
        """
        if attr == 'default' and isinstance(value, str) and value.startswith('$'):
            from coframe import defaults as cf_defaults
            name = value[1:]
            if cf_defaults.get_default(name) is None:
                raise ValueError(
                    f"Unknown system default '{value}' on a column: not registered "
                    f"in coframe.defaults (known: {sorted(cf_defaults.default_names())})"
                )
            self.imports.standard_imports.add('import coframe.defaults')
            return f'coframe.defaults.{name}'
        return f'{value}'

    def _process_foreign_key(self, column: DbColumn, table: DbTable) -> str:
        """
        Process foreign key definition.

        Args:
            column: Column with foreign key
            table: Parent table

        Returns:
            Foreign key argument string
        """
        fk = column.attributes['foreign_key']
        fk_table = fk['table']
        fk_id = fk['id']

        # Explicit relationship names, when the generated ones would be poor or
        # ambiguous. Coframe hints, like `constraint` — never ForeignKey kwargs.
        relation = fk.get('relation')
        backref = fk.get('backref')

        # `constraint: false` → soft FK: keep the navigable relationship but emit
        # NO DB-level FK constraint, so the column may hold values with no matching
        # parent (unknown / late-bound codes, dirty imports). See PLUGIN_MODEL § 4.5.
        soft = fk.get('constraint') is False

        # Additional ForeignKey() arguments (ondelete, onupdate, …). `constraint`
        # is a Coframe hint, not a ForeignKey kwarg — never forward it.
        fk_args = []
        for a in fk:
            if a not in ['target', 'table', 'id', 'constraint', 'relation', 'backref']:
                fk_args.append(f"{a}={fk[a]}")

        fk_args_str = f", {', '.join(fk_args)}" if fk_args else ""

        if soft:
            foreign = ""   # no ForeignKey → no DB constraint, orphan values allowed
        else:
            foreign = f", ForeignKey('{fk_table.table_name}.{fk_id}'{fk_args_str})"
            self.imports.column_imports.add('ForeignKey')

        # Record the relationship; names and join spec are decided in resolve(),
        # once the whole model is known.
        self.relationships.add_foreign_key_relation(
            table, column.name, fk_table, fk_id, soft, relation, backref
        )
        self.imports.add_relationship_imports()

        return foreign


class Generator:
    """
    Main class for SQLAlchemy model source code generation.
    Coordinates the generation process and writes the final output.
    """

    def __init__(self, db: DB):
        """
        Initialize the generator.

        Args:
            db: Database schema manager
        """
        self.db = db
        self.imports = ModelImportManager(db)
        self.relationships = RelationshipManager()
        self.column_generator = ColumnGenerator(db, self.imports, self.relationships)
        self.class_finder = PluginClassFinder(db.pm)
        self.class_finder.scan_plugin_sources()

        self.mixins: Set[str] = set()  # Set of mixin classes
        self.tables: Dict[str, str] = {}  # Table name -> table class code
        self.source = ""  # Complete source code

    def generate(self, filename: str = "model.py") -> None:
        """
        Generate SQLAlchemy model code and write to file.

        Args:
            filename: Output filename
        """        """
        Process a single source file to find relevant classes.

        Args:
            source_file: Path to the source file
            plugin: Plugin object that owns the source file
        """

        self._process_tables()
        self._generate_source()

        with open(filename, 'w') as f:
            f.write(self.source)

    def _process_tables(self) -> None:
        """Process all tables and generate their class definitions."""
        # Process each table definition
        for name in self.db.tables_list:
            self._process_table(name)

        # Names and join specs can only be decided now that every FK is known
        self.relationships.resolve()

        # Add relationships to table definitions
        self._add_relationships_to_tables()

    def _process_table(self, name: str) -> None:
        """
        Process a single table and generate its class definition.

        Args:
            name: Table name
        """
        table = self.db.tables[name]

        # Collect mixins
        mixins = table.attributes.get('mixins', [])
        for mixin in mixins:
            self.mixins.add(mixin)

        # Find plugin-defined classes with the same name
        plugin_classes = self.class_finder.get_class_inheritance(name)

        # Combine base classes (Base, mixins, plugin classes)
        base_classes = ["Base"]
        base_classes.extend(mixins)
        base_classes.extend(plugin_classes)

        # Add imports for plugin classes
        for plugin_class in plugin_classes:
            module_path = plugin_class.rsplit('.', 1)[0]
            self.imports.standard_imports.add(f"import {module_path}")

        # Generate class declaration
        class_declaration = f"class {name}({', '.join(base_classes)}):\n"

        # Generate tablename declaration with declared_attr for dynamic resolution
        tablename_code = (
            "    @declared_attr\n"
            "    def __tablename__(cls):\n"
            f"        return resolve_table_name('{name}', '{table.table_name}')\n"
        )
        code = [class_declaration, tablename_code]

        # Generate many to many columns
        m2m = table.attributes.get('many_to_many', None)
        if m2m:
            def _m2m_column(target: Dict[str, Any]):
                t = target['db_type']
                py_type = t.python_type.__name__
                sa_type = t.name
                if t.inheritance:
                    sa_type = t.inheritance[-1]
                code.append(
                    f"    {target['column']}: Mapped[{py_type}] = mapped_column({sa_type}, "
                    f"ForeignKey('{target['table'].table_name}.{target['id']}'), primary_key=True)\n"
                )
            try:
                _m2m_column(m2m['target1'])
                _m2m_column(m2m['target2'])

            except (ValueError, KeyError) as e:
                print(f"Error processing many-to-many relationship in {name}: {e}")

        # Generate columns
        for column in table.columns:
            column_code = self.column_generator.generate_column(column, table)
            if column_code:
                code.append(column_code)

        # Generate compound indexes (__table_args__)
        indexes_code = self._generate_table_indexes(table)
        if indexes_code:
            code.append("\n")
            code.append(indexes_code)

        # Generate many to many relationships
        if m2m:
            def _m2m_retations(target1: Dict[str, Any], target2: Dict[str, Any]):
                code.append(
                    f"    {target1['table'].name.lower()}: Mapped['{target1['table'].name}'] = "
                    f"relationship(back_populates='{target2['table'].name.lower()}_m2m')\n"
                )

            def _m2m_back_relations(target1: Dict[str, Any], target2: Dict[str, Any]):
                relation = self.relationships.add_relation_key(target1['table'].name, self.relationships.back_relations)
                relation.append(
                    f"    {target2['table'].name.lower()}_m2m: Mapped[List['{name}']]"
                    f" = relationship(back_populates='{target1['table'].name.lower()}')\n"
                )
                relation.append(
                    f"    {target2['table'].table_name}: Mapped[List['{target2['table'].name}']] = "
                    f"relationship(secondary='{table.table_name}', "
                    f"back_populates='{target1['table'].table_name}', viewonly=True)\n"
                )
            try:
                code.append("\n")
                _m2m_retations(m2m['target1'], m2m['target2'])
                _m2m_retations(m2m['target2'], m2m['target1'])
                _m2m_back_relations(m2m['target1'], m2m['target2'])
                _m2m_back_relations(m2m['target2'], m2m['target1'])

            except (ValueError, KeyError) as e:
                print(f"Error processing many-to-many relationship in {name}: {e}")

        self.tables[name] = ''.join(code)

    def _generate_table_indexes(self, table: DbTable) -> str:
        """
        Generate __table_args__ for compound indexes.

        Args:
            table: Table definition

        Returns:
            String with __table_args__ definition, or empty string if no indexes
        """
        indexes = table.attributes.get('indexes', [])
        if not indexes:
            return ""

        # Add Index to imports
        self.imports.column_imports.add('Index')

        index_lines = []
        for i, idx in enumerate(indexes):
            # Build column list
            columns = idx.get('columns', [])
            if not columns:
                continue

            name = idx.get('name', f"idx_{'_'.join(columns)}")
            cols_str = ', '.join(f"'{col}'" for col in columns)

            # Add unique flag if specified
            unique = idx.get('unique', False)
            unique_str = ', unique=True' if unique else ''

            # Add comma (except for last element)
            comma = ',' if i < len(indexes) - 1 else ''

            # Optional description as comment
            description = idx.get('description', '')
            comment = f"  # {description}" if description else ''

            index_lines.append(f"        Index('{name}', {cols_str}{unique_str}){comma}{comment}")

        if not index_lines:
            return ""

        # Generate __table_args__ (no join with comma since we added commas manually)
        args = '\n'.join(index_lines)
        return f"    __table_args__ = (\n{args}\n    )\n"

    def _add_relationships_to_tables(self) -> None:
        """Add relationship definitions to table classes."""
        # Add direct relationships
        for table_name, relations in self.relationships.direct_relations.items():
            if relations:
                self.tables[table_name] += '\n' + ''.join(relations)

        # Add back references
        for table_name, relations in self.relationships.back_relations.items():
            if relations:
                self.tables[table_name] += '\n' + ''.join(relations)

    def _generate_mixin_classes(self) -> str:
        """
        Generate mixin class definitions.

        Returns:
            String with all mixin class definitions
        """
        code = []

        # sorted(): self.mixins is a set — iterating it raw makes the *textual*
        # order of these (independent) class blocks depend on PYTHONHASHSEED.
        # No functional impact (column order follows the MRO / bases list, not the
        # print order), but stable output is nicer to read/diff. Matches how
        # imports are already emitted (sorted()).
        for mixin in sorted(self.mixins):
            if mixin in self.db.types:
                type_def = self.db.types[mixin]

                # Inherit from plugin Python class if defined (carries methods/protocol)
                plugin_classes = self.class_finder.get_class_inheritance(mixin)
                if plugin_classes:
                    for cls in plugin_classes:
                        module_path = cls.rsplit('.', 1)[0]
                        self.imports.standard_imports.add(f"import {module_path}")
                    bases = ', '.join(plugin_classes)
                    mixin_code = [f"class {mixin}({bases}):\n"]
                else:
                    mixin_code = [f"class {mixin}:\n"]

                # Only generate real columns (virtual ones live as hybrid_property in Python)
                has_columns = False
                for column in type_def.columns:
                    column_code = self.column_generator.generate_column(column, type_def)
                    if column_code:
                        mixin_code.append(column_code)
                        has_columns = True

                if not has_columns:
                    mixin_code.append("    pass\n")

                code.append(''.join(mixin_code))

        return '\n\n'.join(code) if code else ""

    def _generate_source(self) -> None:
        """Generate the complete source code."""
        # Build body parts FIRST: mixin generation registers the imports of its
        # base classes (e.g. `import common.model` for Archivable), so it must run
        # before import statements are rendered — otherwise those imports are lost.
        helper_code = self._generate_helper_functions()
        mixin_code = self._generate_mixin_classes()

        source_parts = []

        # Add file header warning
        source_parts.append(self._generate_file_header())
        source_parts.append("")

        # Add imports (now complete — all body generators have run)
        source_parts.append(self.imports.generate_import_statements())
        source_parts.append("")
        source_parts.append("")

        # Add helper functions if any (currently empty - resolve_table_name moved to utils)
        if helper_code:
            source_parts.append(helper_code)
            source_parts.append("")
            source_parts.append("")

        # Add mixins
        if mixin_code:
            source_parts.append(mixin_code)
            source_parts.append("")

        # Add table classes
        for name in self.db.tables_list:
            source_parts.append(self.tables[name])
            source_parts.append("")

        # Add additional source code
        additional_code = self.db.pm.config.get('source_add', '')
        if additional_code:
            source_parts.append(additional_code)

        self.source = '\n'.join(source_parts)

    def _generate_file_header(self) -> str:
        """
        Generate file header warning about automatic generation.

        Returns:
            String with warning header
        """
        return '''# ============================================================================
# GENERATED FILE - DO NOT EDIT MANUALLY
# ============================================================================
# This file is automatically generated by Coframe source generator.
# Any manual changes will be lost when the file is regenerated.
#
# To make changes:
#   1. Edit the plugin YAML files (plugins/**/model.yaml)
#   2. Regenerate this file (python devtest.py or coframe generate)
#
# Generator: coframe.source.Generator
# ============================================================================'''

    def _generate_helper_functions(self) -> str:
        """
        Generate helper functions for model code.

        Returns:
            String with helper functions (empty now - resolve_table_name moved to coframe.utils)
        """
        return ""
