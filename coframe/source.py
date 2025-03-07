from typing import Dict, List, Set, Any
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

        # Add Base import
        import_statements.append('from coframe.db import Base')

        return '\n'.join(import_statements)


class RelationshipManager:
    """
    Manages relationships between tables for model generation.
    """

    def __init__(self) -> None:
        """Initialize relationship manager."""
        self.direct_relations: Dict[str, List[str]] = {}  # Table name -> list of relation statements
        self.back_relations: Dict[str, List[str]] = {}  # Table name -> list of backref statements

    def add_foreign_key_relation(self, table: str, fk_table_name: str,
                                 column_name: str, fk_table: DbTable, py_type: str) -> None:
        """
        Add foreign key relationship between tables.

        Args:
            table: Current table
            fk_table_name: Foreign table name
            column_name: Current column name
            fk_table: Foreign table object
            py_type: Python type for the relationship
        """
        indent = " " * 4

        # Add forward relation
        relation = self.add_relation_key(table.name, self.direct_relations)
        relation.append(
            f"{indent}{fk_table.name.lower()}: Mapped[{py_type}] = "
            f"relationship('{fk_table.name}', back_populates='{table.table_name.lower()}')\n"
        )

        # Add back relation
        relation = self.add_relation_key(fk_table.name, self.back_relations)
        relation.append(
            f"{indent}{table.table_name.lower()}: Mapped[List['{table.name}']] = "
            f"relationship('{table.name}', back_populates='{fk_table.name.lower()}')\n"
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
            foreign = self._process_foreign_key(column, table, py_type)

        # Process column arguments
        field_args = [f"{a}={column.attr_field[a]}" for a in column.attr_field]
        args_str = ', '.join([""] + field_args) if field_args else ""

        # Construct final column definition
        return f"{indent}{column.name}: Mapped[{py_type}] = mapped_column({sa_type}{foreign}{args_str})\n"

    def _process_foreign_key(self, column: DbColumn, table: DbTable, py_type: str) -> str:
        """
        Process foreign key definition.

        Args:
            column: Column with foreign key
            table: Parent table
            py_type: Python type for the relationship

        Returns:
            Foreign key argument string
        """
        fk = column.attributes['foreign_key']
        fk_table = fk['table']
        fk_id = fk['id']

        # Process additional foreign key arguments
        fk_args = []
        for a in fk:
            if a not in ['target', 'table', 'id']:
                fk_args.append(f"{a}={fk[a]}")

        fk_args_str = f", {', '.join(fk_args)}" if fk_args else ""
        foreign = f", ForeignKey('{fk_table.table_name}.{fk_id}'{fk_args_str})"

        # Add relationship definitions
        self.relationships.add_foreign_key_relation(
            table, fk_table.name, column.name, fk_table, py_type
        )

        # Add needed imports
        self.imports.column_imports.add('ForeignKey')
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

        self.mixins: Set[str] = set()  # Set of mixin classes
        self.tables: Dict[str, str] = {}  # Table name -> table class code
        self.source = ""  # Complete source code

    def generate(self, filename: str = "model.py") -> None:
        """
        Generate SQLAlchemy model code and write to file.

        Args:
            filename: Output filename
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

        # Generate class declaration
        if mixins:
            class_declaration = f"class {name}(Base, {', '.join(mixins)}):\n"
        else:
            class_declaration = f"class {name}(Base):\n"

        # Generate tablename declaration
        code = [class_declaration, " " * 4 + f"__tablename__ = '{table.table_name}'\n"]

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
                    f"back_populates='{target1['table'].table_name}', viewonly=True)\n\n"
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

        for mixin in self.mixins:
            if mixin in self.db.types:
                type_def = self.db.types[mixin]
                mixin_code = [f"class {mixin}:\n"]

                for column in type_def.columns:
                    column_code = self.column_generator.generate_column(column, type_def)
                    if column_code:
                        mixin_code.append(column_code)

                code.append(''.join(mixin_code))

        return '\n\n'.join(code) if code else ""

    def _generate_source(self) -> None:
        """Generate the complete source code."""
        source_parts = []

        # Add imports
        source_parts.append(self.imports.generate_import_statements())
        source_parts.append("")
        source_parts.append("")

        # Add mixins
        mixin_code = self._generate_mixin_classes()
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
