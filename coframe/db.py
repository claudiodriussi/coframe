import inspect
import sqlalchemy.types
from sqlalchemy.ext.declarative import declarative_base
from coframe.plugins import PluginsManager, Plugin


class DB:
    """
    Database schema manager that handles types, tables, and columns defined in plugins.

    This class is responsible for:
    - Loading and validating all database types (both SQLAlchemy built-ins and custom types)
    - Managing table definitions and their relationships
    - Ensuring data consistency across the schema
    - Resolving type inheritance and foreign key relationships
    """

    def __init__(self):
        """
        Initialize the database schema manager.
        - self.pm: Plugin manager instance
        - self.types: Dictionary of all available types (built-in and custom)
        - self.tables: Dictionary of all defined tables
        - self.tables_list: Ordered list of table names
        """
        self.pm = None
        self.types = {}
        self.tables = {}
        self.tables_list = []

    def calc_db(self, plugins: PluginsManager):
        """
        Build the complete database schema from plugin definitions.

        Process flow:
        1. Load all type definitions
        2. Create table structures
        3. Process and validate column definitions

        Args:
            plugins (PluginsManager): Instance containing all loaded plugins
        """
        self.pm = plugins
        self._calc_types()
        self._calc_tables()
        self._calc_columns()

    def _calc_types(self):
        """
        Process and validate all type definitions. This includes:
        1. Loading built-in SQLAlchemy types
        2. Processing custom types from plugins
        3. Resolving type inheritance hierarchies

        Raises:
            ValueError: If a type is redefined across plugins
        """
        # Load built-in SQLAlchemy types
        self.types = {}
        type_classes = [obj for name, obj in inspect.getmembers(sqlalchemy.types) if isinstance(obj, type)]

        for t in type_classes:
            try:
                py_type = t().python_type
                self.types[t.__name__] = Type(t.__name__, "", python_type=py_type)
            except Exception:
                # Skip types that don't have a python_type equivalent
                continue

        # Process plugin-defined types
        for name in self.pm.sorted:
            plugin = self.pm.plugins[name]
            for data in plugin.data:
                types = data.get('types', {})
                for type_name, value in types.items():
                    if type_name in self.types:
                        raise ValueError(f"Type already defined: {type_name}")
                    self.types[type_name] = Type(type_name, plugin, attributes=value)

        # Resolve inheritance relationships
        for type_name in self.types:
            self.types[type_name].resolve(self.types)

    def _calc_tables(self):
        """
        Process all table definitions from plugins.

        Handles:
        - Table creation and updates
        - Merging table definitions from multiple plugins
        - Maintaining table order
        """
        self.tables = {}
        self.tables_list = []

        for name in self.pm.sorted:
            plugin = self.pm.plugins[name]
            for data in plugin.data:
                tables = data.get('tables', {})
                for table_name, value in tables.items():
                    if table_name in self.tables:
                        self.tables[table_name].update(value, plugin)
                    else:
                        table = Table(table_name, plugin, value)
                        self.tables[table_name] = table
                        self.tables_list.append(table_name)

    def _calc_columns(self):
        """
        Process and validate all column definitions.

        Handles:
        1. Resolving column types for composite types
        2. Processing table columns and their relationships
        3. Validating foreign key references
        4. Checking for column name duplicates

        Raises:
            ValueError: If duplicate column names are found or invalid foreign keys are referenced
        """
        # Process composite type columns
        for type_name in self.types:
            for column in self.types[type_name].col_def:
                col = Field(column, self)
                col.resolve(f"type: {type_name}")
                self.types[type_name].columns.append(col)

        # Process table columns
        for table_name in self.tables:
            for column in self.tables[table_name].col_def:
                col = Field(column, self)
                col.resolve(f"table: {table_name}")

                # Handle composite types
                if col.type and col.type.columns:
                    prefix = column.get('prefix', "")
                    for type_column in col.type.columns:
                        composed_col = Field(type_column.attributes, self)
                        composed_col.name = prefix + composed_col.name
                        composed_col.resolve(f"table: {table_name}")
                        self.tables[table_name].columns.append(composed_col)
                else:
                    self.tables[table_name].columns.append(col)

                # Check for duplicate column names
                for i, c1 in enumerate(self.tables[table_name].columns):
                    for c2 in self.tables[table_name].columns[i+1:]:
                        if c1.name == c2.name:
                            raise ValueError(f'Duplicated column "{c1.name}" in table "{table_name}"')

        # resolve foreign keys
        for table_name in self.tables:
            for col in self.tables[table_name].columns:
                col.resolve_foreign(f"table: {table_name}")


class Type:
    """
    Represents a database column type, either built-in SQLAlchemy type or custom.

    Features:
    - Support for type inheritance
    - Custom attributes for extended type information
    - Automatic attribute inheritance from parent types
    """

    def __init__(self, name: str, plugin: Plugin, attributes: dict = None, python_type: object = None):
        """
        Initialize a new type definition.

        Args:
            name (str): Type name
            plugin (Plugin): Plugin that defines this type
            attributes (dict, optional): Type attributes and configuration
            python_type (object, optional): Corresponding Python type
        """
        self.name = name
        self.plugin = plugin
        self.python_type = python_type
        self.attributes = attributes or {}
        self.inheritance = []
        self.col_def = attributes.get('columns', []) if attributes else []
        self.columns = []

    def resolve(self, types: dict):
        """
        Resolve type inheritance chain and merge attributes.

        Walks up the inheritance chain and merges attributes from parent types
        into the current type's attributes.

        Args:
            types (dict): Dictionary of all available types

        Raises:
            ValueError: If an inherited type is not found
        """
        type_obj = self
        while 'inherits' in type_obj.attributes:
            if type_obj.name not in types:
                raise ValueError(f'Type "{type_obj.name}" declared in "{type_obj.plugin.name}" is not found')
            type_obj = types[type_obj.attributes['inherits']]
            self.inheritance.append(type_obj.name)

            # Merge attributes, giving preference to current type's attributes
            attr = type_obj.attributes.copy()
            attr.update(self.attributes)
            self.attributes = attr

        self.python_type = type_obj.python_type


class Table:
    """
    Represents a database table with support for:
    - Multi-plugin table definitions
    - Column management
    - Table attributes and metadata
    """

    def __init__(self, name: str, plugin: Plugin, attributes: dict):
        """
        Initialize a new table definition.

        Args:
            name (str): Table class name
            plugin (Plugin): Plugin that initially defines this table
            attributes (dict): Table configuration and columns
        """
        self.name = name
        self.table_name = attributes.get('name', name.lower())
        self.plugins = []
        self.attributes = {}
        self.col_def = []
        self.columns = []

        self.update(attributes, plugin)

    def update(self, attributes: dict, plugin: Plugin):
        """
        Update table definition with additional attributes from a plugin.

        Args:
            attributes (dict): New attributes to merge
            plugin (Plugin): Plugin providing the updates

        Raises:
            ValueError: If a column is redefined across plugins
        """
        # Process columns
        for column in attributes['columns']:
            for existing_col in self.col_def:
                if column['name'] == existing_col['name']:
                    raise ValueError(
                        f'Column "{column["name"]}" in "{plugin.name}" plugin '
                        f'already defined in "{existing_col["plugin"].name}" plugin'
                    )
            column['plugin'] = plugin
            self.col_def.append(column)

        # Merge attributes
        attr = attributes.copy()
        attr.update(self.attributes)
        self.attributes = attr

        self.plugins.append(plugin)
        # Remove processed columns from attributes
        self.attributes.pop('columns', None)


class Field:
    """
    Represents a table or type column field with support for:
    - Type resolution
    - Foreign key relationships
    - Field attributes
    """

    def __init__(self, attributes: dict, db: DB):
        """
        Initialize a new field.

        Args:
            attributes (dict): Field configuration
            db (DB): Database schema manager instance
        """
        self.db = db
        self.attributes = attributes
        self.name = attributes['name']
        self.type = None
        self.attr_field = {}
        self.attr_type = {}
        self.attr_relation = {}
        self.attr_other = {}

    def resolve(self, caller: str):
        """
        Resolve field type and relationships.

        Handles:
        - Basic type resolution
        - Foreign key relationships
        - Many-to-many relationships
        - Attribute inheritance from type definitions

        Args:
            caller (str): Context information for error messages

        Raises:
            ValueError: If type resolution fails or references are invalid
        """
        cur_type = self.attributes['type']

        # Inherit attributes from type definition
        if cur_type in self.db.types:
            attr = self.db.types[cur_type].attributes
            for key, value in attr.items():
                if key not in self.attributes:
                    self.attributes[key] = value
            self.type = self.db.types[cur_type]

        # split attributes types
        field_keys = ['primary_key', 'autoincrement', 'unique', 'nullable', 'index', 'default']
        type_keys = ['length', 'precision', 'scale', 'timezone']
        relation_keys = ['onupdate', 'ondelete']
        for key, value in self.attributes.items():
            if key in field_keys:
                self.attr_field[key] = value
            elif key in type_keys:
                self.attr_type[key] = value
            elif key in relation_keys:
                self.attr_relation[key] = value
            else:
                self.attr_other[key] = value

        # Handle foreign key references
        if cur_type not in self.db.types:
            try:
                table, id = self.attributes['type'].split('.')
                foreign = self.db.tables[table]
                self.attributes['foreign_key'] = foreign
                self.attributes['foreign_id'] = id

                # many2many intermediate table
                if self.name.startswith('m2m.'):
                    self.attributes['m2m_class'] = self.name.split('.')[1]

            except Exception:
                raise ValueError(f"Field: {self.name} in {caller} has invalid type")
            return

    def resolve_foreign(self, caller: str):
        """
        Resolve foreign relationships. It is done after all columns resolution
        to avoid forward resolutions in case of m2m relationships

        Args:
            caller (str): Context information for error messages

        Raises:
            ValueError: If type resolution fails or references are invalid
        """
        foreign = self.attributes.get('foreign_key', None)
        if not foreign:
            return
        id = self.attributes['foreign_id']

        # Find the referenced column type
        for col in foreign.columns:
            if id == col.name:
                self.type = col.type
        if not self.type:
            raise ValueError(f"Field: {self.name} has invalid foreign reference")


class BaseApp:
    """
    Base class for all SQLAlchemy models.
    Provides access to the database schema information.
    """
    __app__ = DB()


Base = declarative_base(cls=BaseApp)
