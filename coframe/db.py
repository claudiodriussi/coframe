import inspect
import threading
from typing import Dict, List, Any, Optional, Union, Iterator
from types import ModuleType
from contextlib import contextmanager
import sqlalchemy.types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from coframe.plugins import PluginsManager, Plugin
from coframe.endpoints import CommandProcessor
import coframe


class DB:
    """
    Database schema manager that handles types, tables, and columns defined in plugins.

    This class is responsible for:
    - Loading and validating all database types (both SQLAlchemy built-ins and custom types)
    - Managing table definitions and their relationships
    - Ensuring data consistency across the schema
    - Resolving type inheritance and foreign key relationships
    """

    def __init__(self) -> None:
        """
        Initialize the database schema manager.
        - self.pm: Plugin manager instance
        - self.cp: Command processor instance
        - self.types: Dictionary of all available types (built-in and custom)
        - self.tables: Dictionary of all defined tables
        - self.tables_list: Ordered list of table names
        - self.model: Module containing all models
        - self.models: Dictionary of all db models
        - self.engine: The instanced db engine
        - self.db_type: Database type: "sqlite", "postgresql", "mysql" and so on
        """
        self.pm: Optional[PluginsManager] = None
        self.cp: Optional[CommandProcessor] = None
        self.types: Dict[str, DbType] = {}
        self.tables: Dict[str, DbTable] = {}
        self.tables_list: List[str] = []
        self.model: Any = None
        self.models: Dict[str, Any] = {}
        self.engine: Any = None
        self.db_type: str = "unknown"

    def calc_db(self, plugins: PluginsManager) -> None:
        """
        Build the complete database schema from plugin definitions.

        Process flow:
        1. Load all type definitions
        2. Create table structures
        3. Process and validate column definitions
        4. register all endpoints from plugins and from package

        Args:
            plugins: Instance containing all loaded plugins
        """
        self.pm = plugins
        self._calc_types()
        self._calc_tables()
        self._calc_columns()
        self._calc_endpoints()

    def _calc_types(self) -> None:
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
                self.types[t.__name__] = DbType(t.__name__, "", python_type=py_type)
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
                    self.types[type_name] = DbType(type_name, plugin, attributes=value)

        # Resolve inheritance relationships
        for type_name in self.types:
            self.types[type_name].resolve(self.types)

    def _calc_tables(self) -> None:
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
                        table = DbTable(table_name, plugin, value)
                        self.tables[table_name] = table
                        self.tables_list.append(table_name)

    def _calc_columns(self) -> None:
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
            for column in self.types[type_name].attributes.get('columns', []):
                col = DbColumn(column, self)
                col.resolve(f"type: {type_name}")
                self.types[type_name].columns.append(col)

        # Process table columns
        for table_name in self.tables:
            for column in self.tables[table_name]._columns:
                col = DbColumn(column, self)
                col.resolve(f"table: {table_name}")

                # Handle composite types
                if col.db_type and col.db_type.columns:
                    prefix = column.get('prefix', "")
                    for type_column in col.db_type.columns:
                        composed_col = DbColumn(type_column.attributes, self)
                        composed_col.name = prefix + composed_col.name
                        composed_col.resolve(f"table: {table_name}")
                        self.tables[table_name].columns.append(composed_col)
                else:
                    self.tables[table_name].columns.append(col)

                # Check for duplicate column names after composite types integration
                for i, c1 in enumerate(self.tables[table_name].columns):
                    for c2 in self.tables[table_name].columns[i+1:]:
                        if c1.name == c2.name:
                            raise ValueError(f'Duplicated column "{c1.name}" in table "{table_name}"')

            # No need for _column variable anymore
            delattr(self.tables[table_name], '_columns')

        # Resolve foreign keys and many to many relationships
        for table_name in self.tables:
            for col in self.tables[table_name].columns:
                col.resolve_foreign(f"table: {table_name}")
            self.tables[table_name].resolve_m2m(self)

    def _calc_endpoints(self) -> None:
        """
        Resolve all endpoints coming from plugins and the ones defined in package
        """
        self.cp = CommandProcessor()
        sources = self.pm.get_sources()
        self.cp.resolve_endpoints(sources)
        self.cp.resolve_endpoints('endpoint_db.py')

    def find_model_class(self, table_name: str) -> Any:
        """
        Find the model class from the name of class,

        Args:
            table_name: the name to search

        Returns:
            The model class or None if not found
        """
        return self.models.get(table_name, None)

    def initialize_db(self, db_url: str, model: ModuleType) -> Any:
        """
        Initialize the database with the given connection URL, register the
        model module and build the models dictionary

        Args:
            db_url: Database connection URL for SQLAlchemy
            model: Module containing all models

        Returns:
            The created engine instance
        """
        self.model = model
        self.models = {name: cls for name, cls in vars(self.model).items()
                       if isinstance(cls, type) and not name.startswith('_')}
        from sqlalchemy import create_engine
        engine = create_engine(db_url)
        Base.metadata.create_all(engine)
        self.engine = engine
        self.db_type = self.get_database_type()
        return engine

    def get_database_type(self) -> str:
        """
        Get the type of database being used.

        Returns:
            String representing database type ('sqlite', 'postgresql', 'mysql', etc.)
        """
        if not self.engine:
            return None

        connection_url = str(self.engine.url)

        if 'sqlite' in connection_url:
            return 'sqlite'
        elif 'postgresql' in connection_url or 'postgres' in connection_url:
            return 'postgresql'
        elif 'mysql' in connection_url:
            return 'mysql'
        elif 'mariadb' in connection_url:
            return 'mariadb'
        # add further db types if needed
        else:
            return 'unknown'

    @contextmanager
    def get_session(self, context: Dict[str, Any] = None) -> Iterator[Session]:
        """
        Context manager that provides a session for database operations.

        The session is automatically closed when the context is exited.
        If an exception occurs, the session is rolled back before being closed.

        Args:
            context: Optional context manager to use for this session

        Yields:
            SQLAlchemy session object
        """

        # save the current context and set a new one
        old_context = None
        if context is not None:
            old_context = BaseApp.get_context()
            BaseApp.set_context(context)

        session_factory = sessionmaker(bind=self.engine)
        Session = scoped_session(session_factory)
        session = Session()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
            # restore previous context
            if context is not None and old_context is not None:
                BaseApp.set_context(old_context)


class DbType:
    """
    Represents a database column type, either built-in SQLAlchemy type or custom.

    Features:
    - Support for type inheritance
    - Custom attributes for extended type information
    - Automatic attribute inheritance from parent types
    """

    def __init__(self, name: str, plugin: Union[Plugin, str], attributes: Optional[Dict[str, Any]] = None,
                 python_type: Optional[object] = None) -> None:
        """
        Initialize a new type definition.

        Args:
            name: Type name
            plugin: Plugin that defines this type
            attributes: Type attributes and configuration
            python_type: Corresponding Python type
        """
        self.name: str = name
        self.plugin: Union[Plugin, str] = plugin
        self.python_type: Optional[object] = python_type
        self.attributes: Dict[str, Any] = attributes or {}
        self.inheritance: List[str] = []
        self.columns: List['DbColumn'] = []

    def resolve(self, types: Dict[str, 'DbType']) -> None:
        """
        Resolve type inheritance chain and merge attributes.

        Walks up the inheritance chain and merges attributes from parent types
        into the current type's attributes.

        Args:
            types: Dictionary of all available types

        Raises:
            ValueError: If an inherited type is not found
        """
        type_obj = self
        while 'base' in type_obj.attributes:
            if type_obj.name not in types:
                raise ValueError(f'Type "{type_obj.name}" declared in "{type_obj.plugin.name}" is not found')
            type_obj = types[type_obj.attributes['base']]
            self.inheritance.append(type_obj.name)
            coframe.deep_merge(self.attributes, type_obj.attributes)

        self.python_type = type_obj.python_type


class DbTable:
    """
    Represents a database table with support for:
    - Multi-plugin table definitions
    - Column management
    - Table attributes and metadata
    """

    def __init__(self, name: str, plugin: Plugin, attributes: Dict[str, Any]) -> None:
        """
        Initialize a new table definition.

        Args:
            name: Table class name
            plugin: Plugin that initially defines this table
            attributes: Table configuration and columns
        """
        self.name: str = name
        self.table_name: str = attributes.get('name', name.lower())
        self.plugins: List[Plugin] = []
        self.attributes: Dict[str, Any] = {}
        self._columns: List[Dict[str, Any]] = []  # Temporary variable used to build columns
        self.columns: List[DbColumn] = []

        self.update(attributes, plugin)

    def update(self, attributes: Dict[str, Any], plugin: Plugin) -> None:
        """
        Update table definition with additional attributes from a plugin.

        Args:
            attributes: New attributes to merge
            plugin: Plugin providing the updates
        """
        # Process columns
        for column in attributes['columns']:
            column['plugin'] = plugin
            self._columns.append(column)

        # Merge attributes
        coframe.deep_merge(self.attributes, attributes)

        self.plugins.append(plugin)
        # Remove processed columns from attributes
        self.attributes.pop('columns', None)

    def resolve_m2m(self, db: DB) -> None:
        """
        Resolve many-to-many relationship information.

        This method processes target tables in a many-to-many relationship,
        finding the referenced columns and their types.

        Args:
            db: Database schema manager instance

        Raises:
            ValueError: If the many-to-many relationship has invalid configuration
        """
        m2m = self.attributes.get('many_to_many', None)
        if not m2m:
            return

        def _resolve_target(target: Dict[str, Any]) -> None:
            table, id = target['table'].split('.')
            foreign = db.tables[table]
            target['table'] = foreign
            target['id'] = id

            # Find the referenced column type
            for col in foreign.columns:
                if id == col.name:
                    target['db_type'] = col.db_type
                    break
            if not target['db_type']:
                raise ValueError(f"Many to Many Column error in table: {self.name}")

        try:
            _resolve_target(m2m['target1'])
            _resolve_target(m2m['target2'])
        except Exception:
            raise ValueError(f"Many to Many error for table: {self.name}")


class DbColumn:
    """
    Represents a table or type column with support for:
    - Type resolution
    - Foreign key relationships
    - Column attributes
    """

    def __init__(self, attributes: Dict[str, Any], db: DB) -> None:
        """
        Initialize a new column.

        Args:
            attributes: Column configuration
            db: Database schema manager instance
        """
        self.db: DB = db
        self.attributes: Dict[str, Any] = attributes
        self.name: str = attributes['name']
        self.db_type: Optional[DbType] = None
        self.attr_field: Dict[str, Any] = {}
        self.attr_type: Dict[str, Any] = {}
        self.attr_other: Dict[str, Any] = {}

    def resolve(self, caller: str) -> None:
        """
        Resolve column type and relationships.

        Handles:
        - Basic type resolution
        - Foreign key relationships
        - Many-to-many relationships
        - Attribute inheritance from type definitions

        Args:
            caller: Context information for error messages

        Raises:
            ValueError: If type resolution fails or references are invalid
        """
        if 'foreign_key' in self.attributes:
            try:
                fk = self.attributes['foreign_key']
                table, id = fk['target'].split('.')
                foreign = self.db.tables[table]
                fk['table'] = foreign
                fk['id'] = id
            except Exception:
                raise ValueError(f"Foreign key for column: {self.name} in {caller} has invalid type")
            return

        cur_type = self.attributes['type']

        # Inherit attributes from type definition
        if cur_type in self.db.types:
            attr = self.db.types[cur_type].attributes
            for key, value in attr.items():
                if key not in self.attributes:
                    self.attributes[key] = value
            self.db_type = self.db.types[cur_type]

        # Split attributes by type
        field_keys = ['primary_key', 'autoincrement', 'unique', 'nullable', 'index', 'default']
        type_keys = ['length', 'precision', 'scale', 'timezone']
        for key, value in self.attributes.items():
            if key in field_keys:
                self.attr_field[key] = value
            elif key in type_keys:
                self.attr_type[key] = value
            else:
                self.attr_other[key] = value

    def resolve_foreign(self, caller: str) -> None:
        """
        Resolve foreign relationships. This is done after all columns resolution
        to avoid forward resolutions in case of many-to-many relationships.

        Args:
            caller: Context information for error messages

        Raises:
            ValueError: If type resolution fails or references are invalid
        """
        fk = self.attributes.get('foreign_key', None)
        if not fk:
            return
        foreign = fk['table']
        id = fk['id']

        # Find the referenced column type
        for col in foreign.columns:
            if id == col.name:
                self.db_type = col.db_type
                break
        if not self.db_type:
            raise ValueError(f"Column: {self.name} has invalid foreign reference")


class BaseApp:
    """
    Base class for all SQLAlchemy models.
    Provides access to the database schema information.
    """
    __coframe_app__: DB = DB()

    @classmethod
    def get_context(cls):
        """
        Get current context from thread-local storage
        """
        if not hasattr(BaseApp, '_context'):
            BaseApp._context = threading.local()
        return getattr(BaseApp._context, 'value', None)

    @classmethod
    def set_context(cls, context):
        """
        Set current context in thread-local storage
        """
        if not hasattr(BaseApp, '_context'):
            BaseApp._context = threading.local()
        BaseApp._context.value = context


Base = declarative_base(cls=BaseApp)
