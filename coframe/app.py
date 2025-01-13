import sys
import inspect
from pathlib import Path
import yaml
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.types


class App:

    def __init__(self):
        """
        The `App` class is a singleton available for all `Table` classes
        defined in the "coframe" framework.

        Tables and column types are declared in YAML files within a plugin
        system. The `load_config` and `load_plugins` methods load everything
        needed.

        A source generator writes a Python script with all the models for
        SQLAlchemy.

        Then, using SQLAlchemy, all tables are augmented with attributes
        declared in the plugins. A typical app starts with these commands:

        ```python
        import coframe
        app = coframe.app.Base.__app__
        app.load_config("config.yaml")
        app.load_plugins()
        ```
        """
        self.config = {}
        """dict: global configuration"""
        self.plugins = {}
        """dict: all defined plugins"""
        self.sorted = []
        """list: plugins sorted by dependency to avoid forward declarations."""
        self.types = {}
        """dict: columns types"""
        self.tables = {}
        """dict: the defined tables"""
        self.tables_list = []
        """list: tables ordered by definition"""

    def load_config(self, config: str = "config.yaml"):
        """
        This method is called once at application startup, and it loads global
        configuration from a YAML file.

        Args:
            config (str, optional): The file with configuration. Defaults to "config.yaml".
        """
        defaults = {
            "name": "myapp",
            "version": '',
            "description": "",
            "author": "",
            "license": "",
            "plugins": ['plugins'],
            "db_engine": "",
            "admin_user": "",
            "admin_passwd": "",
            "session_time": 60 * 60 * 2,
            }
        with open(config) as f:
            data = yaml.safe_load(f)
        self.config = defaults.copy()
        self.config.update(data)

    def load_plugins(self):
        """
        Gets all plugin directories from the configuration. For each folder, it
        reads subfolders and checks if they are plugins. Since plugin directories
        are added to the Python path, all plugins must have a unique name. All
        plugins are added to a dictionary of plugins.

        After the plugins are read, the system calculates the types, the tables,
        and resolves the columns. If something is wrong, an exception is thrown.
        """
        self.plugins = {}
        for plugins_dir in self.config['plugins']:
            plugins_dir = Path(plugins_dir)
            if not plugins_dir.exists():
                raise ValueError(f"The plugins folder: {plugins_dir} does not exist")
            # The plugins directory is added to the Python path, so all plugins can be imported.
            sys.path.append(str(Path.cwd() / plugins_dir.name))
            for plugin_dir in plugins_dir.iterdir():
                if plugin_dir.is_dir():
                    if not (plugin_dir / "config.yaml").exists():
                        # this directory does not contain a plugin
                        continue
                    plugin = Plugin(plugin_dir)
                    if plugin.name in self.plugins:
                        raise ValueError(f"Duplicate plugin name: {plugin.name}")
                    self.plugins[plugin.name] = plugin

        self._sort_dependencies()
        self._calc_types()
        self._calc_tables()
        self._calc_columns()

    def _sort_dependencies(self):
        """
        Support method: sorts plugins by dependencies using Kahn's algorithm.
        """

        # Build the graph of dependencies
        dependencies = {}
        for name, value in self.plugins.items():
            deps = value.config.get('depends_on', [])
            if isinstance(deps, str):
                deps = [deps]
            dependencies[name] = set(deps)

        # check dependencies
        all_items = set(dependencies.keys())
        for item, deps in dependencies.items():
            unknown_deps = deps - all_items
            if unknown_deps:
                raise ValueError(f"Not found dependence for {item}: {unknown_deps}")

        # Sort using Kahn algorithm
        result = []
        no_deps = [k for k, v in dependencies.items() if not v]
        while no_deps:
            current = no_deps.pop(0)
            result.append(current)
            for item, deps in dependencies.items():
                if current in deps:
                    deps.remove(current)
                    if not deps:
                        no_deps.append(item)

        # check circular dependencies
        if len(result) != len(dependencies):
            remaining = set(dependencies.keys()) - set(result)
            raise ValueError(f"Circular dependence found between: {remaining}")

        self.sorted = result

    def _calc_types(self):
        """
        Support method: calculates all types from plugins and `sqlalchemy.types`.
        Resolves type inheritance. The types are stored in a dictionary with
        the name as key and the `Type` object as value.
        """

        # Find all standard types in `sqlalchemy.types`.
        self.types = {}
        type_classes = [obj for name, obj in inspect.getmembers(sqlalchemy.types) if isinstance(obj, type)]
        for t in type_classes:
            try:
                py_type = t().python_type
                self.types[t.__name__] = Type(t.__name__, "", python_type=py_type)
            except Exception:
                # when the type does not have a python_type
                continue

        # find all types in plugins
        for name in self.sorted:
            plugin = self.plugins[name]
            for data in plugin.data:
                types = data.get('types', {})
                for name, value in types.items():
                    if name in self.types:
                        raise ValueError(f"Type already defined: {name}")
                    self.types[name] = Type(name, plugin, attributes=value)

        # resolve type inheritance
        for name in self.types:
            self.types[name].resolve(self.types)

    def _calc_tables(self):
        """
        Support method: calculates all tables from plugins. The tables are
        stored in a dictionary with the name as key and the `Table` object as
        value. Tables are also stored in a list ordered by definition to
        avoid forward references.
        """
        self.tables = {}
        self.tables_list = []

        # find all tables in plugins
        for name in self.sorted:
            plugin = self.plugins[name]
            for data in plugin.data:
                tables = data.get('tables', {})
                for name, value in tables.items():
                    if name in self.tables:
                        self.tables[name].update(value, plugin)
                    else:
                        table = Table(name, plugin, value)
                        self.tables[name] = table
                        self.tables_list.append(name)

    def _calc_columns(self):
        """
        Support method: after calculating types and tables, the columns of
        composite types and tables are calculated to link the correct type and
        resolve foreign keys.
        """
        # resolve the field type of composite types
        for name in self.types:
            if 'columns' in self.types[name].attributes:
                for column in self.types[name].attributes['columns']:
                    self._resolve_column(column, f"type: {name}")
        # resolve the field type of columns
        for name in self.tables:
            for column in self.tables[name].columns:
                self._resolve_column(column, f"table: {name}")

    def _resolve_column(self, column: dict, caller: str):
        """Support method: Resolve the type of fields for the columns of tables
        and composite types.

        Args:
            column (dict): the column to resolve
            caller (str): description of caller in case of errors
        """
        if column['type'] not in self.types:
            # TODO we have to handle many2many fields.
            try:
                table, id = column['type'].split('.')
                foreign = self.tables[table]
                column['foreign_key'] = foreign
                column['foreign_id'] = id
                # TODO search id field in foreign table
            except Exception:
                raise ValueError(f"Column: {column['name']} in {caller} has wrong type")
            return
        attr = self.types[column['type']].attributes
        for key, value in attr.items():
            if key not in column:
                column[key] = value
        column['field_type'] = self.types[column['type']]


class Plugin:

    def __init__(self, plugin_dir: Path):
        """
        A plugin is a folder that contains a `config.yaml` file plus other
        YAML files, Python files, and other types of files if needed.

        Args:
            plugin_dir (Path): the directory of the plugin.
        """
        defaults = {
            "name": plugin_dir.name,
            "version": '0.0.1',
            "description": "",
            "author": "",
            "license": "",
            "depends_on": [],
            }
        with open(plugin_dir / "config.yaml") as f:
            data = yaml.safe_load(f)
        self.config = defaults.copy()
        self.config.update(data['config'])
        self.name = self.config['name']
        self.plugin_dir = plugin_dir

        # Load data from YAML files and get a list of Python files.
        self.data = []
        self.sources = []
        self.files = []
        for file in plugin_dir.iterdir():
            if file.is_file():
                if file.suffix.lower() == '.py':
                    self.sources.append(file.stem)
                elif file.suffix.lower() == '.yaml' and file.stem != 'config':
                    with open(file) as f:
                        self.data.append(yaml.safe_load(f))
                else:
                    self.files.append(file)


class Type:

    def __init__(self, name: str, plugin: Plugin, attributes: dict = {}, python_type: object = None):
        """
        A `Type` is a class that defines a column type. It can be a standard
        SQLAlchemy type or a custom type defined in a plugin. Types can inherit
        from other types. In this case, the attributes of the inherited type
        are copied to the inheriting type. Attributes are defaulted in table
        fields.

        Args:
            name (str): the name of the type.
            plugin (Plugin): the plugin that defines the type.
            attributes (dict, optional): attributes of the type. Defaults to {}.
            python_type (obj, optional): the final Python type. Defaults to None.
        """
        self.name = name
        self.plugin = plugin
        self.python_type = python_type
        self.attributes = attributes
        self.inheritance = []
        self.columns = attributes.get('columns', [])

    def resolve(self, types: dict):
        """
        Resolves type inheritance recursively. If a type inherits from another
        type, then the attributes of the inherited type are copied to the
        inheriting type. Inheritance is resolved recursively until the type
        no longer has any inheritance.

        Args:
            types (dict): a dictionary of all types.
        """
        type = self
        while 'inherits' in type.attributes:
            if type.name not in types:
                raise ValueError(f"Type \"{type.name}\" declared in \"{type.plugin.name}\" is not found")
            type = types[type.attributes['inherits']]
            self.inheritance.append(type.name)
            attr = type.attributes.copy()
            attr.update(self.attributes)
            self.attributes = attr
        self.python_type = type.python_type


class Table:

    def __init__(self, name: str, plugin: Plugin, attributes: dict):
        """
        A table is the representation of an SQLAlchemy table. It can be used
        to generate the source code of SQLAlchemy models and can be accessed
        at runtime to obtain additional information.

        Args:
            name (str): Name of the table class.
            plugin (Plugin): the first plugin that defines the table.
            attributes (dict): attributes of the table.
        """
        self.name = name
        """str: name of the table class."""
        self.table_name = attributes.get('name', name.lower())
        """str: name of the table in the DB."""
        self.plugins = []
        """list: list of plugins that defined the table."""
        self.attributes = {}
        """dict: attributes are additional information for the table used to
        generate the model source code and that can be used by clients and
        servers."""
        self.columns = []
        """list: list of columns in order of definition. Usually the first
        column is the ID and the second is the main description field."""

        self.update(attributes, plugin)

    def update(self, attributes: dict, plugin: Plugin):
        """
        Updates the list of columns with the ones coming from the plugin.
        Attributes are merged with the existing ones.

        Args:
            attributes (dict): attributes of the table.
            plugin (Plugin): the plugin where they were found.
        """
        for column in attributes['columns']:
            for d in self.columns:
                if column['name'] == d['name']:
                    raise ValueError(f"Column \"{column['name']}\" in \"{plugin.name}\" "
                                     f"plugin already defined in \"{d['plugin'].name}\" plugin")
            column['plugin'] = plugin
            self.columns.append(column)
        # Merge table attributes.
        attr = attributes.copy()
        attr.update(self.attributes)
        self.attributes = attr
        # Add this plugin to the list.
        self.plugins.append(plugin)
        # Clean up columns already imported.
        self.attributes.pop('columns', None)


class BaseApp:
    """Each table in the system knows all about Coframe tables, types, etc."""
    __app__ = App()


Base = declarative_base(cls=BaseApp)
