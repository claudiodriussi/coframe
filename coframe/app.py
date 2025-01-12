import sys
import inspect
from pathlib import Path
import yaml
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.types


class App:

    def __init__(self):
        self.config = {}
        self.plugins = []
        self.sorted = []
        self.types = {}
        self.tables = {}
        self.tables_list = []

    def load_config(self, config: str = "config.yaml"):
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
        """Get all plugins dir from config. For each folder read all subfolders
        and check if they are plugins, since plugins dir are added to the python
        path, all plugins must have a unique name. All plugins are added to a
        dict of plugins.
        """
        self.plugins = {}
        for plugins_dir in self.config['plugins']:
            plugins_dir = Path(plugins_dir)
            if not plugins_dir.exists():
                raise ValueError(f"The plugins folder: {plugins_dir} does not exist")
            # the plugins dir is added to the python path, so all plugins can be imported
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

    def _sort_dependencies(self):
        """Sort plugins by dependencies using Kahn's algorithm"""

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
        """Calculate all types from plugins and sqlalchemy.types. Resolve
        inheritance of types. The types are stored in a dict with the name as
        key and the Type object as value.
        """

        # find all standard types in sqlalchemy.types
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
                    self.types[name] = Type(name, plugin.name, attributes=value)

        # resolve inheritance of types
        for name in self.types:
            self.types[name].resolve(self.types)

    def _calc_tables(self):
        """"""
        self.tables = {}
        self.tables_list = []

        # find all tables in plugins
        for name in self.sorted:
            plugin = self.plugins[name]
            for data in plugin.data:
                tables = data.get('tables', {})
                for name, value in tables.items():
                    if name in self.tables:
                        self.tables[name].update(value, self)
                    else:
                        table = Table(name, plugin.name, value, self)
                        self.tables[name] = table
                        self.tables_list.append(name)


class Plugin:

    def __init__(self, plugin_dir: Path):
        """A plugin is a folder that contains a config.yaml file plus other yaml
        files, python files and if needed other type of files.

        Args:
            plugin_dir (Path): the dir of the plugin
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
        # load data from yaml files and get list of python files
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

    def __init__(self, name: str, plugin: str, attributes: dict = {}, python_type: object = None):
        """a Type is a class that defines a type of data. It can be a standard
        type from sqlalchemy or a custom type defined in a plugin.
        Types can inherit from other types, in this case the attributes of the
        inherited type are copied to the inheriting type. Attributes are
        defaulted in fields of tables

        Args:
            name (str): the name of the type
            plugin (str): the name of the plugin that defines the type
            attributes (dict, optional): attributes of the type. Defaults to {}.
            python_type (obj, optional): the final python type. Defaults to None.
        """
        self.name = name
        self.plugin = plugin
        self.python_type = python_type
        self.attributes = attributes
        self.inheritance = []
        self.columns = attributes.get('columns', [])

    def resolve(self, types: dict):
        """Resolve inheritance of types recursively. If a type inherits from
        another type, then the attributes of the inherited type are copied to
        the inheriting type. The inheritance is resolved recursively until the
        type does not have any more inheritance.

        Args:
            types (dict): a dictionary of all types
        """
        type = self
        while 'inherits' in type.attributes:
            if type.name not in types:
                raise ValueError(f"Type {type.name} not found")
            type = types[type.attributes['inherits']]
            self.inheritance.append(type.name)
            attr = type.attributes.copy()
            attr.update(self.attributes)
            self.attributes = attr
        self.python_type = type.python_type


class Table:

    def __init__(self, name: str, plugin: str, attributes: dict, app: App):
        self.name = attributes.get('name', name.lower())
        self.plugin = plugin
        self.attributes = attributes
        self.columns = []
        self.update(attributes, app)

    def update(self, attributes: dict, app: App):
        for column in attributes['columns']:
            self.columns.append(column)
        attr = attributes.copy()
        attr.update(self.attributes)
        self.attributes = attr
        self.attributes.pop('columns', None)


class BaseApp:
    __app__ = App()


Base = declarative_base(cls=BaseApp)
