import sys
import os
from pathlib import Path
import yaml
from typing import Dict, List
import coframe


class PluginsManager:
    """
    Manages the loading, dependency resolution, and merging of plugins.

    This class handles:
    - Plugin discovery and loading from filesystem
    - Plugin dependency resolution
    - Configuration management
    - Data merging from multiple plugins
    - History tracking of all plugin operations
    """

    def __init__(self, logger_name='coframe'):
        """
        Initialize the plugin manager.

        Args:
            logger_name (str): Name for the logger instance

        Attributes:
            history (dict): Tracks the history of all key definitions
            data (dict): Merged data from all plugins
            config (dict): Global configuration
            plugins (dict): Dictionary of loaded Plugin instances
            sorted (list): Plugins sorted by dependencies
        """
        self.history = {}
        self.data = {}
        self.config = {}
        self.plugins = {}
        self.sorted = []

        # Initialize logging
        self.logger = coframe.get_logger(logger_name)
        coframe.set_formatter(self.logger, '%(name)s|%(levelname)s|%(message)s')

    def load_config(self, config: str = "config.yaml"):
        """
        Load global configuration from YAML file.

        Args:
            config (str): Path to configuration file

        The configuration includes application metadata and settings:
        - Application name, version, description
        - Author and license information
        - Plugin directories
        - Database configuration
        - Admin credentials
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
        }
        with open(config) as f:
            data = yaml.safe_load(f)
        self.config = defaults.copy()
        self.config.update(data.get('config', {}))

    def load_plugins(self):
        """
        Load and initialize all plugins from configured directories.

        Process:
        1. Load configuration if not already loaded
        2. Discover plugins in configured directories
        3. Sort plugins by dependencies
        4. Merge data from all plugins

        Raises:
            ValueError: If plugin directory doesn't exist or duplicate plugin names found
        """
        if not self.config:
            self.load_config()

        # Discover plugins
        self.plugins = {}
        for plugins_dir in self.config['plugins']:
            plugins_dir = Path(plugins_dir)
            if not plugins_dir.exists():
                raise ValueError(f"The plugins folder: {plugins_dir} does not exist")

            # Add plugin directory to Python path for imports
            sys.path.append(str(Path.cwd() / str(plugins_dir)))

            # Scan for plugin directories
            for plugin_dir in plugins_dir.iterdir():
                if plugin_dir.is_dir():
                    config_file = plugin_dir / "config.yaml"
                    if not config_file.exists():
                        continue  # Not a plugin directory

                    plugin = Plugin(plugin_dir)
                    if plugin.name in self.plugins:
                        raise ValueError(f"Duplicate plugin name: {plugin.name}")
                    self.plugins[plugin.name] = plugin

        # Process plugins in dependency order
        self._sort_dependencies()

        # Merge plugin data
        for name in self.sorted:
            plugin = self.plugins[name]
            for data in plugin.data:
                self.merge_dicts(data, name)

    def _sort_dependencies(self):
        """
        Sort plugins based on their dependencies using Kahn's topological sort algorithm.

        Process:
        1. Build dependency graph
        2. Validate all dependencies exist
        3. Sort using Kahn's algorithm
        4. Check for circular dependencies

        Raises:
            ValueError: If dependencies are missing or circular dependencies detected
        """
        # Create dependency graph
        dependencies = {}
        for name, value in self.plugins.items():
            deps = value.config.get('depends_on', [])
            if isinstance(deps, str):
                deps = [deps]
            dependencies[name] = set(deps)

        # Validate dependencies
        all_items = set(dependencies.keys())
        for item, deps in dependencies.items():
            unknown_deps = deps - all_items
            if unknown_deps:
                raise ValueError(f"Not found dependence for {item}: {unknown_deps}")

        # Perform topological sort
        result = []
        no_deps = [k for k, v in dependencies.items() if not v]

        while no_deps:
            current = no_deps.pop(0)
            result.append(current)

            # Remove current node from all dependencies
            for item, deps in dependencies.items():
                if current in deps:
                    deps.remove(current)
                    if not deps:
                        no_deps.append(item)

        # Check for circular dependencies
        if len(result) != len(dependencies):
            remaining = set(dependencies.keys()) - set(result)
            raise ValueError(f"Circular dependence found between: {remaining}")

        self.sorted = result

    def merge_dicts(self, d: Dict, plugin: str) -> Dict:
        """
        Entry point for merging a new dictionary into existing data.

        Args:
            d (Dict): New dictionary to merge
            plugin (str): Name of source plugin

        Returns:
            Dict: Merged dictionary
        """
        self.data = self._recursive_merge(self.data, d, plugin)
        return self.data

    def _recursive_merge(self, base: Dict, new: Dict, plugin: str, current_path: List[str] = None) -> Dict:
        """
        Recursively merge dictionaries while preserving plugin information.

        Features:
        - Deep merging of nested dictionaries
        - List concatenation with duplicate removal
        - Plugin attribution tracking
        - Type compatibility checking
        - History logging

        Args:
            base (Dict): Base dictionary to merge into
            new (Dict): New dictionary to merge from
            plugin (str): Source plugin namemodel.py.txt
            Dict: Merged dictionary

        Raises:
            TypeError: If incompatible types are found during merge
        """
        if current_path is None:
            current_path = []

        result = {}

        # Copy base dictionarymodel.py.txt
        for key in base:
            result[key] = base[key]

        # Process new dictionary
        for key in new:
            key_path = self._build_key_path(current_path, key)
            self._add_to_history(key_path, plugin)

            if key in base:
                v1, v2 = base[key], new[key]

                # Validate type compatibility
                if type(v1) is not type(v2):
                    raise TypeError(
                        f"Incompatible types for key '{key_path}' between existing and {plugin}: "
                        f"{type(v1).__name__} vs {type(v2).__name__}"
                    )

                # Handle different value types
                if isinstance(v1, dict):
                    self.logger.debug(f"[{plugin}] Merging dict at key '{key_path}'")
                    result[key] = self._recursive_merge(v1, v2, plugin, current_path + [key])
                    # Preserve original plugin attribution
                    if '_plugin' in v1:
                        result[key]['_plugin'] = v1['_plugin']
                    else:
                        result[key]['_plugin'] = plugin

                elif isinstance(v1, list):
                    self.logger.debug(f"[{plugin}] Extending list at key '{key_path}'")
                    result[key] = v1 + [item for item in v2 if item not in v1]
                    # Handle plugin attribution for dictionary items in lists
                    for item in result[key]:
                        if isinstance(item, dict):
                            if '_plugin' not in item:
                                if item in v2:
                                    item['_plugin'] = plugin
                                else:
                                    item['_plugin'] = v1[0].get('_plugin', plugin)
                else:
                    self.logger.warning(f"[{plugin}] Overlapping value for key '{key_path}': {v1} -> {v2}")
                    result[key] = v2
            else:
                # Handle new keys
                self.logger.info(f"[{plugin}] Adding new key '{key_path}'")
                if isinstance(new[key], dict):
                    result[key] = self._recursive_merge({}, new[key], plugin, current_path + [key])
                    result[key]['_plugin'] = plugin
                elif isinstance(new[key], list):
                    result[key] = new[key]
                    # Add plugin attribution to dictionary items
                    for item in result[key]:
                        if isinstance(item, dict) and '_plugin' not in item:
                            item['_plugin'] = plugin
                else:
                    result[key] = new[key]

        return result

    def _add_to_history(self, key_path: str, plugin: str):
        """
        Track the history of key definitions by plugin.

        Args:
            key_path (str): Complete path to the key
            plugin (str): Plugin defining the key
        """
        if key_path not in self.history:
            self.history[key_path] = []
        self.history[key_path].append(plugin)

    def _build_key_path(self, current_path: List[str], key: str) -> str:
        """
        Build the complete path for a key in the dictionary hierarchy.

        Args:
            current_path (List[str]): Current position in hierarchy
            key (str): Current key name

        Returns:
            str: Complete dot-notation path to key
        """
        if current_path:
            return f"{'.'.join(current_path)}.{key}"
        return key

    def print_history(self):
        """
        Display the complete history of key definitions across all plugins.Path
        """
        format_str = coframe.set_formatter(self.logger, '%(name)s|%(message)s')
        self.logger.info("Definition History:")
        for key_path, plugins in sorted(self.history.items()):
            self.logger.info(f"{key_path}: defined in {sorted(plugins)}")
        coframe.set_formatter(self.logger, format_str)

    def export_pythonpath(self, windows: bool = os.name == 'nt') -> str:
        """
        Prepare a string for environment settings for linux/mac or windows

        Args:
            windows (bool): True for windows settings, (default export the string for the os in use)

        Returns:
            str: the string for environment script
        """
        env = ""
        for plugins_dir in self.config['plugins']:
            plugins_dir = Path(plugins_dir)
            if windows:
                s = f'set PYTHONPATH="{str(Path.cwd() / str(plugins_dir))}";%PYTHONPATH%\n'
                env += s.replace("/", "\\")
            else:
                s = f'export PYTHONPATH="{str(Path.cwd() / str(plugins_dir))}:$PYTHONPATH"\n'
                env += s.replace("\\", "/")
        return env


class Plugin:
    """
    Represents a single plugin with its configuration and associated files.

    A plugin consists of:
    - Configuration file (config.yaml)
    - Data files (additional YAML files)
    - Python source files
    - Other resource files
    """

    def __init__(self, plugin_dir: Path):
        """
        Initialize a plugin from its directory.

        Args:
            plugin_dir (Path): Path to plugin directory

        Attributes:
            config (dict): Plugin configuration
            name (str): Plugin name
            plugin_dir (Path): Plugin directory path
            data (list): Data loaded from YAML files
            sources (list): Python source files
            data_files (list): YAML data files
            files (list): Other resource files
        """
        # Load plugin configuration with defaults
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

        # Initialize file lists
        self.data = []
        self.sources = []
        self.data_files = []
        self.files = []

        # Categorize and load files
        for file in plugin_dir.iterdir():
            if file.is_file():
                if file.suffix.lower() == '.py':
                    self.sources.append(file)
                elif file.suffix.lower() == '.yaml' and file.stem != 'config':
                    with open(file) as f:
                        self.data.append(yaml.safe_load(f))
                    self.data_files.append(file)
                else:
                    self.files.append(file)
