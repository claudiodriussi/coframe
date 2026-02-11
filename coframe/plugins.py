import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import yaml
from coframe.utils import get_logger, set_formatter, logging_to_file, deep_merge


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

    def __init__(self, logger_name: str = 'coframe') -> None:
        """
        Initialize the plugin manager.

        Args:
            logger_name: Name for the logger instance
        """
        self.history: Dict[str, List[str]] = {}
        self.data: Dict[str, Any] = {}
        self.config: Dict[str, Any] = {}
        self.plugins: Dict[str, 'Plugin'] = {}
        self.sorted: List[str] = []
        self.original_handlers = None
        self.merge_handlers: Dict[str, Any] = {}

        # Initialize logging
        self.logger = get_logger(logger_name)
        set_formatter(self.logger, '%(name)s|%(levelname)s|%(message)s')

    def register_merge_handler(self, pattern: str, handler: Any) -> None:
        """
        Register a custom merge handler for a specific data path pattern.

        Args:
            pattern: Dot-notation path pattern (supports wildcards like 'tables.*.columns')
            handler: Callable that takes (base_list, new_list, plugin) and returns merged list
        """
        self.merge_handlers[pattern] = handler

    def _get_merge_handler(self, key_path: str) -> Optional[Any]:
        """
        Find a merge handler for the given key path.

        Args:
            key_path: Complete dot-notation path

        Returns:
            Handler function if found, None otherwise
        """
        import fnmatch

        # Exact match first
        if key_path in self.merge_handlers:
            return self.merge_handlers[key_path]

        # Pattern match with wildcards
        for pattern, handler in self.merge_handlers.items():
            if fnmatch.fnmatch(key_path, pattern):
                return handler

        return None

    def load_config(self, config: Union[str, Path] = "config.yaml") -> None:
        """
        Load global configuration from YAML file.

        Args:
            config: Path to configuration file
        """
        self.config = {
            "name": "myapp",
            "version": '',
            "description": "",
            "author": "",
            "license": "",
            "plugins": ['plugins'],
            "db_engine": "",
            "log_file": "",
        }
        with open(config) as f:
            data = yaml.safe_load(f)
        deep_merge(self.config, data)

        # Redirect logging to file if specified in config
        if self.config['log_file']:
            self.original_handlers, _ = logging_to_file(self.logger, self.config['log_file'])

    def load_plugins(self) -> None:
        """
        Load and initialize all plugins from configured directories.

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

    def _sort_dependencies(self) -> None:
        """
        Sort plugins based on their dependencies using Kahn's topological sort algorithm.

        Raises:
            ValueError: If dependencies are missing or circular dependencies detected
        """
        # Create dependency graph
        dependencies: Dict[str, set] = {}
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

    def merge_dicts(self, d: Dict[str, Any], plugin: str) -> Dict[str, Any]:
        """
        Entry point for merging a new dictionary into existing data.

        Args:
            d: New dictionary to merge
            plugin: Name of source plugin

        Returns:
            Merged dictionary
        """
        self.data = self._recursive_merge(self.data, d, plugin)
        return self.data

    def _recursive_merge(self,
                         base: Dict[str, Any],
                         new: Dict[str, Any],
                         plugin: str,
                         current_path: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Recursively merge dictionaries while preserving plugin information.

        Args:
            base: Base dictionary to merge into
            new: New dictionary to merge from
            plugin: Source plugin name
            current_path: Current path in the dictionary hierarchy

        Returns:
            Merged dictionary

        Raises:
            TypeError: If incompatible types are found during merge
        """
        if current_path is None:
            current_path = []

        result: Dict[str, Any] = {}

        # Copy base dictionary
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

                if isinstance(v1, dict):
                    self.logger.debug(f"[{plugin}] Merging dict at key '{key_path}'")
                    result[key] = self._recursive_merge(v1, v2, plugin, current_path + [key])
                    if '_plugin' in v1:
                        result[key]['_plugin'] = v1['_plugin']
                    else:
                        result[key]['_plugin'] = plugin

                elif isinstance(v1, list):
                    # Check if there's a custom merge handler for this path
                    handler = self._get_merge_handler(key_path)
                    if handler:
                        self.logger.debug(f"[{plugin}] Merging list at key '{key_path}' using custom handler")
                        result[key] = handler(v1, v2, plugin)
                    else:
                        self.logger.debug(f"[{plugin}] Extending list at key '{key_path}'")
                        result[key] = v1 + [item for item in v2 if item not in v1]
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
                self.logger.info(f"[{plugin}] Adding new key '{key_path}'")
                if isinstance(new[key], dict):
                    result[key] = self._recursive_merge({}, new[key], plugin, current_path + [key])
                    result[key]['_plugin'] = plugin
                elif isinstance(new[key], list):
                    result[key] = new[key]
                    for item in result[key]:
                        if isinstance(item, dict) and '_plugin' not in item:
                            item['_plugin'] = plugin
                else:
                    result[key] = new[key]

        return result

    def _add_to_history(self, key_path: str, plugin: str) -> None:
        """
        Track the history of key definitions by plugin.

        Args:
            key_path: Complete path to the key
            plugin: Plugin defining the key
        """
        if key_path not in self.history:
            self.history[key_path] = []
        self.history[key_path].append(plugin)

    def _build_key_path(self, current_path: List[str], key: str) -> str:
        """
        Build the complete path for a key in the dictionary hierarchy.

        Args:
            current_path: Current position in hierarchy
            key: Current key name

        Returns:
            Complete dot-notation path to key
        """
        if current_path:
            return f"{'.'.join(current_path)}.{key}"
        return key

    def print_history(self) -> None:
        """Display the complete history of key definitions across all plugins."""
        format_str = set_formatter(self.logger, '%(name)s|%(message)s')
        self.logger.info("Definition History:")
        for key_path, plugins in sorted(self.history.items()):
            self.logger.info(f"{key_path}: defined in {sorted(plugins)}")
        set_formatter(self.logger, format_str)

    def export_pythonpath(self, windows: bool = os.name == 'nt') -> str:
        """
        Prepare a string for environment settings for linux/mac or windows.

        Args:
            windows: True for windows settings, False for Unix/Linux
                    (defaults to current OS)

        Returns:
            The string for environment script
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

    def get_timestamp(self) -> float:
        """
        Get the most recent timestamp from all plugins.

        Returns:
            float: The timestamp of the most recent file across all plugins
        """
        latest_timestamp = 0
        for name, plugin in self.plugins.items():
            if plugin.timestamp > latest_timestamp:
                latest_timestamp = plugin.timestamp

        return latest_timestamp

    def get_formatted_timestamp(self) -> str:
        """
        Get the most recent timestamp from all plugins as a formatted date string.

        Returns:
            str: The formatted date of the most recent file
        """
        timestamp = self.get_timestamp()
        if timestamp > 0:
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        return "Unknown"

    def should_regenerate(self, filename: str) -> bool:
        """
        Determine if a file should be regenerated by comparing its timestamp
        with the most recent timestamp among all plugins.

        Args:
            filename: Path to the file to check

        Returns:
            bool: True if the file must be regenerated
        """
        # If file doesn't exist, it needs to be generated
        if not os.path.exists(filename):
            return True

        # Compare timestamps
        file_timestamp = os.path.getmtime(filename)
        plugins_timestamp = self.get_timestamp()
        return file_timestamp < plugins_timestamp

    def get_sources(self, to_str: bool = False) -> List[Path]:
        """
        Get a list of all Python source files from all plugins.

        Args:
            to_str: if needed transform the Path object to string

        Returns:
            List[Path]: List of all Python source files
        """
        sources = []
        for name, plugin in self.plugins.items():
            sources.extend(plugin.sources)
        if to_str:
            return [str(s) for s in sources]
        return sources


class Plugin:
    """
    Represents a single plugin with its configuration and associated files.

    A plugin consists of:
    - Configuration file (config.yaml)
    - Data files (additional YAML files)
    - Python source files
    - Other resource files
    """

    def __init__(self, plugin_dir: Path) -> None:
        """
        Initialize a plugin from its directory.

        Args:
            plugin_dir: Path to plugin directory
        """
        # Load plugin configuration with defaults
        self.config: Dict[str, Any] = {
            "name": plugin_dir.name,
            "version": '0.0.1',
            "description": "",
            "author": "",
            "license": "",
            "depends_on": [],
        }

        with open(plugin_dir / "config.yaml") as f:
            data = yaml.safe_load(f)
        deep_merge(self.config, data)

        self.name: str = self.config['name']
        self.plugin_dir: Path = plugin_dir

        # Initialize file lists and timestamp
        self.data: List[Dict[str, Any]] = []
        self.sources: List[Path] = []
        self.data_files: List[Path] = []
        self.files: List[Path] = []
        self.timestamp: float = 0  # Default timestamp

        # Categorize and load files
        for file in plugin_dir.iterdir():
            if file.is_file():
                # Update timestamp if this file is newer
                file_timestamp = os.path.getmtime(file)
                if file_timestamp > self.timestamp:
                    self.timestamp = file_timestamp

                # Categorize file by type
                if file.suffix.lower() == '.py':
                    self.sources.append(file)
                elif file.suffix.lower() == '.yaml' and file.stem != 'config':
                    with open(file) as f:
                        self.data.append(yaml.safe_load(f))
                    self.data_files.append(file)
                else:
                    self.files.append(file)
