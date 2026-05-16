import sys
import os
import importlib
import importlib.util
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
                    if '$plugin' in v1:
                        result[key]['$plugin'] = v1['$plugin']
                    else:
                        result[key]['$plugin'] = plugin

                elif isinstance(v1, list):
                    # Check if there's a custom merge handler for this path
                    handler = self._get_merge_handler(key_path)
                    if handler:
                        self.logger.debug(f"[{plugin}] Merging list at key '{key_path}' using custom handler")
                        result[key] = handler(v1, v2, plugin)
                    else:
                        result[key] = self._merge_lists(v1, v2, plugin, key_path)
                else:
                    self.logger.warning(f"[{plugin}] Overlapping value for key '{key_path}': {v1} -> {v2}")
                    result[key] = v2
            else:
                self.logger.info(f"[{plugin}] Adding new key '{key_path}'")
                if isinstance(new[key], dict):
                    result[key] = self._recursive_merge({}, new[key], plugin, current_path + [key])
                    result[key]['$plugin'] = plugin
                elif isinstance(new[key], list):
                    result[key] = new[key]
                    for item in result[key]:
                        if isinstance(item, dict) and '$plugin' not in item:
                            item['$plugin'] = plugin
                else:
                    result[key] = new[key]

        return result

    # Identity keys checked in priority order to find how a list item is identified.
    _IDENTITY_KEYS = ('id', 'name', 'field', 'group')

    def _detect_identity_key(self, items: list) -> Optional[str]:
        """
        Return the identity key for a list of dicts, or None for scalar/unkeyed lists.

        Scans the first dict item for the first key in _IDENTITY_KEYS.
        Pure string lists and dicts without a recognised identity key are treated
        as plain sequences (append semantics, unchanged from previous behaviour).
        """
        for item in items:
            if isinstance(item, dict):
                for k in self._IDENTITY_KEYS:
                    if k in item:
                        return k
                return None  # dict items without a recognised identity key
        return None  # all items are scalars

    def _merge_lists(self, base: list, new: list, plugin: str, key_path: str) -> list:
        """
        Merge two lists using identity-aware semantics when possible, plain append otherwise.

        Identity-aware merge (triggered when list items are dicts with a known identity key):
          - same identity           → deep-merge the item's properties
          - $remove: true           → drop the item from the result
          - $after: <id> / $before: <id>  → insert at the given position
          - new identity            → append at the end

        The $remove / $after / $before directives are consumed here and never
        appear in the resolved descriptor sent to the frontend.

        Plain-append fallback (string lists, unkeyed dicts):
          result = base + [item for item in new if item not in base]
        """
        id_key = self._detect_identity_key(base) or self._detect_identity_key(new)

        if id_key is None:
            # Plain sequence: extend without duplicates (original behaviour)
            self.logger.debug(f"[{plugin}] Extending plain list at '{key_path}'")
            merged = base + [item for item in new if item not in base]
            return merged

        self.logger.debug(f"[{plugin}] Smart-merging list at '{key_path}' by '{id_key}'")

        # Build an ordered index of base items keyed by identity value.
        # Use a list of (identity, item) pairs to preserve insertion order.
        index: Dict[str, Any] = {}   # identity → item dict
        order: list = []             # identity values in base order

        for item in base:
            if isinstance(item, dict):
                identity = item.get(id_key)
                if identity is not None:
                    index[identity] = dict(item)
                    order.append(identity)
            else:
                # Scalar mixed into an otherwise-identified list — keep as-is
                order.append(item)

        # Deferred positional inserts: list of (anchor_id, position, item)
        deferred: list = []

        for item in new:
            if not isinstance(item, dict):
                if item not in order:
                    order.append(item)
                continue

            identity = item.get(id_key)
            remove   = item.get('$remove', False)
            after    = item.get('$after')
            before   = item.get('$before')

            # Strip merge directives from the item before storing
            clean = {k: v for k, v in item.items() if k not in ('$remove', '$after', '$before')}
            clean.setdefault('$plugin', plugin)

            if remove:
                if identity in index:
                    del index[identity]
                    order.remove(identity)
                continue

            if identity in index:
                # Deep-merge properties into existing item
                existing = index[identity]
                for k, v in clean.items():
                    if k == id_key:
                        continue
                    if isinstance(v, dict) and isinstance(existing.get(k), dict):
                        existing[k] = self._recursive_merge(existing[k], v, plugin, [])
                    else:
                        existing[k] = v
                existing['$plugin'] = plugin
            else:
                # New item
                index[identity] = clean
                if after or before:
                    deferred.append((after, before, identity))
                else:
                    order.append(identity)

        # Apply deferred positional inserts
        for (after, before, identity) in deferred:
            anchor = after or before
            if anchor in order:
                pos = order.index(anchor)
                order.insert(pos + 1 if after else pos, identity)
            else:
                self.logger.warning(
                    f"[{plugin}] Positional anchor '{anchor}' not found in '{key_path}', appending '{identity}'"
                )
                order.append(identity)

        # Rebuild the ordered list
        result = []
        for entry in order:
            if isinstance(entry, str) and entry in index:
                result.append(index[entry])
            elif not isinstance(entry, str):
                result.append(entry)  # scalar passthrough
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

    def get(self, path: str) -> Any:
        """
        Get a value from plugins.data by dotted path.

        Args:
            path: Dot-notation path, e.g. "views.book_list_view"

        Returns:
            The value at that path, or None if not found
        """
        current = self.data
        for part in path.split('.'):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None
        return current

    def resolve_refs(self, obj: Any, _seen: Optional[frozenset] = None,
                     plugin_context: Optional[str] = None) -> Any:
        """
        Recursively resolve all ``$ref`` fields in obj.

        A dict containing ``$ref: "section.key"`` is replaced by the object
        at that path in plugins.data.  Any sibling keys are merged on top
        of the resolved value (allowing local overrides).

        Relative refs (section.id) are qualified using the plugin_context
        derived from $plugin metadata on the containing dict.

        Args:
            obj: Object to resolve (dict, list, or scalar)
            _seen: Internal set of already-visited ref paths (cycle detection)
            plugin_context: Plugin namespace for relative $ref resolution

        Returns:
            Object with all refs resolved

        Raises:
            ValueError: If a circular ref is detected
        """
        if _seen is None:
            _seen = frozenset()

        if isinstance(obj, dict):
            current_plugin = obj.get('$plugin', plugin_context)

            if '$ref' in obj and isinstance(obj['$ref'], str):
                ref_path = obj['$ref']
                if ref_path in _seen:
                    raise ValueError(f"Circular ref detected: {ref_path}")
                target = self.get(ref_path)
                if target is None:
                    self.logger.warning(f"Unresolved ref: '{ref_path}'")
                    return obj
                target_plugin = target.get('$plugin', current_plugin) if isinstance(target, dict) else current_plugin
                resolved = self.resolve_refs(target, _seen | {ref_path}, target_plugin)
                # Merge sibling keys on top of the resolved object
                if isinstance(resolved, dict):
                    result = dict(resolved)
                    for k, v in obj.items():
                        if k != '$ref':
                            result[k] = self.resolve_refs(v, _seen | {ref_path}, current_plugin)
                    return result
                return resolved

            return {k: self.resolve_refs(v, _seen, current_plugin) for k, v in obj.items()}

        if isinstance(obj, list):
            return [self.resolve_refs(item, _seen, plugin_context) for item in obj]

        return obj

    def load_locale(self, locale: str) -> None:
        """
        Load translations for the given locale from the core library and all plugins.

        - Core: coframe.locale.{locale}  (standard importlib)
        - Plugins: {plugin_dir}/locale/{locale}.py  (spec_from_file_location, no __init__ needed)

        Call after load_plugins(). Safe to call with locale='en' (no-op).
        """
        if locale == 'en':
            return

        # Core library translations
        try:
            importlib.import_module(f'coframe.locale.{locale}')
        except ModuleNotFoundError:
            pass

        # Plugin translations in dependency order
        for name in self.sorted:
            plugin = self.plugins[name]
            locale_file = plugin.plugin_dir / 'locale' / f'{locale}.py'
            if not locale_file.exists():
                continue
            spec = importlib.util.spec_from_file_location(
                f'_coframe_locale_{name}_{locale}', locale_file
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)  # type: ignore[union-attr]
                self.logger.info(f'[i18n] Loaded {locale_file}')

    def load_all_locales(self) -> None:
        """
        Load every locale file found in the core library and all plugins.

        Scans coframe/locale/*.py and {plugin_dir}/locale/*.py for each plugin,
        then calls load_locale() for each unique locale found.
        Replaces load_locale(single_locale) when multi-language support is needed.
        """
        core_locale_dir = Path(__file__).parent / 'locale'
        dirs: List[Path] = [core_locale_dir]
        for name in self.sorted:
            dirs.append(self.plugins[name].plugin_dir / 'locale')

        loaded: set = set()
        for d in dirs:
            if not d.exists():
                continue
            for f in sorted(d.glob('*.py')):
                if f.stem != '__init__' and f.stem not in loaded:
                    self.load_locale(f.stem)
                    loaded.add(f.stem)

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
