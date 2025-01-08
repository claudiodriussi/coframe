import importlib
from pathlib import Path


def autoimport(file, package):
    """Automatically import all modules in the same directory of the package."""
    package_dir = Path(file).resolve().parent

    for file in package_dir.glob("*.py"):
        if file.name == "__init__.py":
            continue
        module_name = file.stem
        module = importlib.import_module(f".{module_name}", package=package)
        globals()[module_name] = module


autoimport(__file__, __package__)
