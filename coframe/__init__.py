
__version__ = "0.5.0"

# Import only autoimport for internal use
# Other utilities available via coframe.utils
from .utils import autoimport

# Auto-import internal modules
autoimport(__file__, __package__)

# Public API
__all__ = ['autoimport']
