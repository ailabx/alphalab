from . import performance
from . import plotting
from . import tears
from . import utils

try:
    from ._version import version as __version__
    from ._version import version_tuple
except ImportError:
    __version__ = "unknown version"
    version_tuple = (0, 0, "unknown version")


__all__ = ["performance", "plotting", "tears", "utils"]
