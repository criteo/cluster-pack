"""Environment detection and optimization settings for cluster-pack."""

import logging
import os
import shutil
from enum import Enum
from typing import NamedTuple, Union

_logger = logging.getLogger(__name__)


class LayoutOptimizationParams(NamedTuple):
    """Parameters derived from a LayoutOptimization mode."""
    pex_layout: str  # "packed" or "loose"
    use_zipfile: bool  # True to use zipfile module, False for shutil
    compress_level: int  # 0 = store only, 1-9 = compression level


class LayoutOptimization(Enum):
    """Layout optimization modes for large pex creation.

    SLOW_FAST_SMALL:   slow build, fastest 1st execution, smallest archive
    FAST_MID_BIG:      fastest build, medium 1st execution, biggest archive
    MID_SLOW_SMALL:    medium build, slowest 1st execution, small archive
    DISABLED (legacy): slowest build, fast 1st execution, small archive

    benchmark results to build a pex with torch==2.8.0+cu128:
    --------------------------------------------------
    |     Mode        | Creation| 1st Exec |  Size   |
    |-----------------|---------|----------|---------|
    | SLOW_FAST_SMALL | 348.27s |  30.60s  | 3737 MB |
    | FAST_MID_BIG    | 27.65s  |  40.36s  | 6386 MB |
    | MID_SLOW_SMALL  | 167.17s |  54.77s  | 3878 MB |
    | DISABLED        | 442.79s |  34.10s  | 3726 MB |
    """
    SLOW_FAST_SMALL = "SLOW_FAST_SMALL"
    FAST_MID_BIG = "FAST_MID_BIG"
    MID_SLOW_SMALL = "MID_SLOW_SMALL"
    DISABLED = "DISABLED"

    @classmethod
    def from_string(cls, value: str) -> "LayoutOptimization":
        """Parse a string value into a LayoutOptimization enum member."""
        try:
            return cls(value.upper())
        except ValueError:
            valid = [m.value for m in cls]
            raise ValueError(f"Invalid mode '{value}'. Valid values: {valid}")

    def get_params(self) -> LayoutOptimizationParams:
        """Return the implementation parameters for this optimization mode."""
        if self == LayoutOptimization.SLOW_FAST_SMALL:
            return LayoutOptimizationParams(pex_layout="packed", use_zipfile=True, compress_level=0)
        elif self == LayoutOptimization.FAST_MID_BIG:
            return LayoutOptimizationParams(pex_layout="loose", use_zipfile=True, compress_level=0)
        elif self == LayoutOptimization.MID_SLOW_SMALL:
            return LayoutOptimizationParams(pex_layout="loose", use_zipfile=True, compress_level=1)
        else:  # DISABLED
            return LayoutOptimizationParams(pex_layout="packed", use_zipfile=False, compress_level=0)


def _detect_uv() -> bool:
    """Detect if uv is installed and available in PATH."""
    if shutil.which("uv") is not None:
        return True
    else:
        _logger.warning("Cluster-pack can now interact with uv to speed up uploads, it is recommended to install it.")
        return False


# Global settings with defaults
UV_AVAILABLE: bool = _detect_uv()
LAYOUT_OPTIMIZATION: LayoutOptimization = LayoutOptimization.SLOW_FAST_SMALL
VENV_OPTIMIZATION_LEVEL: int = 1
PYPI_INDEX_URL: Union[str, None] = None


def set_layout_optimization(mode: Union[str, LayoutOptimization]) -> None:
    """Set the layout optimization mode for large pex creation.

    :param mode: A LayoutOptimization enum member or string
    :raises ValueError: if mode is an invalid string
    """
    global LAYOUT_OPTIMIZATION
    if isinstance(mode, str):
        LAYOUT_OPTIMIZATION = LayoutOptimization.from_string(mode)
    else:
        LAYOUT_OPTIMIZATION = mode


def set_venv_optimization_level(level: int) -> None:
    """Set the venv optimization level for pex builds.

    :param level: optimization level (0=disabled, 1=default optimizations, 2=aggressive)
    """
    global VENV_OPTIMIZATION_LEVEL
    VENV_OPTIMIZATION_LEVEL = level


def get_layout_optimization() -> LayoutOptimization:
    """Get the current layout optimization mode."""
    return LAYOUT_OPTIMIZATION


def get_venv_optimization_level() -> int:
    """Get the current venv optimization level."""
    return VENV_OPTIMIZATION_LEVEL


def is_uv_available() -> bool:
    """Check if uv is available."""
    return UV_AVAILABLE


# Criteo-specific settings
CRITEO_PYPI_URL = "https://filer-pypi-integration.crto.in/repository/criteo.moab.pypi-read/simple"


def _is_criteo() -> bool:
    """Check if running in Criteo environment."""
    return "CRITEO_ENV" in os.environ


def set_pypi_index(pypi_index: str) -> None:
    global PYPI_INDEX_URL
    if pypi_index:
        PYPI_INDEX_URL = pypi_index
    elif _is_criteo():
        PYPI_INDEX_URL = CRITEO_PYPI_URL
    else:
        PYPI_INDEX_URL = None


def get_pypi_index() -> str:
    return PYPI_INDEX_URL


# Initialize from environment variables
set_layout_optimization(os.environ.get("C_PACK_LAYOUT_OPTIMIZATION", "SLOW_FAST_SMALL"))
set_venv_optimization_level(int(os.environ.get("C_PACK_VENV_OPTIMIZATION_LEVEL", "1")))
set_pypi_index(os.environ.get("C_PACK_PYPI_URL", None))
