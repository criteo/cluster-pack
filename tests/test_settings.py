import os
from unittest import mock

from cluster_pack.settings import (
    LayoutOptimization,
    LayoutOptimizationParams,
    set_layout_optimization,
    get_layout_optimization,
    CRITEO_PYPI_URL,
    get_pypi_index,
    set_pypi_index
)


class TestSetLayoutOptimization:
    def test_set_with_string(self):
        try:
            default = get_layout_optimization()
            assert default.get_params() == LayoutOptimizationParams(
                pex_layout="packed", use_zipfile=True, compress_level=0)

            set_layout_optimization(LayoutOptimization.MID_SLOW_SMALL)
            assert get_layout_optimization().get_params() == LayoutOptimizationParams(
                pex_layout="loose", use_zipfile=True, compress_level=1)

            set_layout_optimization("FAST_MID_BIG")
            assert get_layout_optimization().get_params() == LayoutOptimizationParams(
                pex_layout="loose", use_zipfile=True, compress_level=0)

            set_layout_optimization("DISABLED")
            assert get_layout_optimization().get_params() == LayoutOptimizationParams(
                pex_layout="packed", use_zipfile=False, compress_level=0)

        finally:
            set_layout_optimization(default)


class TestPypiIndex:
    def test_get_pypi_when_criteo(self):
        with mock.patch.dict(os.environ, {"CRITEO_ENV": "1"}):
            set_pypi_index(None)
            assert get_pypi_index() == CRITEO_PYPI_URL
            set_pypi_index("http://dummy/url")
            assert get_pypi_index() == "http://dummy/url"

    def test_is_criteo_when_env_not_set(self):
        with mock.patch.dict(os.environ, clear=True):
            set_pypi_index(None)
            assert get_pypi_index() is None
            set_pypi_index("http://dummy/url")
            assert get_pypi_index() == "http://dummy/url"
