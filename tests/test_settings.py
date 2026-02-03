import contextlib
import os
from unittest import mock

from cluster_pack import settings, packaging
from cluster_pack.settings import (
    LayoutOptimization,
    LayoutOptimizationParams,
    set_layout_optimization,
    get_layout_optimization,
    CRITEO_PYPI_URL,
    get_pypi_index,
    set_pypi_index
)


def test_get_current_user_with_empty_env_variable():
    """Test that empty C_PACK_USER falls back to getpass.getuser()."""
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch("getpass.getuser", return_value="system_user")
        )

        settings.set_current_user(None)
        assert settings._get_current_user() == "system_user"

        settings.set_current_user("")
        assert settings._get_current_user() == "system_user"

        settings.set_current_user("    ")
        assert settings._get_current_user() == "system_user"


def test_get_current_user_strips_whitespace():
    """Test that C_PACK_USER whitespace is stripped."""
    settings.set_current_user("  spaced_user  ")
    assert settings._get_current_user() == "spaced_user"


def test_build_package_path_uses_c_pack_user_env():
    """Test that _build_package_path uses C_PACK_USER when set."""
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch("cluster_pack.packaging.get_default_fs", return_value="hdfs://")
        )
        settings.set_current_user("env_user")
        result = packaging._build_package_path("myenv", "pex")
        expected = "hdfs:///user/env_user/envs/myenv.pex"

        assert result == expected


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
