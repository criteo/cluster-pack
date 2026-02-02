from cluster_pack.settings import (
    LayoutOptimization,
    LayoutOptimizationParams,
    set_layout_optimization,
    get_layout_optimization,
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
