#!/usr/bin/env python3
"""Benchmark pex creation and 7zip parameters for compression/decompression."""

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Any, Optional

PYTHON_VERSION= "3.11"
TORCH_VERSION = "torch==2.8.0"
BENCHMARK_DIR = "/tmp/pex_benchmark"

# 7z compression options to test
SEVENZIP_OPTIONS = [
    {"name": "store", "mx": 0, "desc": "No compression"},
    {"name": "fastest", "mx": 1, "desc": "Fastest compression"},

    # These settings are not interesting w.r.t. speed vs size tradeoff for our use cases
    # {"name": "fast", "mx": 3, "desc": "Fast compression"},
    # {"name": "normal", "mx": 5, "desc": "Normal compression"},
    # {"name": "maximum", "mx": 6, "desc": "Maximum (current default)"},
]

# Default 7z option for non-varied tests
DEFAULT_7Z_OPTION = {"name": "fastest", "mx": 1, "desc": "Fastest compression"}


def clear_cache(name: str = "pex"):
    """Clear PEX/pip caches."""
    pex_root = os.path.expanduser(f"~/.{name}")
    if os.path.exists(pex_root):
        shutil.rmtree(pex_root, ignore_errors=True)
    shutil.rmtree(os.path.expanduser(f"~/.cache/{name}"), ignore_errors=True)
    for item in Path("/tmp").glob(f"*.{name}*"):
        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        else:
            item.unlink(missing_ok=True)


def clear_all_caches():
    for name in ["pex", "pip"]:
        clear_cache(name)


def create_venv_with_torch(venv_path: str):
    """Create a venv with torch installed using uv."""
    print(f"Creating venv with {TORCH_VERSION} using uv...")
    subprocess.run(["uv", "venv", "--python", PYTHON_VERSION, venv_path], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["uv", "pip", "install", "--python", os.path.join(venv_path, "bin", "python"),
                    TORCH_VERSION, "pex"], check=True)
    print(f"Venv created at {venv_path}")


def create_pex_with_layout(venv_path: str, output_path: str, layout: str,
                           no_pre_install_wheel: bool = False) -> float:
    """Create a pex with the specified layout. Returns time in seconds."""
    clear_all_caches()
    pex_bin = os.path.join(venv_path, "bin", "pex")

    cmd = [
        pex_bin,
        TORCH_VERSION,
        "--layout", layout,
        "--venv-repository", venv_path,
        "--max-install-jobs", "0",
        "-o", output_path,
    ]

    if no_pre_install_wheel:
        cmd.append("--no-pre-install-wheel")

    flag_str = " --no-piw" if no_pre_install_wheel else ""
    print(f"  Creating pex with layout={layout}{flag_str}...")
    start = time.time()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    elapsed = time.time() - start

    if os.path.isfile(output_path):
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
    else:
        size_mb = sum(f.stat().st_size for f in Path(output_path).rglob('*') if f.is_file()) / (1024 * 1024)
    print(f"    Pex created in {elapsed:.2f}s, size: {size_mb:.1f}MB")
    return elapsed


def zip_directory_7z(source_dir: str, output_zip: str, mx: int) -> tuple[float, float]:
    """Zip using 7z with specified compression level. Returns (time, size_mb)."""
    if os.path.exists(output_zip):
        os.remove(output_zip)

    cmd = [
        "7z", "a",
        "-tzip",
        f"-mx={mx}",
        "-mmt=on",
        output_zip,
        os.path.join(source_dir, "*"),
    ]

    start = time.time()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=source_dir)
    elapsed = time.time() - start
    size_mb = os.path.getsize(output_zip) / (1024 * 1024)
    return elapsed, size_mb


def unzip_with_7z(zip_path: str, extract_dir: str) -> float:
    """Unzip using 7z. Returns time in seconds."""
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    os.makedirs(extract_dir)

    start = time.time()
    subprocess.run(["7z", "x", "-y", "-mmt=on", f"-o{extract_dir}", zip_path],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    elapsed = time.time() - start
    return elapsed


def measure_pex_execution(extract_dir: str) -> float:
    """Measure first execution time of an already extracted pex. Returns time in seconds."""
    clear_all_caches()

    cmd = ["python", os.path.join(extract_dir, "__main__.py"), "-c", "import torch; print(torch.__version__)"]

    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"    Execution failed: {result.stderr}")
    else:
        print(f"    First execution: {elapsed:.2f}s (torch version: {result.stdout.strip()})")

    return elapsed


def measure_pex_execution_zipapp(pex_path: str) -> float:
    """Measure first execution time of a zipapp pex. Returns time in seconds."""
    clear_all_caches()

    cmd = [pex_path, "-c", "import torch; print(torch.__version__)"]

    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"    Execution failed: {result.stderr}")
    else:
        print(f"    First execution: {elapsed:.2f}s (torch version: {result.stdout.strip()})")

    return elapsed


def benchmark_layout_with_7z_options(venv_path: str, layout: str,
                                      no_pre_install_wheel: bool) -> List[Dict[str, Any]]:
    """Run benchmark for a specific layout with all 7z options."""
    flag_suffix = "_nopiw" if no_pre_install_wheel else ""

    print("\n" + "="*80)
    print(f"LAYOUT: {layout.upper()}{flag_suffix} - Testing 7z options")
    print("="*80)

    output_path = os.path.join(BENCHMARK_DIR, f"test_{layout}{flag_suffix}")
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    pex_time = create_pex_with_layout(venv_path, output_path, layout, no_pre_install_wheel)

    uncompressed_size_mb = sum(f.stat().st_size for f in Path(output_path).rglob('*') if f.is_file()) / (1024 * 1024)

    results = []

    print(f"\n  Testing 7z compression options (uncompressed: {uncompressed_size_mb:.1f}MB)...")
    print(f"  {'Option':<12} {'mx':>4} {'Compress':>10} {'Size':>10} {'Ratio':>8} {'Extract':>10}")
    print("  " + "-"*60)

    for opt in SEVENZIP_OPTIONS:
        zip_path = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_mx{opt['mx']}.zip")
        extract_dir = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_extract_mx{opt['mx']}")

        compress_time, compressed_size_mb = zip_directory_7z(output_path, zip_path, opt['mx'])
        ratio = (compressed_size_mb / uncompressed_size_mb) * 100

        extract_time = unzip_with_7z(zip_path, extract_dir)
        if opt['mx'] != 1:
            os.remove(zip_path)

        print(f"  {opt['name']:<12} {opt['mx']:>4} {compress_time:>8.2f}s {compressed_size_mb:>8.1f}MB {ratio:>6.1f}% {extract_time:>8.2f}s")

        results.append({
            "layout": layout,
            "no_pre_install_wheel": no_pre_install_wheel,
            "layout_display": f"{layout}{flag_suffix}",
            "7z_option": opt['name'],
            "mx": opt['mx'],
            "pex_creation_time": pex_time,
            "compress_time": compress_time,
            "compressed_size_mb": compressed_size_mb,
            "uncompressed_size_mb": uncompressed_size_mb,
            "ratio": ratio,
            "extract_time": extract_time,
            "vary_7z": True,
        })

        shutil.rmtree(extract_dir)

    # Measure execution time with mx=1
    print(f"\n  Measuring pex execution time (using mx=1 archive)...")
    zip_path = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_mx1.zip")
    extract_dir = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_extract_exec")
    unzip_with_7z(zip_path, extract_dir)
    exec_time = measure_pex_execution(extract_dir)

    for r in results:
        r["execution_time"] = exec_time

    shutil.rmtree(output_path)
    shutil.rmtree(extract_dir)

    return results


def benchmark_layout_simple(venv_path: str, layout: str,
                            no_pre_install_wheel: bool) -> Dict[str, Any]:
    """Run benchmark for a specific layout with default 7z option only."""
    flag_suffix = "_nopiw" if no_pre_install_wheel else ""

    print("\n" + "="*80)
    print(f"LAYOUT: {layout.upper()}{flag_suffix}")
    print("="*80)

    is_zipapp = (layout == "zipapp")

    if is_zipapp:
        output_path = os.path.join(BENCHMARK_DIR, f"test_{layout}{flag_suffix}.pex")
        if os.path.exists(output_path):
            os.remove(output_path)
    else:
        output_path = os.path.join(BENCHMARK_DIR, f"test_{layout}{flag_suffix}")
        if os.path.exists(output_path):
            shutil.rmtree(output_path)

    pex_time = create_pex_with_layout(venv_path, output_path, layout, no_pre_install_wheel)

    if is_zipapp:
        print("\n  Measuring execution time (zipapp)...")
        exec_time = measure_pex_execution_zipapp(output_path)

        return {
            "layout": layout,
            "no_pre_install_wheel": no_pre_install_wheel,
            "layout_display": f"{layout}{flag_suffix}",
            "7z_option": "N/A",
            "mx": -1,
            "pex_creation_time": pex_time,
            "compress_time": 0,
            "compressed_size_mb": os.path.getsize(output_path) / (1024 * 1024),
            "uncompressed_size_mb": 0,
            "ratio": 0,
            "extract_time": 0,
            "execution_time": exec_time,
            "vary_7z": False,
        }
    else:
        uncompressed_size_mb = sum(f.stat().st_size for f in Path(output_path).rglob('*') if f.is_file()) / (1024 * 1024)

        opt = DEFAULT_7Z_OPTION
        zip_path = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_mx{opt['mx']}.zip")
        extract_dir = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_extract")

        print(f"\n  Compressing with 7z mx={opt['mx']}...")
        compress_time, compressed_size_mb = zip_directory_7z(output_path, zip_path, opt['mx'])
        ratio = (compressed_size_mb / uncompressed_size_mb) * 100
        print(f"    Compress: {compress_time:.2f}s, size: {compressed_size_mb:.1f}MB ({ratio:.1f}%)")

        print(f"\n  Extracting...")
        extract_time = unzip_with_7z(zip_path, extract_dir)
        print(f"    Extract: {extract_time:.2f}s")

        print(f"\n  Measuring execution time...")
        exec_time = measure_pex_execution(extract_dir)

        shutil.rmtree(output_path)
        shutil.rmtree(extract_dir)

        return {
            "layout": layout,
            "no_pre_install_wheel": no_pre_install_wheel,
            "layout_display": f"{layout}{flag_suffix}",
            "7z_option": opt['name'],
            "mx": opt['mx'],
            "pex_creation_time": pex_time,
            "compress_time": compress_time,
            "compressed_size_mb": compressed_size_mb,
            "uncompressed_size_mb": uncompressed_size_mb,
            "ratio": ratio,
            "extract_time": extract_time,
            "execution_time": exec_time,
            "vary_7z": False,
        }


def run_benchmark():
    """Run the full benchmark suite."""
    os.makedirs(BENCHMARK_DIR, exist_ok=True)
    venv_path = os.path.join(BENCHMARK_DIR, "torch_venv")

    if not os.path.exists(venv_path):
        create_venv_with_torch(venv_path)
    else:
        print(f"Using existing venv at {venv_path}")

    all_results: List[Dict[str, Any]] = []

    # Define test configurations
    # (layout, no_pre_install_wheel, vary_7z_options)
    test_configs = [
        ("packed", False, True),
        ("packed", True, True),
        ("loose", False, True),
        ("loose", True, True),
        ("zipapp", False, False),
        ("zipapp", True, False),
    ]

    for layout, no_piw, vary_7z in test_configs:
        if vary_7z:
            results = benchmark_layout_with_7z_options(venv_path, layout, no_piw)
            all_results.extend(results)
        else:
            result = benchmark_layout_simple(venv_path, layout, no_piw)
            all_results.append(result)

    # Summary - Simple results (non-varied 7z)
    simple_results = [r for r in all_results if not r['vary_7z']]
    if simple_results:
        print("\n" + "="*140)
        print("SUMMARY: LAYOUT COMPARISON (default 7z mx=6)")
        print("="*140)

        print(f"\n{'Layout':<35} {'Pex Create':>12} {'Compress':>10} {'Size':>10} {'Extract':>10} {'1st Exec':>10} {'Total Create':>14} {'Total Exec':>12}")
        print("-" * 140)

        for r in simple_results:
            total_create = r['pex_creation_time'] + r['compress_time']
            total_exec = r['extract_time'] + r['execution_time']
            print(f"{r['layout_display']:<35} {r['pex_creation_time']:>10.2f}s {r['compress_time']:>8.2f}s {r['compressed_size_mb']:>8.1f}MB {r['extract_time']:>8.2f}s {r['execution_time']:>8.2f}s {total_create:>12.2f}s {total_exec:>10.2f}s")

    # Summary - 7z varied results
    varied_results = [r for r in all_results if r['vary_7z']]
    if varied_results:
        print("\n" + "="*140)
        print("SUMMARY: 7z COMPRESSION OPTIONS COMPARISON")
        print("="*140)

        print(f"\n{'Layout':<35} {'7z Option':<12} {'mx':>4} {'Pex Create':>12} {'Compress':>10} {'Size':>10} {'Ratio':>8} {'Extract':>10} {'Total Create':>14}")
        print("-" * 140)

        for r in varied_results:
            total_create = r['pex_creation_time'] + r['compress_time']
            print(f"{r['layout_display']:<35} {r['7z_option']:<12} {r['mx']:>4} {r['pex_creation_time']:>10.2f}s {r['compress_time']:>8.2f}s {r['compressed_size_mb']:>8.1f}MB {r['ratio']:>6.1f}% {r['extract_time']:>8.2f}s {total_create:>12.2f}s")

        # Speedup analysis
        print("\n" + "="*140)
        print("SPEEDUP ANALYSIS (vs mx=6)")
        print("="*140)

        for layout in ["packed", "loose"]:
            layout_results = [r for r in varied_results if r['layout'] == layout]
            if not layout_results:
                continue
            baseline = next((r for r in layout_results if r['mx'] == 6), None)
            if not baseline:
                continue

            flag_label = " (--no-pre-install-wheel)" if layout_results[0]['no_pre_install_wheel'] else ""
            print(f"\n{layout.upper()}{flag_label}:")
            for r in layout_results:
                if r['mx'] != 6:
                    compress_speedup = baseline['compress_time'] / r['compress_time'] if r['compress_time'] > 0 else 0
                    extract_speedup = baseline['extract_time'] / r['extract_time'] if r['extract_time'] > 0 else 0
                    size_diff = ((r['compressed_size_mb'] - baseline['compressed_size_mb']) / baseline['compressed_size_mb']) * 100
                    print(f"  mx={r['mx']} ({r['7z_option']:<8}): compress {compress_speedup:>5.2f}x faster, extract {extract_speedup:>5.2f}x faster, size {size_diff:>+6.1f}%")

        # Recommendation
        print("\n" + "="*140)
        print("RECOMMENDATION")
        print("="*140)

        for layout in ["packed", "loose"]:
            layout_results = [r for r in varied_results if r['layout'] == layout]
            if not layout_results:
                continue

            mx6 = next((r for r in layout_results if r['mx'] == 6), None)
            mx1 = next((r for r in layout_results if r['mx'] == 1), None)
            mx3 = next((r for r in layout_results if r['mx'] == 3), None)

            if mx6 and mx1 and mx3:
                flag_label = " (--no-pre-install-wheel)" if layout_results[0]['no_pre_install_wheel'] else ""
                print(f"\n{layout.upper()}{flag_label}:")
                print(f"  Current (mx=6): compress={mx6['compress_time']:.2f}s, size={mx6['compressed_size_mb']:.1f}MB")
                print(f"  Fastest (mx=1): compress={mx1['compress_time']:.2f}s ({mx6['compress_time']/mx1['compress_time']:.1f}x faster), size={mx1['compressed_size_mb']:.1f}MB (+{((mx1['compressed_size_mb']-mx6['compressed_size_mb'])/mx6['compressed_size_mb'])*100:.1f}%)")
                print(f"  Balanced (mx=3): compress={mx3['compress_time']:.2f}s ({mx6['compress_time']/mx3['compress_time']:.1f}x faster), size={mx3['compressed_size_mb']:.1f}MB (+{((mx3['compressed_size_mb']-mx6['compressed_size_mb'])/mx6['compressed_size_mb'])*100:.1f}%)")


if __name__ == "__main__":
    run_benchmark()
