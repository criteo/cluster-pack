#!/usr/bin/env python3
"""Benchmark pex creation comparing shutil vs zipfile for zipping, across 3 pex layouts.

This benchmark only runs when uv is available and uses --max-install-jobs 0 setting.

This has been written by AMP, is not linted and is meant to only run experiments for optimization purposes.
"""

import os
import shutil
import subprocess
import time
import zipfile
from pathlib import Path
from typing import List, Dict, Any

PYTHON_VERSION = "3.11"
TORCH_VERSION = "torch==2.8.0"
BENCHMARK_DIR = "/tmp/zip_methods_benchmark"

PEX_LAYOUTS = ["zipapp", "packed", "loose"]
ZIP_METHODS = [
    ("shutil", None),
    ("zipfile_cl0", 0),
    ("zipfile_cl1", 1),
]


def check_uv_available() -> bool:
    """Check if uv is installed and available."""
    return shutil.which("uv") is not None


def clear_cache(name: str = "pex") -> None:
    """Clear PEX/pip caches."""
    pex_root = os.path.expanduser(f"~/.{name}")
    if os.path.exists(pex_root):
        shutil.rmtree(pex_root, ignore_errors=True)
    cache_dir = os.path.expanduser(f"~/.cache/{name}")
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir, ignore_errors=True)
    for item in Path("/tmp").glob(f"*.{name}*"):
        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        else:
            item.unlink(missing_ok=True)


def clear_all_caches() -> None:
    for name in ["pex", "pip"]:
        clear_cache(name)


def create_venv_with_torch(venv_path: str) -> None:
    """Create a venv with torch installed using uv."""
    print(f"Creating venv with {TORCH_VERSION} using uv...")
    subprocess.run(["uv", "venv", "--python", PYTHON_VERSION, venv_path], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["uv", "pip", "install", "--python", os.path.join(venv_path, "bin", "python"),
                    TORCH_VERSION, "pex"], check=True)
    print(f"Venv created at {venv_path}")


def create_pex_with_layout(venv_path: str, output_path: str, layout: str) -> float:
    """Create a pex with the specified layout using --max-install-jobs 0. Returns time in seconds."""
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

    print(f"  Creating pex with layout={layout}...")
    start = time.time()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    elapsed = time.time() - start

    if os.path.isfile(output_path):
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
    else:
        size_mb = sum(f.stat().st_size for f in Path(output_path).rglob('*') if f.is_file()) / (1024 * 1024)
    print(f"    Pex created in {elapsed:.2f}s, size: {size_mb:.1f}MB")
    return elapsed


def zip_with_shutil(source_dir: str, output: str) -> tuple[float, float]:
    """Zip using shutil.make_archive. Returns (time, size_mb)."""
    if os.path.exists(output + ".zip"):
        os.remove(output + ".zip")

    start = time.time()
    shutil.make_archive(output, "zip", source_dir)
    elapsed = time.time() - start
    size_mb = os.path.getsize(output + ".zip") / (1024 * 1024)
    return elapsed, size_mb


def zip_with_zipfile(source_dir: str, output: str, compresslevel: int = 1) -> tuple[float, float]:
    """Zip using zipfile module. Returns (time, size_mb)."""
    output_zip = output + ".zip"
    if os.path.exists(output_zip):
        os.remove(output_zip)

    start = time.time()
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED, compresslevel=compresslevel) as zf:
        for root, _, files in os.walk(source_dir):
            for f in files:
                full_path = os.path.join(root, f)
                arc_name = os.path.relpath(full_path, source_dir)
                zf.write(full_path, arc_name)
    elapsed = time.time() - start
    size_mb = os.path.getsize(output_zip) / (1024 * 1024)
    return elapsed, size_mb


def unzip_archive(zip_path: str, extract_dir: str) -> float:
    """Unzip using system's unzip command. Returns time in seconds."""
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    os.makedirs(extract_dir)

    start = time.time()
    subprocess.run(
        ["unzip", "-q", zip_path, "-d", extract_dir],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    elapsed = time.time() - start
    return elapsed


def get_dir_size_mb(path: str) -> float:
    """Get directory size in MB."""
    return sum(f.stat().st_size for f in Path(path).rglob('*') if f.is_file()) / (1024 * 1024)


def measure_pex_execution(pex_path: str, is_zipapp: bool = False) -> float:
    """Measure first execution time of a pex after clearing caches. Returns time in seconds."""
    clear_all_caches()

    if is_zipapp:
        cmd = [pex_path, "-c", "import torch; print(torch.__version__)"]
    else:
        cmd = ["python", os.path.join(pex_path, "__main__.py"), "-c", "import torch; print(torch.__version__)"]

    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"    Execution failed: {result.stderr}")
    else:
        print(f"    First execution: {elapsed:.2f}s (torch version: {result.stdout.strip()})")

    return elapsed


def benchmark_layout(venv_path: str, layout: str) -> List[Dict[str, Any]]:
    """Benchmark a single layout with both zip methods."""
    results = []

    print(f"\n{'='*80}")
    print(f"LAYOUT: {layout.upper()}")
    print("="*80)

    if layout == "zipapp":
        output_path = os.path.join(BENCHMARK_DIR, f"test_{layout}.pex")
        if os.path.exists(output_path):
            os.remove(output_path)
    else:
        output_path = os.path.join(BENCHMARK_DIR, f"test_{layout}")
        if os.path.exists(output_path):
            shutil.rmtree(output_path)

    pex_time = create_pex_with_layout(venv_path, output_path, layout)

    if layout == "zipapp":
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"  zipapp is already a zip file: {size_mb:.1f}MB")

        print(f"\n  Measuring first pex execution...")
        exec_time = measure_pex_execution(output_path, is_zipapp=True)

        results.append({
            "layout": layout,
            "zip_method": "N/A (already zipped)",
            "pex_creation_time": pex_time,
            "compress_time": 0,
            "compressed_size_mb": size_mb,
            "uncompressed_size_mb": size_mb,
            "extract_time": 0,
            "execution_time": exec_time,
        })
    else:
        uncompressed_size_mb = get_dir_size_mb(output_path)
        print(f"  Uncompressed size: {uncompressed_size_mb:.1f}MB")

        for method_name, compresslevel in ZIP_METHODS:
            zip_output = os.path.join(BENCHMARK_DIR, f"{layout}_{method_name}")

            print(f"\n  Testing {method_name}...")
            if method_name == "shutil":
                compress_time, compressed_size_mb = zip_with_shutil(output_path, zip_output)
            else:
                compress_time, compressed_size_mb = zip_with_zipfile(output_path, zip_output, compresslevel)

            ratio = (compressed_size_mb / uncompressed_size_mb) * 100

            extract_dir = os.path.join(BENCHMARK_DIR, f"{layout}_{method_name}_extract")
            extract_time = unzip_archive(zip_output + ".zip", extract_dir)

            print(f"    Compress: {compress_time:.2f}s, Size: {compressed_size_mb:.1f}MB ({ratio:.1f}%), Extract: {extract_time:.2f}s")

            print(f"    Measuring first pex execution...")
            exec_time = measure_pex_execution(extract_dir, is_zipapp=False)

            results.append({
                "layout": layout,
                "zip_method": method_name,
                "pex_creation_time": pex_time,
                "compress_time": compress_time,
                "compressed_size_mb": compressed_size_mb,
                "uncompressed_size_mb": uncompressed_size_mb,
                "ratio": ratio,
                "extract_time": extract_time,
                "execution_time": exec_time,
            })

            shutil.rmtree(extract_dir)
            os.remove(zip_output + ".zip")

        shutil.rmtree(output_path)

    return results


def run_benchmark() -> None:
    """Run the full benchmark suite."""
    if not check_uv_available():
        print("ERROR: uv is not available. This benchmark requires uv.")
        print("Install uv from: https://github.com/astral-sh/uv")
        return

    print("uv detected, proceeding with benchmark...")
    os.makedirs(BENCHMARK_DIR, exist_ok=True)

    venv_path = os.path.join(BENCHMARK_DIR, "torch_venv")
    if not os.path.exists(venv_path):
        create_venv_with_torch(venv_path)
    else:
        print(f"Using existing venv at {venv_path}")

    all_results: List[Dict[str, Any]] = []

    for layout in PEX_LAYOUTS:
        results = benchmark_layout(venv_path, layout)
        all_results.extend(results)

    print("\n" + "="*120)
    print("SUMMARY: ZIP METHODS COMPARISON ACROSS PEX LAYOUTS")
    print("="*120)

    print(f"\n{'Layout':<12} {'Zip Method':<22} {'Pex Create':>12} {'Compress':>10} {'Size':>10} {'Ratio':>8} {'Extract':>10} {'1st Exec':>10} {'Total':>12}")
    print("-" * 140)

    for r in all_results:
        total = r['pex_creation_time'] + r['compress_time'] + r.get('extract_time', 0) + r.get('execution_time', 0)
        ratio_str = f"{r.get('ratio', 0):.1f}%" if 'ratio' in r else "N/A"
        exec_time = r.get('execution_time', 0)
        print(f"{r['layout']:<12} {r['zip_method']:<22} {r['pex_creation_time']:>10.2f}s {r['compress_time']:>8.2f}s {r['compressed_size_mb']:>8.1f}MB {ratio_str:>8} {r.get('extract_time', 0):>8.2f}s {exec_time:>8.2f}s {total:>10.2f}s")

    print("\n" + "="*120)
    print("COMPARISON: shutil vs zipfile (for packed and loose layouts)")
    print("="*120)

    for layout in ["packed", "loose"]:
        layout_results = [r for r in all_results if r['layout'] == layout]
        if len(layout_results) < 2:
            continue

        shutil_r = next((r for r in layout_results if r['zip_method'] == 'shutil'), None)
        zipfile_cl0_r = next((r for r in layout_results if r['zip_method'] == 'zipfile_cl0'), None)
        zipfile_cl1_r = next((r for r in layout_results if r['zip_method'] == 'zipfile_cl1'), None)

        if shutil_r:
            print(f"\n{layout.upper()}:")
            print(f"  shutil:       compress={shutil_r['compress_time']:.2f}s, size={shutil_r['compressed_size_mb']:.1f}MB")

            if zipfile_cl0_r:
                speedup = shutil_r['compress_time'] / zipfile_cl0_r['compress_time'] if zipfile_cl0_r['compress_time'] > 0 else 0
                size_diff = ((zipfile_cl0_r['compressed_size_mb'] - shutil_r['compressed_size_mb']) / shutil_r['compressed_size_mb']) * 100
                print(f"  zipfile_cl0:  compress={zipfile_cl0_r['compress_time']:.2f}s, size={zipfile_cl0_r['compressed_size_mb']:.1f}MB ({speedup:.2f}x {'faster' if speedup > 1 else 'slower'}, size {size_diff:+.1f}%)")

            if zipfile_cl1_r:
                speedup = shutil_r['compress_time'] / zipfile_cl1_r['compress_time'] if zipfile_cl1_r['compress_time'] > 0 else 0
                size_diff = ((zipfile_cl1_r['compressed_size_mb'] - shutil_r['compressed_size_mb']) / shutil_r['compressed_size_mb']) * 100
                print(f"  zipfile_cl1:  compress={zipfile_cl1_r['compress_time']:.2f}s, size={zipfile_cl1_r['compressed_size_mb']:.1f}MB ({speedup:.2f}x {'faster' if speedup > 1 else 'slower'}, size {size_diff:+.1f}%)")


if __name__ == "__main__":
    run_benchmark()
