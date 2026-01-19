#!/usr/bin/env python3
"""Benchmark pex creation and execution with different compression methods and layouts."""

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Any

PYTHON_VERSION= "3.11"
TORCH_VERSION = "torch==2.8.0"
BENCHMARK_DIR = "/tmp/pex_benchmark"


def clear_cache(name: str = "pex"):
    """Clear all PEX caches."""
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
    
    size_mb = os.path.getsize(output_path) / (1024 * 1024) if os.path.isfile(output_path) else \
              sum(f.stat().st_size for f in Path(output_path).rglob('*') if f.is_file()) / (1024 * 1024)
    print(f"    Pex created in {elapsed:.2f}s, size: {size_mb:.1f}MB")
    return elapsed


def zip_directory_shutil(source_dir: str, output_zip: str) -> float:
    """Zip using shutil.make_archive. Returns time in seconds."""
    output_base = output_zip.replace(".zip", "")
    start = time.time()
    shutil.make_archive(output_base, "zip", source_dir)
    elapsed = time.time() - start
    size_mb = os.path.getsize(output_zip) / (1024 * 1024)
    print(f"    shutil.make_archive: {elapsed:.2f}s, size: {size_mb:.1f}MB")
    return elapsed


def zip_directory_7z(source_dir: str, output_zip: str, threads: int = 0) -> float:
    """Zip using 7z. threads=0 means auto. Returns time in seconds."""
    if os.path.exists(output_zip):
        os.remove(output_zip)
    
    cmd = [
        "7z", "a",
        "-tzip",
        "-mx=6",
        f"-mmt={'on' if threads == 0 else threads}",
        output_zip,
        os.path.join(source_dir, "*"),
    ]
    
    start = time.time()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=source_dir)
    elapsed = time.time() - start
    size_mb = os.path.getsize(output_zip) / (1024 * 1024)
    print(f"    7z (threads={'auto' if threads == 0 else threads}): {elapsed:.2f}s, size: {size_mb:.1f}MB")
    return elapsed


def unzip_with_unzip(zip_path: str, extract_dir: str) -> float:
    """Unzip using system unzip command. Returns time in seconds."""
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    
    start = time.time()
    subprocess.run(["unzip", "-q", zip_path, "-d", extract_dir], check=True)
    elapsed = time.time() - start
    print(f"    unzip: {elapsed:.2f}s")
    return elapsed


def unzip_with_7z(zip_path: str, extract_dir: str) -> float:
    """Unzip using 7z. Returns time in seconds."""
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    os.makedirs(extract_dir)
    
    start = time.time()
    subprocess.run(["7z", "x", "-y", "-mmt=on", f"-o{extract_dir}", zip_path],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    elapsed = time.time() - start
    print(f"    7z extract: {elapsed:.2f}s")
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


def benchmark_layout(venv_path: str, layout: str, no_pre_install_wheel: bool, 
                     test_num: int) -> Dict[str, Any]:
    """Run benchmark for a specific layout and flag combination."""
    flag_suffix = "_nopiw" if no_pre_install_wheel else ""
    flag_label = " (no-pre-install-wheel)" if no_pre_install_wheel else ""
    
    print("\n" + "="*70)
    print(f"TEST {test_num}: Layout {layout.upper()}{flag_label}")
    print("="*70)
    
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
            "layout": f"{layout}{flag_suffix}",
            "layout_display": f"{layout}{flag_label}",
            "pex_creation_time": pex_time,
            "shutil_zip_time": 0,
            "7z_zip_time": 0,
            "unzip_time": 0,
            "7z_unzip_time": 0,
            "execution_time": exec_time,
        }
    else:
        print("\n  Compressing pex...")
        zip_shutil = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_shutil.zip")
        zip_7z = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_7z.zip")
        
        shutil_time = zip_directory_shutil(output_path, zip_shutil)
        sevenzip_time = zip_directory_7z(output_path, zip_7z)
        shutil.rmtree(output_path)
        
        print("\n  Measuring unzip times...")
        extract_dir_unzip = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_extract_unzip")
        extract_dir_7z = os.path.join(BENCHMARK_DIR, f"{layout}{flag_suffix}_extract_7z")
        
        unzip_time_unzip = unzip_with_unzip(zip_7z, extract_dir_unzip)
        shutil.rmtree(extract_dir_unzip)
        unzip_time_7z = unzip_with_7z(zip_7z, extract_dir_7z)
        
        print("\n  Measuring execution time (after 7z unzip)...")
        exec_time = measure_pex_execution(extract_dir_7z)
        shutil.rmtree(extract_dir_7z)
        
        return {
            "layout": f"{layout}{flag_suffix}",
            "layout_display": f"{layout}{flag_label}",
            "pex_creation_time": pex_time,
            "shutil_zip_time": shutil_time,
            "7z_zip_time": sevenzip_time,
            "unzip_time": unzip_time_unzip,
            "7z_unzip_time": unzip_time_7z,
            "execution_time": exec_time,
        }


def run_benchmark():
    """Run the full benchmark suite."""
    os.makedirs(BENCHMARK_DIR, exist_ok=True)
    venv_path = os.path.join(BENCHMARK_DIR, "torch_venv")
    
    if not os.path.exists(venv_path):
        create_venv_with_torch(venv_path)
    else:
        print(f"Using existing venv at {venv_path}")
    
    results: List[Dict[str, Any]] = []
    
    layouts = ["zipapp", "packed", "loose"]
    no_pre_install_wheel_options = [True, False]
    
    test_num = 1
    for layout in layouts:
        for no_piw in no_pre_install_wheel_options:
            result = benchmark_layout(venv_path, layout, no_piw, test_num)
            results.append(result)
            test_num += 1
    
    # Summary
    print("\n" + "="*150)
    print("SUMMARY")
    print("="*150)
    header1 = f"{'Layout':<30} {'Pex Create':>12} {'shutil zip':>12} {'7z zip':>10} {'Total Create':>14} {'Total Create':>14} {'unzip':>10} {'7z unzip':>10} {'1st Exec':>10} {'Total Exec':>12} {'Total Exec':>12}"
    header2 = f"{'':30} {'':>12} {'':>12} {'':>10} {'(shutil)':>14} {'(7z)':>14} {'':>10} {'':>10} {'':>10} {'(unzip)':>12} {'(7z)':>12}"
    print(f"\n{header1}")
    print(header2)
    print("-" * 160)
    
    for r in results:
        total_create_shutil = r['pex_creation_time'] + r['shutil_zip_time']
        total_create_7z = r['pex_creation_time'] + r['7z_zip_time']
        total_exec_unzip = r['unzip_time'] + r['execution_time']
        total_exec_7z = r['7z_unzip_time'] + r['execution_time']
        print(f"{r['layout_display']:<30} {r['pex_creation_time']:>10.2f}s {r['shutil_zip_time']:>10.2f}s {r['7z_zip_time']:>8.2f}s {total_create_shutil:>12.2f}s {total_create_7z:>12.2f}s {r['unzip_time']:>8.2f}s {r['7z_unzip_time']:>8.2f}s {r['execution_time']:>8.2f}s {total_exec_unzip:>10.2f}s {total_exec_7z:>10.2f}s")
    
    print("\n" + "="*150)
    print("SPEEDUP ANALYSIS")
    print("="*150)
    for r in results:
        if r['shutil_zip_time'] > 0:
            zip_speedup = r['shutil_zip_time'] / r['7z_zip_time']
            print(f"{r['layout_display']}: compression - 7z is {zip_speedup:.2f}x faster than shutil")
        if r['unzip_time'] > 0:
            unzip_speedup = r['unzip_time'] / r['7z_unzip_time']
            print(f"{r['layout_display']}: extraction - 7z is {unzip_speedup:.2f}x faster than unzip")


if __name__ == "__main__":
    run_benchmark()
