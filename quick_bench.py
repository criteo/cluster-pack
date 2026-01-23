#!/usr/bin/env python3
"""Quick benchmark: 7z vs zipfile with mx=0 and mx=1."""

import os
import shutil
import subprocess
import time
import zipfile
from pathlib import Path

BENCHMARK_DIR = "/tmp/quick_bench"
VENV_PATH = os.path.join(BENCHMARK_DIR, "torch_venv")
LOOSE_DIR = os.path.join(BENCHMARK_DIR, "loose_pex")


def setup_venv():
    if os.path.exists(VENV_PATH):
        print(f"Using existing venv: {VENV_PATH}")
        return
    print("Creating venv with torch==2.8.0...")
    subprocess.run(["uv", "venv", VENV_PATH], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["uv", "pip", "install", "--python", f"{VENV_PATH}/bin/python",
                    "torch==2.8.0", "pex"], check=True)
    print("Venv ready.")


def build_loose_pex():
    if os.path.exists(LOOSE_DIR):
        print(f"Using existing loose pex: {LOOSE_DIR}")
        return
    print("Building loose pex with --max-install-jobs 0 --venv-repository...")
    cmd = [
        f"{VENV_PATH}/bin/pex",
        "torch==2.8.0",
        "--layout", "loose",
        "--venv-repository", VENV_PATH,
        "--max-install-jobs", "0",
        "-o", LOOSE_DIR,
    ]
    start = time.time()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"Pex built in {time.time() - start:.2f}s")


def get_dir_size_mb(path):
    return sum(f.stat().st_size for f in Path(path).rglob('*') if f.is_file()) / (1024 * 1024)


def zip_with_7z(source, output, mx, method="Deflate"):
    if os.path.exists(output):
        os.remove(output)
    cmd = ["/usr/local/7z/7za", "a", "-tzip", f"-mm={method}", f"-mx={mx}", "-mmt=on", output, f"{source}/*"]
    start = time.time()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=source)
    elapsed = time.time() - start
    size = os.path.getsize(output) / (1024 * 1024)
    return elapsed, size


def unzip_with_7z(zip_path, extract_dir):
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    os.makedirs(extract_dir)
    start = time.time()
    subprocess.run(["/usr/local/7z/7za", "x", "-y", "-mmt=on", f"-o{extract_dir}", zip_path],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    elapsed = time.time() - start
    return elapsed


def zip_with_zipfile(source, output, compresslevel, method=zipfile.ZIP_DEFLATED):
    if os.path.exists(output):
        os.remove(output)
    start = time.time()
    with zipfile.ZipFile(output, 'w', method, compresslevel=compresslevel) as zf:
        for root, _, files in os.walk(source):
            for f in files:
                full = os.path.join(root, f)
                arc = os.path.relpath(full, source)
                zf.write(full, arc)
    elapsed = time.time() - start
    size = os.path.getsize(output) / (1024 * 1024)
    return elapsed, size

def zip_with_shutil(source, output):
    if os.path.exists(output):
        os.remove(output)
    start = time.time()
    shutil.make_archive(output, "zip", source)
    elapsed = time.time() - start
    size = os.path.getsize(f"{output}.zip") / (1024 * 1024)
    return elapsed, size



def main():
    os.makedirs(BENCHMARK_DIR, exist_ok=True)
    setup_venv()
    build_loose_pex()

    uncompressed_mb = get_dir_size_mb(LOOSE_DIR)
    print(f"\nUncompressed size: {uncompressed_mb:.1f} MB")
    print("\n" + "="*90)
    print(f"{'Method':<30} {'Level':>10} {'Compress':>10} {'Size':>10} {'Ratio':>8} {'Extract':>10}")
    print("="*90)

    tests = [
        # (name, output_file, compress_func, extract_needed)
        #("shutil", "shutil.zip", lambda: zip_with_shutil(LOOSE_DIR, f"{BENCHMARK_DIR}/shutil"), False),
        # ("7z Deflate", "7z_deflate_mx0.zip", lambda: zip_with_7z(LOOSE_DIR, f"{BENCHMARK_DIR}/7z_deflate_mx0.zip", 0, "Deflate"), True),
        # ("7z Deflate", "7z_deflate_mx1.zip", lambda: zip_with_7z(LOOSE_DIR, f"{BENCHMARK_DIR}/7z_deflate_mx1.zip", 1, "Deflate"), True),
        # ("7z Zstd", "7z_zstd_mx0.zip", lambda: zip_with_7z(LOOSE_DIR, f"{BENCHMARK_DIR}/7z_zstd_mx0.zip", 0, "Zstd"), True),
        # ("7z Zstd", "7z_zstd_mx1.zip", lambda: zip_with_7z(LOOSE_DIR, f"{BENCHMARK_DIR}/7z_zstd_mx1.zip", 1, "Zstd"), True),
        # ("7z LZMA", "7z_lzma_mx0.zip", lambda: zip_with_7z(LOOSE_DIR, f"{BENCHMARK_DIR}/7z_lzma_mx0.zip", 0, "LZMA"), True),
        # ("7z LZMA", "7z_lzma_mx1.zip", lambda: zip_with_7z(LOOSE_DIR, f"{BENCHMARK_DIR}/7z_lzma_mx1.zip", 1, "LZMA"), True),
        ("zipfile", "zipfile_cl0.zip", lambda: zip_with_zipfile(LOOSE_DIR, f"{BENCHMARK_DIR}/zipfile_cl0.zip", 0, zipfile.ZIP_ZSTANDARD ), False),
        ("zipfile", "zipfile_cl1.zip", lambda: zip_with_zipfile(LOOSE_DIR, f"{BENCHMARK_DIR}/zipfile_cl1.zip", 1), False),
    ]

    tests = [
        ("zipfile", f"{method}_cl{mx}.zip", lambda: zip_with_zipfile(LOOSE_DIR, f"{BENCHMARK_DIR}/{method}_cl{mx}.zip", mx, method ), True)
        for method in [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED, zipfile.ZIP_BZIP2, zipfile.ZIP_LZMA]
        for mx in [0, 1, 3]
    ]


    for name, output_file, compress_func, do_extract in [tests[t] for t in [1, 3, 4, 6]]:
        # Get level from filename
        if "mx0" in output_file or "cl0" in output_file:
            level = "store/0"
        elif "mx1" in output_file or "cl1" in output_file:
            level = "fast/1"
        else:
            level = "default"

        t, s = compress_func()
        ratio = s / uncompressed_mb * 100

        # Measure extraction if applicable
        if do_extract:
            extract_dir = f"{BENCHMARK_DIR}/extract_{output_file.replace('.zip', '')}"
            extract_t = unzip_with_7z(f"{BENCHMARK_DIR}/{output_file}", extract_dir)
            print(f"{name:<30} {level:>10} {t:>8.2f}s {s:>8.1f}MB {ratio:>6.1f}% {extract_t:>8.2f}s")
            shutil.rmtree(extract_dir)
        else:
            print(f"{name:<30} {level:>10} {t:>8.2f}s {s:>8.1f}MB {ratio:>6.1f}% {'N/A':>10}")

        os.remove(f"{BENCHMARK_DIR}/{output_file}")
    print("="*90)


if __name__ == "__main__":
    main()
