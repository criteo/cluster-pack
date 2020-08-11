import os
import pytest
import subprocess
import tempfile
from cluster_pack import filesystem


lines = ("abcdef\n"
         "\n"
         "\n"
         "123456789\n"
         "\n"
         "\n")


def _create_temp_file(temp_dir: str, filename: str = "myfile.txt"):
    file = os.path.join(temp_dir, filename)
    with open(file, "wb") as f:
        f.write(lines.encode())
    return file


@pytest.mark.parametrize(
    "size,expected_line",
    [
        (None, b"abcdef\n"),
        (3, b"abc"),
        (7, b"abcdef\n"),
        (10, b"abcdef\n"),
    ]
)
def test_readline(size, expected_line):
    with tempfile.TemporaryDirectory() as temp_dir:
        file = _create_temp_file(temp_dir)

        resolved_fs, path = filesystem.resolve_filesystem_and_path(file)

        with resolved_fs.open(file, "rb") as fs_file:
            line = fs_file.readline(size)
            assert line == expected_line


@pytest.mark.parametrize(
    "size,expected_lines",
    [
        (None, [b"abcdef\n", b"\n", b"\n", b"123456789\n", b"\n", b"\n"]),
        (3, [b'abcdef\n']),
        (7, [b'abcdef\n', b'\n']),
        (10, [b"abcdef\n", b"\n", b"\n", b"123456789\n"]),
    ]
)
def test_readlines(size, expected_lines):
    with tempfile.TemporaryDirectory() as temp_dir:
        file = _create_temp_file(temp_dir)

        resolved_fs, path = filesystem.resolve_filesystem_and_path(file)

        with resolved_fs.open(file, "rb") as fs_file:
            lines = fs_file.readlines(size)
            assert lines == expected_lines


def test_file_as_lines_list():
    with tempfile.TemporaryDirectory() as temp_dir:
        file = _create_temp_file(temp_dir)

        resolved_fs, path = filesystem.resolve_filesystem_and_path(file)

        with resolved_fs.open(file, "rb") as fs_file:
            lines = list(fs_file)
            assert lines == [b"abcdef\n", b"\n", b"\n", b"123456789\n", b"\n", b"\n"]


def test_file_iterator():
    with tempfile.TemporaryDirectory() as temp_dir:
        file = _create_temp_file(temp_dir)

        resolved_fs, path = filesystem.resolve_filesystem_and_path(file)

        with resolved_fs.open(file, "rb") as fs_file:
            it = iter(fs_file)
            line = next(it)
            assert line == b"abcdef\n"
            line = next(it)
            assert line == b"\n"


def test_chmod():
    with tempfile.TemporaryDirectory() as temp_dir:
        file = f"{temp_dir}/script.sh"
        with open(file, "wb") as f:
            lines = ("#! /bin/bash\n"
                     "echo 'Hello world'\n")
            f.write(lines.encode())

        fs, _ = filesystem.resolve_filesystem_and_path(file)

        with pytest.raises(PermissionError):
            subprocess.check_output([file])
        fs.chmod(file, 0o755)

        output = subprocess.check_output([file])
        assert "Hello world" in output.decode()


def test_rm():
    with tempfile.TemporaryDirectory() as temp_dir:
        d = os.path.join(temp_dir, "a", "b", "c")
        os.makedirs(d)
        file1 = _create_temp_file(d, "file1.txt")
        file2 = _create_temp_file(d, "file2.txt")

        fs, _ = filesystem.resolve_filesystem_and_path(file1)

        assert fs.exists(file1)
        assert fs.exists(file2)
        assert fs.exists(d)

        fs.rm(file1)
        fs.rm(d, recursive=True)

        assert not fs.exists(file1)
        assert not fs.exists(file2)
        assert not fs.exists(d)


def test_put():
    with tempfile.TemporaryDirectory() as temp_dir:
        file = f"{temp_dir}/script.sh"
        with open(file, "wb") as f:
            lines = ("#! /bin/bash\n"
                     "echo 'Hello world'\n")
            f.write(lines.encode())
        os.chmod(file, 0o755)

        fs, _ = filesystem.resolve_filesystem_and_path(file)

        remote_file = f"{temp_dir}/copied_script.sh"
        fs.put(file, remote_file)

        assert os.path.exists(remote_file)
        assert os.stat(remote_file).st_mode & 0o777 == 0o755
