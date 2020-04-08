import pytest
import tempfile
from cluster_pack import filesystem


lines = ("abcdef\n"
         "\n"
         "\n"
         "123456789\n"
         "\n"
         "\n")


def _create_temp_file(temp_dir: str):
    file = f"{temp_dir}/myfile.txt"
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
