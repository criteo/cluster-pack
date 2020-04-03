import tempfile
from cluster_pack import filesystem


def _create_temp_file(temp_dir: str):
    file = f"{temp_dir}/myfile.txt"
    with open(file, "wb") as f:
        for i in range(3):
            f.write(f"line {i}\n".encode())
    return file


def test_readline():
    with tempfile.TemporaryDirectory() as temp_dir:
        file = _create_temp_file(temp_dir)

        resolved_fs, path = filesystem.resolve_filesystem_and_path(file)

        with resolved_fs.open(file, "rb") as fs_file:
            line = fs_file.readline()
            assert line == b'line 0\n'


def test_readlines():
    with tempfile.TemporaryDirectory() as temp_dir:
        file = _create_temp_file(temp_dir)

        resolved_fs, path = filesystem.resolve_filesystem_and_path(file)

        with resolved_fs.open(file, "rb") as fs_file:
            lines = fs_file.readlines()
            assert lines == [b'line 0\n', b'line 1\n', b'line 2\n']
