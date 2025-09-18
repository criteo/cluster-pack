import logging
import functools
import pytest
import skein
import uuid
import tempfile

from cluster_pack import yarn_launcher
from cluster_pack import filesystem

pytestmark = pytest.mark.hadoop

_logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def path_to_hdfs():
    file_content = "Hello!"
    path_on_hdfs = f"hdfs:///tmp/{uuid.uuid4()}"
    filepath_on_hdfs = f"{path_on_hdfs}/hello.txt"

    fs, _ = filesystem.resolve_filesystem_and_path(path_on_hdfs)

    with tempfile.TemporaryDirectory() as temp_dir:
        with open(f"{temp_dir}/hello.txt", "w") as fd:
            fd.write(file_content)
        fs.mkdir(path_on_hdfs)
        fs.put(f"{temp_dir}/hello.txt", filepath_on_hdfs)
    yield filepath_on_hdfs, file_content
    fs.rm(path_on_hdfs, recursive=True)
    fs.close()


def _submit_and_await_app_master(
    func, assert_result_status=True, assert_log_content=None
):
    with skein.Client() as client:
        log_output_path = f"hdfs:///tmp/{uuid.uuid4()}.log"
        app_id = yarn_launcher.submit_func(
            client,
            func=func,
            args=[],
            memory="2 GiB",
            process_logs=functools.partial(
                yarn_launcher.upload_logs_to_hdfs, log_output_path
            ),
        )
        result = yarn_launcher.wait_for_finished(client, app_id)

        fs, _ = filesystem.resolve_filesystem_and_path(log_output_path)
        with fs.open(log_output_path, "rb") as f:
            logs = f.read().decode()
            assert result == assert_result_status
            _logger.info(f"appmaster logs:\n{logs}")
            assert assert_log_content in logs


def test_failing_app(path_to_hdfs):
    filepath_on_hdfs, file_content = path_to_hdfs

    def launch_app():
        print("exit appli ..")
        raise ValueError

    _submit_and_await_app_master(
        launch_app, assert_result_status=False, assert_log_content="exit appli .."
    )


def test_skein(path_to_hdfs):
    filepath_on_hdfs, file_content = path_to_hdfs

    def launch_skein():
        with skein.Client() as client:
            service = skein.Service(
                resources=skein.model.Resources("1 GiB", 1),
                script=f"""
                    set -x
                    hdfs dfs -cat {filepath_on_hdfs}
                """,
            )
            spec = skein.ApplicationSpec(services={"service": service})
            app_id = client.submit(spec)
            yarn_launcher.wait_for_finished(client, app_id)
            logs = yarn_launcher.get_application_logs(client, app_id, 2)
            for key, value in logs.items():
                print(f"skein logs:{key} {value}")

    _submit_and_await_app_master(launch_skein, assert_log_content=file_content)


@pytest.mark.parametrize(
    "submit_mode",
    [True, False],
)
def test_pyspark(path_to_hdfs, submit_mode):
    filepath_on_hdfs, file_content = path_to_hdfs

    def env(x):
        import subprocess

        return subprocess.check_output(["hdfs", "dfs", "-cat", filepath_on_hdfs])

    def launch_pyspark():
        from pyspark.sql import SparkSession
        import cluster_pack
        from cluster_pack.spark import spark_config_builder

        archive, _ = cluster_pack.upload_env()
        ssb = SparkSession.builder.master("yarn").config(
            "spark.submit.deployMode", submit_mode
        )
        spark_config_builder.add_packaged_environment(ssb, archive)
        sc = ssb.getOrCreate().sparkContext
        hdfs_cat_res = sc.parallelize([1], numSlices=1).map(env).collect()[0]
        print(f"pyspark result:{hdfs_cat_res}")

    _submit_and_await_app_master(launch_pyspark, assert_log_content=file_content)
