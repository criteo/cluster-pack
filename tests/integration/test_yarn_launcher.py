import getpass
import skein
import pytest
import uuid
import tempfile

from cluster_pack.skein import yarn_launcher
from cluster_pack import filesystem

pytestmark = pytest.mark.hadoop


@pytest.fixture(scope="module")
def path_to_hdfs():
    file_content = "Hello!"
    path_on_hdfs = f"hdfs:///tmp/{uuid.uuid4()}"
    filepath_on_hdfs = f"{path_on_hdfs}/hello.txt"

    fs, _ = filesystem.resolve_filesystem_and_path(path_on_hdfs)

    with tempfile.TemporaryDirectory() as temp_dir:
        with open(f"{temp_dir}/hello.txt", 'w') as fd:
            fd.write(file_content)
        fs.mkdir(path_on_hdfs)
        fs.put(f"{temp_dir}/hello.txt", filepath_on_hdfs)
    yield filepath_on_hdfs, file_content
    fs.rm(path_on_hdfs, recursive=True)
    fs.close()


def submit_and_await_app_master(func, file_content, user=None):
    with skein.Client() as client:
        app_id = yarn_launcher.submit_func(
            client,
            func=func,
            args=[],
            user=user,
            memory="2 GiB")
        yarn_launcher.wait_for_finished(client, app_id)
        logs = yarn_launcher.get_application_logs(client, app_id, 2)
        merged_logs = ""
        for key, value in logs.items():
            merged_logs += value
            print(f"appmaster logs:{key} {value}")
        assert file_content in merged_logs


def test_execute_skein(path_to_hdfs):
    filepath_on_hdfs, file_content = path_to_hdfs

    def launch_skein():
        with skein.Client() as client:
            service = skein.Service(
                resources=skein.model.Resources("1 GiB", 1),
                script=f'''
                    set -x
                    hdfs dfs -cat {filepath_on_hdfs}
                '''
            )
            spec = skein.ApplicationSpec(services={"service": service})
            app_id = client.submit(spec)
            yarn_launcher.wait_for_finished(client, app_id)
            logs = yarn_launcher.get_application_logs(client, app_id, 2)
            for key, value in logs.items():
                print(f"skein logs:{key} {value}")

    submit_and_await_app_master(launch_skein, file_content)


def test_execute_pyspark(path_to_hdfs):
    filepath_on_hdfs, file_content = path_to_hdfs

    def env(x):
        import subprocess
        return subprocess.check_output(["hdfs", "dfs", "-cat", filepath_on_hdfs])

    def launch_pyspark():
        from pyspark.sql import SparkSession
        import cluster_pack
        from cluster_pack.spark import spark_config_builder
        archive, _ = cluster_pack.upload_env()
        ssb = SparkSession.builder.master("yarn").config("spark.submit.deployMode", "client")
        spark_config_builder.add_packaged_environment(ssb, archive)
        sc = ssb.getOrCreate().sparkContext
        hdfs_cat_res = sc.parallelize([1], numSlices=1).map(env).collect()[0]
        print(f"pyspark result:{hdfs_cat_res}")

    submit_and_await_app_master(launch_pyspark, file_content)
