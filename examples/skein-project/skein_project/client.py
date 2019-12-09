import logging
import skein

from cluster_pack import packaging
from cluster_pack.skein import skein_config_builder, skein_helper


if __name__ == "__main__":

    logging.basicConfig(level="INFO")

    package_path, _ = packaging.upload_env_to_hdfs()

    script = skein_config_builder.get_script(
        package_path,
        module_name="skein_project.worker")
    files = skein_config_builder.get_files(package_path)

    with skein.Client() as client:
        service = skein.Service(
            resources=skein.model.Resources("1 GiB", 1),
            files=files,
            script=script
        )
        spec = skein.ApplicationSpec(services={"service": service})
        app_id = client.submit(spec)

        skein_helper.wait_for_finished(client, app_id)
        logs = skein_helper.get_application_logs(client, app_id, 2)
        if logs:
            for key, value in logs.items():
                print(f"skein logs:{key} {value}")
