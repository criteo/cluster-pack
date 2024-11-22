import logging
import skein
import tempfile

import cluster_pack
from cluster_pack import yarn_launcher
from cluster_pack.skein import skein_config_builder


if __name__ == "__main__":

    logging.basicConfig(level="INFO")

    package_path, _ = cluster_pack.upload_env()

    with tempfile.TemporaryDirectory() as tmp_dir:
        skein_config = skein_config_builder.build(
            module_name="skein_project.worker",
            package_path=package_path,
            tmp_dir=tmp_dir
        )

        with skein.Client() as client:
            service = skein.Service(
                resources=skein.model.Resources("1 GiB", 1),
                files=skein_config.files,
                script=skein_config.script
            )
            spec = skein.ApplicationSpec(services={"service": service})
            app_id = client.submit(spec)

            yarn_launcher.wait_for_finished(client, app_id)
            logs = yarn_launcher.get_application_logs(client, app_id, 2)
            if logs:
                for key, value in logs.items():
                    print(f"skein logs:{key} {value}")
