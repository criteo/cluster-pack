import logging
import os
import skein
import time

from typing import Dict, List, Optional

from cluster_pack import packaging

logger = logging.getLogger(__name__)


def get_application_logs(
    client: skein.Client,
    app_id: str,
    wait_for_nb_logs: Optional[int] = None,
    log_tries: int = 15
) -> Optional[skein.model.ApplicationLogs]:
    for ind in range(log_tries):
        try:
            logs = client.application_logs(app_id)
            nb_keys = len(logs.keys())
            logger.info(f"Got {nb_keys}/{wait_for_nb_logs} log files")
            if not wait_for_nb_logs or nb_keys == wait_for_nb_logs:
                return logs
        except Exception:
            logger.warning(
                f"Cannot collect logs (attempt {ind+1}/{log_tries})")
        time.sleep(3)
    return None


def wait_for_finished(client: skein.Client, app_id: str, poll_every_secs: int = 5):
    logger.info(f"waiting for application_id: {app_id}")
    state = None
    while True:
        report = client.application_report(app_id)

        logger.info(
            f"Application report for {app_id} (state: {report.state})")
        if state != report.state:
            logger.info(_format_app_report(report))

        if report.final_status != "undefined":
            logger.info(report.final_status)
            return report.final_status == skein.model.FinalStatus.SUCCEEDED

        time.sleep(poll_every_secs)
        state = report.state

    return False


def _format_app_report(report: skein.model.ApplicationReport) -> str:
    attrs = [
        "queue",
        "start_time",
        "finish_time",
        "final_status",
        "tracking_url",
        "user"
    ]
    return os.linesep + os.linesep.join(
        f"{attr:>16}: {getattr(report, attr) or ''}" for attr in attrs)
