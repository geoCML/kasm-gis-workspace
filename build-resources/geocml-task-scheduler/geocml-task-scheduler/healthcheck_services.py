import os
import subprocess
import yaml

from task_logger import log

def healthcheck_services():
    services = {
        "geocml-desktop": [10000, True],
        "geocml-server": [80, True],
        "geocml-postgres": [5434, True],
    }

    path_to_status_file = os.path.join(os.sep, "Persistence", "geocml-status.yml")

    for service in services:
        out = subprocess.run(["nmap", "-p", str(services[service][0]), service], capture_output=True)

        if out.stderr:
            log(f"{service} is down!")
            services[service][1] = False

    status_file = open(path_to_status_file, "w")
    yaml.safe_dump(services, status_file)
