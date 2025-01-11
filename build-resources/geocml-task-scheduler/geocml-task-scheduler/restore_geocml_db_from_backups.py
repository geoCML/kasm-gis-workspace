from io import StringIO
import psycopg2
import os
import subprocess
from time import time
from task_logger  import log

def restore_geocml_db_from_backups():
    try:
        conn = psycopg2.connect(dbname="geocml_db",
                                user="postgres",
                                password=os.environ["GEOCML_POSTGRES_ADMIN_PASSWORD"],
                                host="geocml-postgres",
                                port=5434)
    except psycopg2.OperationalError:
        log("Couldn\'t connect to geocml_db; is the postgresql service started?")
        return

    db_backups_dir = os.path.join(os.sep, "Persistence", "DBBackups")
    now = time()
    delta = float("inf")
    most_recent_backup = ""
    for subdir in os.walk(db_backups_dir):
        try:
            subdir_timestamp = float(subdir[0].split("/")[-1])
            if now - subdir_timestamp < delta:
                delta = now - subdir_timestamp
                most_recent_backup = subdir[0]
        except ValueError:
            log("WARNING: Found something unexpected in backup directory")

    if most_recent_backup == "":
        log("No recent backups found. Aborting restoration process.")
        return 0

    log("Restoring geocml_db from {}".format(most_recent_backup))

    # Rebuild tables from .tabor file

    out = subprocess.run(
            [
                "tabor",
                "load",
                "--file", os.path.join(most_recent_backup, "geocml_db.tabor"),
                "--db", "geocml_db",
                "--host", "geocml-postgres",
                "--port", "5434",
                "--username", "postgres",
                "--password", os.environ["GEOCML_POSTGRES_ADMIN_PASSWORD"]
            ], capture_output=True)

    if out.stderr:
        log("Failed to load tables from .tabor file")
        return 0

    cursor = conn.cursor()
    cursor.execute("SET session_replication_role = replica;")

    for csv_data_file in os.listdir(most_recent_backup): # load data from CSV backups
        file_name_split = csv_data_file.split(":")

        if file_name_split[0] == "data":
            log("Found CSV data file {}".format(csv_data_file))
            file_name_split = file_name_split[1].split(".")
            data_file = open(os.path.join(db_backups_dir, most_recent_backup, csv_data_file), "r").readlines()
            cursor.copy_from(
                    StringIO("".join(data_file[1::])),
                    f"{file_name_split[1]}", sep=",",
                    columns=tuple(data_file[0].replace("\n", "").split(",")), null="NULL"
            )
            log(f"Finished loading vector data: {file_name_split[1]}")

    # load raster data
    for raster_file in os.listdir(os.path.join(most_recent_backup, "rasters")):
        out = subprocess.run(
                f"raster2pgsql -C -x -q -e -I {os.path.join(most_recent_backup, 'rasters', raster_file, '*.tif')} -t auto -F {raster_file}",
                capture_output=True,
                shell=True
        )

        made_raster_table = True
        for line in out.stdout.decode("utf-8").split("\n"):
            if line == "":
                continue

            try:
                cursor.execute(line)
            except Exception as e:
                log(f"Failed to load raster data: {e}")
                made_raster_table = False
                break

        if made_raster_table:
            cursor.execute(f"ALTER TABLE {raster_file} OWNER TO geocml;")
            log(f"Finished loading raster data: {raster_file}")

    conn.commit()

    cursor.execute("SET session_replication_role = DEFAULT;")

    cursor.close()
    conn.close()
    return 0
