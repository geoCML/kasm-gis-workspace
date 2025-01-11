import psycopg2
import os
import shutil
import subprocess
from time import time
from task_logger import log

ignore_tables = ("spatial_ref_sys", "geometry_columns", "geography_columns", "raster_columns", "raster_overviews")  # TODO: https://github.com/geoCML/tabor/issues/7
ignore_schemas = ("pg_catalog", "information_schema")

def backup_geocml_db():
    try:
        conn = psycopg2.connect(dbname="geocml_db",
                                user="postgres",
                                password=os.environ["GEOCML_POSTGRES_ADMIN_PASSWORD"],
                                host="geocml-postgres",
                                port=5434) # TODO: Ports should be explicitly bound to an env var
    except psycopg2.OperationalError:
        log("Couldn\'t connect to geocml_db; is the postgresql service started?")
        return

    back_up_timestamp = time()
    path_to_backup_dir = os.path.join(os.sep, "Persistence", "DBBackups", str(back_up_timestamp))
    os.mkdir(path_to_backup_dir)
    delete_backup_dir = True

    # Write table schemata to .tabor file
    out = subprocess.run(["tabor", "write", "--db", "geocml_db",
                             "--username", "geocml", "--password", os.environ["GEOCML_POSTGRES_PASSWORD"],
                             "--host", "geocml-postgres",
                             "--port", "5434", # TODO: Ports should be explicitly bound to an env var
                             "--ignore", "raster_columns",
                             "--file", os.path.join(path_to_backup_dir, "geocml_db.tabor")],
                             capture_output=True)

    if out.stderr:
        conn.rollback()
        log("Failed to generate .tabor file {}".format(out.stderr))
        shutil.rmtree(path_to_backup_dir, ignore_errors=True)
        return

    cursor = conn.cursor()
    cursor.execute("""SELECT DISTINCT table_schema FROM information_schema.tables;""")
    schemas = cursor.fetchall()

    # Write backup tables
    for schema in schemas:
        if schema[0] in ignore_schemas:
            continue

        cursor.execute(f"""SELECT * FROM information_schema.tables WHERE table_schema = '{schema[0]}';""")

        tables = cursor.fetchall()

        for table in tables:
            if table[2] in ignore_tables:
                continue

            try:
                cursor.execute(f"""SELECT * from {schema[0]}."{table[2]}", ST_MetaData(rast) as meta;""")
                is_raster = True
            except:
                is_raster = False
                conn.rollback()

            delete_backup_dir = False


            if is_raster:
                if not os.path.exists(os.path.join(path_to_backup_dir, "rasters")):
                    os.mkdir(os.path.join(path_to_backup_dir, "rasters"))

                os.mkdir(os.path.join(path_to_backup_dir, "rasters", f"{schema[0]}.{table[2]}"))
                out = subprocess.run(
                        f'gdal_translate -sds -of GTiff -co "TILED=YES" PG:"host=geocml-postgres port=5434 user=postgres password={os.environ["GEOCML_POSTGRES_ADMIN_PASSWORD"]} dbname=geocml_db schema={schema[0]} table={table[2]}" {os.path.join(path_to_backup_dir, "rasters", f"{schema[0]}.{table[2]}", table[2] + ".tif")}',
                        capture_output=True,
                        shell=True
                )

                if out.stderr.decode("utf-8"):
                    log(f"Failed to backup raster layer: '{table[2]}', {out.stderr}")
                    conn.rollback()
                    shutil.rmtree(path_to_backup_dir, ignore_errors=True)
                    return
            else:
                data_file_path = os.path.join(path_to_backup_dir, "data:{}.{}.csv".format(schema[0], table[2]))
                data_file = open(data_file_path, "w")
                cursor.copy_expert(f"""COPY {schema[0]}."{table[2]}" TO STDOUT WITH (FORMAT csv, DELIMITER ',', HEADER, NULL 'NULL');""", data_file)
                data_file.close()

    if delete_backup_dir: # nothing to back up
        log("Nothing to backup")
        shutil.rmtree(path_to_backup_dir)

    conn.rollback()
    cursor.close()
    conn.close()
