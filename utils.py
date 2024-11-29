from dotenv import load_env
import os
from typing import Any, Mapping
from src.errors import panic_if_empty


def connection_code_from_config(conf: Mapping[str, Any]):
    database_conf = conf["database"]
    panic_if_empty(database_conf, "database conf")
    panic_if_empty(database_conf["name"], "database name")
    panic_if_empty(database_conf["location"], "databasename is empty")
    load_env()
    database = database_conf["location"] + database_conf["name"]
    connection_code = f"""
    {database}?motherduck_token={os.getenv("motherduck_token")}"""
    if database_conf["location"] not in ["md:", "motherduck:"]:
        connection_code = database
    return connection_code
