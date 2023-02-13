# ===============================================================================
# Created:        2 Feb 2023
# @author:        Quinlan Eddy
# Description:    Module to read & write records
# ===============================================================================

import apsw
import apsw.ext
import os
import logging

logger = logging.getLogger(__name__)


# Forward SQLite logs to the logging module
apsw.ext.log_sqlite()


# === Read a SQLite database ===
def read_db():

    # Initialize variable
    tokens = {}

    # Check if SQLite database exists
    if os.path.isfile("dbfile.db3"):
        connection = apsw.Connection("dbfile.db3", flags=apsw.SQLITE_OPEN_READONLY)


        # Get values
        for val1, val2 in connection.execute("select val1, val2 from anaplan"):
            tokens = {"val1": val1, "val2": val2}
            
    else:
        logger.warning("Database file does not exist")
        tokens = {"val1": "empty", "val2": "empty"}

    return tokens

# === Create or update a SQLite database ===
def write_db(values):
    # Check if SQLite database exists
    if os.path.isfile("dbfile.db3"):
        # Create connection to the existing database
        connection = apsw.Connection("dbfile.db3", flags=apsw.SQLITE_OPEN_READWRITE)
        connection.execute("update anaplan set val1=$val1, val2=$val2", values)
    else:
        # Create a new database
        connection = apsw.Connection("dbfile.db3")
        connection.execute("create table if not exists anaplan (val1, val2)")
        connection.execute("insert into anaplan values($val1, $val2)", values)
    
    logger.info("Tokens updated")
