# ===============================================================================
# Description:    Module to read & write OAuth token records
# ===============================================================================

import apsw
import apsw.ext
import jwt
import os
import logging
import AuthToken

logger = logging.getLogger(__name__)


# Forward SQLite logs to the logging module
apsw.ext.log_sqlite()


# === Read a SQLite database ===
def read_db():

    # Initialize variable
    tokens = {}

    # Check if SQLite database exists
    if os.path.isfile("token.db3"):
        # Create connection to the existing database
        connection = apsw.Connection("token.db3", flags=apsw.SQLITE_OPEN_READONLY)

        # Get values
        for val1, val2 in connection.execute("select val1, val2 from anaplan"):
            tokens = {"val1": val1, "val2": jwt.decode(val2, val1, algorithms=["HS256"])['val2']}

    else:
        logger.warning("Database file does not exist")
        tokens = {"val1": "empty", "val2": "empty"}

    return tokens

# === Create or update a SQLite database ===
def write_db():
    
    # Encode 
    encoded_token = jwt.encode(
        {"val2": AuthToken.Auth.refresh_token}, AuthToken.Auth.client_id, algorithm="HS256")
    values = (AuthToken.Auth.client_id, encoded_token)

    # Check if SQLite database exists
    if os.path.isfile("token.db3"):
        # Create connection to the existing database
        connection = apsw.Connection("token.db3", flags=apsw.SQLITE_OPEN_READWRITE)
        connection.execute("update anaplan set val1=$val1, val2=$val2", values)
    else:
        # Create a new database
        connection = apsw.Connection("token.db3")
        connection.execute("create table if not exists anaplan (val1, val2)")
        connection.execute("insert into anaplan values($val1, $val2)", values)
    
    logger.info("Tokens updated")
