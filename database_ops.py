# ===============================================================================
# Description:    Module for executing SQLite operations
# ===============================================================================

import logging
import sqlite3
import sys
import pandas as pd

# Enable logger
logger = logging.getLogger(__name__)

# ===  Read from tables in the SQLite Database  ===
def read_table(database_file, table):
    try:
        # Establish connection to SQLite
        connection = sqlite3.Connection(database_file)

        # Read the contents of the table into a Data Frame
        df = pd.read_sql_query(f"SELECT * FROM {table}", connection)

        # Close connection
        connection.close()

        return df

    except sqlite3.Error as err:
        logger.warning(f'Table `{table}` does not exist')
        print(f'Table `{table}` does not exist')

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)

# ===  Write to tables in the SQLite Database  ===
def update_table(database_file, table, df, mode, add_unique_id=True):
    try:
        # Establish connection to SQLite
        connection = sqlite3.Connection(database_file)

        # Write the contents of Data Frame to the SQLlite table. If unique_id is false, then a new ID will be generated when uploaded to Anaplan
        if add_unique_id:
            df.to_sql(name=table, con=connection, if_exists=mode, index=False)
        else:
            df.to_sql(name=table, con=connection, if_exists=mode, index=True)

        # Commit data and close connection
        connection.commit()
        connection.close()

    except sqlite3.Error as err:
        print(err)
        logger.warning(f'Table `{table}` does not exist')
        print(f'Table `{table}` does not exist')

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)

# ===  Drop existing tables in the SQLite Database  ===
def drop_table(database_file, table):

    try:
        # Establish connection to SQLite
        connection = sqlite3.Connection(database_file)

        # Create a cursor to perform operations on the database
        cursor = connection.cursor()

        # Dropping the specified table
        cursor.execute(f"DROP TABLE {table}")
        logger.info(f'Table `{table}` has been dropped')
        print(f'Table `{table}` has been dropped')

        # Commit data and close connection
        connection.commit()
        connection.close()

    except sqlite3.Error as err:
        logger.warning(f'Table `{table}` does not exist')
        print(f'Table `{table}` does not exist')

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)

# === Create a new table in the SQLite Database  ===
def create_table(database_file, table, columns):
    
        try:
            # Establish connection to SQLite
            connection = sqlite3.Connection(database_file)
    
            # Create a cursor to perform operations on the database
            cursor = connection.cursor()
    
            # Create the table with the specified columns
            cursor.execute(f"CREATE TABLE {table} ({columns})")
            logger.info(f'Table `{table}` has been created')
            print(f'Table `{table}` has been created')
    
            # Commit data and close connection
            connection.commit()
            connection.close()
    
        except sqlite3.Error as err:
            logger.warning(f'Table `{table}` already exists')
            print(f'Table `{table}` already exists')
    
        except Exception as err:
            print(f'{err} in function "{sys._getframe().f_code.co_name}"')
            logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
            sys.exit(1)

# === Check if a table exists in the SQLite Database ===
def table_exists(database_file, table):
    try:
        # Establish connection to SQLite
        connection = sqlite3.Connection(database_file)

        # Create a cursor to perform operations on the database
        cursor = connection.cursor()

        # Check if the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
        table_exists = cursor.fetchone()

        # Commit data and close connection
        connection.commit()
        connection.close()

        return table_exists

    except sqlite3.Error as err:
        logger.warning(f'Table `{table}` does not exist')
        print(f'Table `{table}` does not exist')

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)

# === Truncate a table in the SQLite Database ===
def truncate_table(database_file, table):
    try:
        # Establish connection to SQLite
        connection = sqlite3.Connection(database_file)

        # Create a cursor to perform operations on the database
        cursor = connection.cursor()

        # Truncate the specified table
        cursor.execute(f"DELETE FROM {table}")
        logger.info(f'Table `{table}` has been truncated')
        print(f'Table `{table}` has been truncated')

        # Commit data and close connection
        connection.commit()
        connection.close()

    except sqlite3.Error as err:
        logger.warning(f'Table `{table}` does not exist')
        print(f'Table `{table}` does not exist')

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)