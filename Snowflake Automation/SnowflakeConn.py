try:
    import snowflake.connector
    import os
    import pandas as pd
    from SFConfig import ACCOUNT, USERNAME, PASSWORD, WAREHOUSE, SCHEMA, DATABASE
except ImportError as e:
    print(f" IMport Error; {e} ")
    exit(1)


class SnowConnection:

    def __init__(self):
        pass

    def connect_sf(self, account=ACCOUNT, username=USERNAME, password=PASSWORD, warehouse=WAREHOUSE,
                   database=DATABASE,
                   schema=SCHEMA):
        """
        Connect snowflake to snowflake account using library
        all default parametes are passed from config file if user ignored to pass parameters then it will take from config.

        :param database:
        :param account:
        :param username:
        :param password:
        :param warehouse:
        :param schema:
        :return: Cursor object
        """

        try:
            conn = snowflake.connector.connect(user=username, password=password, account=account, warehouse=warehouse,
                                               database=database, schema=schema)
            cursor = conn.cursor()
            return cursor
        except Exception as err:
            print(f"Error connecting to snowflake {err}")
            exit(1)

    def test_code(self, cursor):
        """
        Testing snowflake connection to database
        :param cursor:
        :return:
        """
        try:
            cursor.execute("SELECT CURRENT_TIMESTAMP(6);")
            print(cursor.fetchall())
        except Exception as err:
            print(f"Error connecting to snowflake {err}")
            exit(1)


if __name__ == '__main__':
    obj = SnowConnection()
    cursor = obj.connect_sf()
    obj.test_code(cursor)
