from sqlalchemy.exc import ProgrammingError

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
            #cursor = conn.cursor()
            return conn
        except Exception as err:
            print(f"Error connecting to snowflake {err}")
            exit(1)

    def test_code(self, conn):
        """
        Testing snowflake connection to database
        :param cursor:
        :return:
        """
        try:
            cursor = conn.cursor()
            result = {}
            cursor.execute("SELECT CURRENT_TIMESTAMP(6);")
            sfqid = cursor.sfqid
            result["sfqid"]= sfqid
            result["query_result"]= cursor.query_result(sfqid)
            print(result)
            conn.get_query_status_throw_if_error(sfqid)

        except ProgrammingError as err:
            print(f"test_code(); Query Error: {err}")
            return err
        except Exception as err:
            print(f"Error fetching result {err}")
            return err

    def sf_select_query(self, conn, query):
        """
        perform given query operation on snowflake server and  return result.
        :param cursor: snowflake connection
        :param query: query to perform on  SF DATABASE
        :return: status str
        """
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as err:
            return {"err":err, "sfqid": cursor.sfqid}
        finally:
            conn.close()

    def sf_run_query(self, conn, query):
        """
        perform given query operation on snowflake server and  return result.
        :param cursor: snowflake connection
        :param query: query to perform on  SF DATABASE
        :return: status str
        """
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            data= cursor.fetchall()
            return "Success"
        except Exception as err:
            return {"err": err, "sfqid": cursor.sfqid}
        finally:
            conn.close()

"""
if __name__ == '__main__':
    obj = SnowConnection()
    conn = obj.connect_sf()
    #print("cursor")
    #obj.test_code(conn)
    res = obj.sf_select_query(conn, "SELECT CURRENT_TIMETAMP(6);")
    
    print(res)
"""