try:
    import teradatasql as td
    from Config import SERVER, USERNAME, PASSWORD
except ImportError as err:
    print(f"Error importing; {err}")
    exit(1)


class TeradataConnection:

    def __init__(self):
        pass

    def td_connection(self, host=SERVER, username=USERNAME, password=PASSWORD):
        """
        Connect to Teradata Server for given host details.
        :param host:
        :param username:
        :param password:
        :return: connection object: connection
        """
        try:
            conn = td.connect(host, username, password)
            return conn
        except Exception as err:
            print(f"Error connecting to Teradata {err}")
            exit(1)
