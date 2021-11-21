try:
    import os
    import pandas as pd
    from SFConfig import SRCFILE
    from SnowflakeConn import SnowConnection
except ImportError as e:
    print(f"Import Error; {e}")
    exit(1)


class TDSFMigration:

    def __init__(self):
        self.warehouse = None
        self.schema = None
        self.database = None
        self.table = None

        # excel filename to read queries and process it
        self.filename = SRCFILE
        self.sfobj = SnowConnection()

    def read_file(self, filename):

        """
        Reading input file of sql queries to deploy on snowflake database.
        input should be excel file and output will be returned as DataFrame.
        :param filename: file Path
        :return: DataFrame
        """

        try:
            df = pd.read_excel(filename)
            return df
        except Exception as err:
            print(f"TDSFMigration; read_file(); Failed to read file; {err}")
            exit(1)

    def process_queries(self):

        try:
            conn = self.sfobj.connect_sf()
            df = self.read_file(self.filename)

            cols = df.columns.tolist()
            cols.append("Status")
            outdf = pd.DataFrame(columns=cols)

            for index, row in df.iterrows():
                # Update index number in row to get respective column from df to run query
                query = row[2]
                response = self.sfobj.sf_run_query(conn, str(query).strip())
                row = list(row)
                row.append(str(response))
                outdf.loc[len(outdf.index)] = row

            out_file = f"OUT_{os.path.basename(self.filename)}"
            outdf.to_excel(out_file, index=False)
            return "Done"
        except Exception as err:
            print(f"TDSFMigration; process_queries(); {err}")
            return err


if __name__ == '__main__':
    obj = TDSFMigration()
    res = obj.process_queries()
    print(res)
