from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine
import os

@contextmanager
def connect_mysql():
    conn_info = (
        f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}"
        f"@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception as e:
        raise RuntimeError("Failed to connect to MySQL") from e

class MySQLIOManager(IOManager):
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # Implement if you need to write data back to MySQL
        table_name = context.name
        with connect_mysql() as db_conn:
            obj.to_sql(table_name, db_conn, if_exists='replace', index=False)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        # Implement if you need to read data from MySQL as input
        table_name = context.upstream_output.name
        sql = f"SELECT * FROM {table_name}"
        return self.extract_data(sql)

    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql() as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn)
            return pd_data