from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception as e:
        raise RuntimeError("Failed to connect to PostgreSQL") from e

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        with connect_psql(self._config) as conn:
            obj.to_sql(name=table, con=conn, schema=schema, if_exists='replace', index=False)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        with connect_psql(self._config) as conn:
            query = f"SELECT * FROM {schema}.{table}"
            return pd.read_sql_query(query, conn)
