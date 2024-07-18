import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from dagster import IOManager, InputContext, OutputContext

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception as e:
        raise RuntimeError("Failed to connect to MinIO") from e

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join(context.asset_key.path)
        )
        if context.has_asset_partitions:
            partition_key = context.asset_partition_key_for_output()
            return f"{key}/{partition_key}.pq", tmp_file_path
        else:
            return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        table = pa.Table.from_pandas(obj)
        pq.write_table(table, tmp_file_path)

        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")

                client.fput_object(bucket_name, key_name, tmp_file_path)
                row_count = len(obj)
                context.add_output_metadata(
                    {"path": key_name, "records": row_count, "tmp": tmp_file_path}
                )
            os.remove(tmp_file_path)
        except Exception as e:
            raise RuntimeError("Failed to handle MinIO output") from e

    def load_input(self, context: InputContext) -> pd.DataFrame:
        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)
        try:
            with connect_minio(self._config) as client:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")

                client.fget_object(bucket_name, key_name, tmp_file_path)
                pd_data = pd.read_parquet(tmp_file_path)
            return pd_data
        except Exception as e:
            raise RuntimeError("Failed to handle MinIO input") from e
