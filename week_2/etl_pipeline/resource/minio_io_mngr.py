import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=config.get("secure", False),
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
        tmp_file_path = os.path.join(
            "tmp", f"file-{datetime.today().strftime('%Y%m%d%H%M%S')}-{'-'.join(context.asset_key.path)}.parquet"
        )
        return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        tmp_dir = os.path.dirname(tmp_file_path)
        os.makedirs(tmp_dir, exist_ok=True)  # Ensure the directory exists

        try:
            # Convert DataFrame to Parquet file
            obj.to_parquet(tmp_file_path)
            
            with connect_minio(self._config) as client:
                # Upload to MinIO
                client.fput_object(
                    self._config.get("bucket"),
                    key_name,
                    tmp_file_path
                )
                context.log.info(f"Successfully uploaded {key_name} to MinIO bucket {self._config.get('bucket')}")
            
            # Clean up tmp file
            os.remove(tmp_file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to handle output for {key_name}") from e

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp_file_path = self._get_path(context)
        tmp_dir = os.path.dirname(tmp_file_path)
        os.makedirs(tmp_dir, exist_ok=True)  # Ensure the directory exists

        try:
            with connect_minio(self._config) as client:
                # Download from MinIO
                client.fget_object(
                    self._config.get("bucket"),
                    key_name,
                    tmp_file_path
                )
                context.log.info(f"Successfully downloaded {key_name} from MinIO bucket {self._config.get('bucket')}")

            # Read the Parquet file into a DataFrame
            df = pd.read_parquet(tmp_file_path)
            
            # Clean up tmp file
            os.remove(tmp_file_path)
            
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load input for {key_name}") from e
