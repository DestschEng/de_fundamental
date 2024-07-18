from .resource.minio_io_manager import MinIOIOManager
from .resource.mysql_io_manager import MySQLIOManager
from .resource.psql_io_manager import PostgreSQLIOManager
# Khai báo các cấu hình cho các resource
MYSQL_CONFIG = {
    "host": "mysql",
    "port": 3306,
    "database": "exampledb",
    "user": "admin",
    "password": "admin123",
}

MINIO_CONFIG = {
    "endpoint_url": "minio:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

PSQL_CONFIG = {
    "host": "postgresql",
    "port": 5432,
    "database": "exampledb",
    "user": "admin",
    "password": "123",
}