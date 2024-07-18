# from dagster import Definitions, load_assets_from_modules

# from . import assets
# from .resource.minio_io_manager import MinIOIOManager
# from .resource.mysql_io_manager import MySQLIOManager
# from .resource.psql_io_manager import PostgreSQLIOManager

# all_assets = load_assets_from_modules([assets])

# MYSQL_CONFIG = {
#     "host": "mysql",
#     "port": 3306,
#     "database": "exampledb",
#     "user": "admin",
#     "password": "admin123",
# }

# MINIO_CONFIG = {
#     "endpoint_url": "minio:9000",
#     "bucket": "warehouse",
#     "aws_access_key_id": "minio",
#     "aws_secret_access_key": "minio123",
# }

# PSQL_CONFIG = {
#     "host": "postgresql",
#     "port": 5432,
#     "database": "exampledb",
#     "user": "admin",
#     "password": "123",
# }

# defs = Definitions(
#     assets=all_assets,
#     resources={
#         "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
#         "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
#         "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
#     },
# )