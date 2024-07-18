import pandas as pd
from dagster import asset, Output, Definitions, AssetIn, multi_asset, AssetOut
from dagster import asset, Output, multi_asset, AssetIn, AssetOut, DailyPartitionsDefinition
from resource.minio_io_manager import MinIOIOManager
from resource.mysql_io_manager import MySQLIOManager
from resource.psql_io_manager import postgresql_io_manager

# Extract data from MySQL and load into MinIO
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_order_items_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_order_items_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "olist_order_items_dataset",
            "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_order_payments_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_order_payments_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "olist_order_payments_dataset",
            "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL",
    partitions_def=DailyPartitionsDefinition(start_date="2017-01-01")
)
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_orders_dataset"
    try:
        partition_date_str = context.asset_partition_key_for_output()
        sql_stm += f" WHERE DATE(order_purchase_timestamp) = '{partition_date_str}'"
        context.log.info(f"Partitioned SQL for olist_orders_dataset: {sql_stm}")
    except Exception as e:
        context.log.info(f"olist_orders_dataset has no partition key!")
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "olist_orders_dataset",
            "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_products_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_products_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "olist_products_dataset",
            "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_product_category_name_translation(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM product_category_name_translation"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "product_category_name_translation",
            "records count": len(pd_data),
        },
    )

# Load data from MinIO into PostgreSQL
@multi_asset(
    ins={
        "olist_order_items_dataset": AssetIn(
            key=["bronze", "ecom", "bronze_olist_order_items_dataset"]
        ),
        "olist_order_payments_dataset": AssetIn(
            key=["bronze", "ecom", "bronze_olist_order_payments_dataset"]
        ),
        "olist_orders_dataset": AssetIn(
            key=["bronze", "ecom", "bronze_olist_orders_dataset"]
        ),
        "olist_products_dataset": AssetIn(
            key=["bronze", "ecom", "bronze_olist_products_dataset"]
        ),
        "product_category_name_translation": AssetIn(
            key=["bronze", "ecom", "bronze_product_category_name_translation"]
        ),
    },
    outs={
        "olist_order_items_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"]
        ),
        "olist_order_payments_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"]
        ),
        "olist_orders_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"]
        ),
        "olist_products_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"]
        ),
        "product_category_name_translation": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"]
        ),
    },
    compute_kind="PostgreSQL",
)
def dwh_datasets(context, olist_order_items_dataset, olist_order_payments_dataset, olist_orders_dataset, olist_products_dataset, product_category_name_translation):
    context.log.info("Loading datasets into PostgreSQL")
    return {
        "olist_order_items_dataset": Output(olist_order_items_dataset, metadata={"table": "olist_order_items_dataset"}),
        "olist_order_payments_dataset": Output(olist_order_payments_dataset, metadata={"table": "olist_order_payments_dataset"}),
        "olist_orders_dataset": Output(olist_orders_dataset, metadata={"table": "olist_orders_dataset"}),
        "olist_products_dataset": Output(olist_products_dataset, metadata={"table": "olist_products_dataset"}),
        "product_category_name_translation": Output(product_category_name_translation, metadata={"table": "product_category_name_translation"}),
    }
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "exampledb",
    "user": "admin",
    "password": "admin123",
}

MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

PSQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "exampledb",
    "user": "admin",
    "password": "123",
}
defs = Definitions(
    assets=[
        bronze_olist_order_items_dataset,
        bronze_olist_order_payments_dataset,
        bronze_olist_orders_dataset,
        bronze_olist_products_dataset,
        bronze_product_category_name_translation,
        dwh_datasets
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": postgresql_io_manager(PSQL_CONFIG),
    },
)