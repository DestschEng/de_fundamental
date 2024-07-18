from dagster import asset, Output
import pandas as pd

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
    compute_kind="MySQL"
)
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_orders_dataset"
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