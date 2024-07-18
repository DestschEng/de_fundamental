# from dagster import asset, Output, AssetIn
# import pandas as pd

# @asset(
#     ins={
#         "bronze_olist_products_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
#         "bronze_product_category_name_translation": AssetIn(key_prefix=["bronze", "ecom"]),
#     },
#     io_manager_key="minio_io_manager",
#     key_prefix=["silver", "ecom"],
#     compute_kind="Python"
# )
# def dim_products(bronze_olist_products_dataset, bronze_product_category_name_translation) -> Output[pd.DataFrame]:
#     df = bronze_olist_products_dataset.merge(
#         bronze_product_category_name_translation,
#         on="product_category_name",
#         how="left"
#     )
#     df = df[["product_id", "product_category_name_english"]]
#     return Output(
#         df,
#         metadata={
#             "table": "dim_products",
#             "records count": len(df),
#         },
#     )

# @asset(
#     ins={
#         "bronze_olist_orders_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
#         "bronze_olist_order_items_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
#         "bronze_olist_order_payments_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
#     },
#     io_manager_key="minio_io_manager",
#     key_prefix=["silver", "ecom"],
#     compute_kind="Python"
# )
# def fact_sales(bronze_olist_orders_dataset, bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset) -> Output[pd.DataFrame]:
#     df = bronze_olist_orders_dataset.merge(
#         bronze_olist_order_items_dataset,
#         on="order_id",
#         how="inner"
#     ).merge(
#         bronze_olist_order_payments_dataset,
#         on="order_id",
#         how="inner"
#     )
#     df = df[[
#         "order_id", "customer_id", "order_purchase_timestamp",
#         "product_id", "payment_value", "order_status"
#     ]]
#     return Output(
#         df,
#         metadata={
#             "table": "fact_sales",
#             "records count": len(df),
#         },
#     )
from dagster import asset, AssetIn, Output, DailyPartitionsDefinition
import pandas as pd

@asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
        "bronze_product_category_name_translation": AssetIn(key_prefix=["bronze", "ecom"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="Python"
)
def dim_products(context, bronze_olist_products_dataset, bronze_product_category_name_translation) -> Output[pd.DataFrame]:
    # Merge product data with category translation
    dim_products_df = pd.merge(
        bronze_olist_products_dataset,
        bronze_product_category_name_translation,
        on="product_category_name"
    )
    
    # Select relevant columns
    dim_products_df = dim_products_df[[
        'product_id',
        'product_category_name',
        'product_category_name_english',
        'product_name_lenght',
        'product_description_lenght',
        'product_photos_qty',
        'product_weight_g',
        'product_length_cm',
        'product_height_cm',
        'product_width_cm'
    ]]
    
    return Output(
        dim_products_df,
        metadata={
            "table": "dim_products",
            "records count": len(dim_products_df),
        },
    )

@asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
        "bronze_olist_order_items_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
        "bronze_olist_order_payments_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="Python",
    partitions_def=DailyPartitionsDefinition(start_date="2016-09-04")
)
def fact_sales(context, bronze_olist_orders_dataset, bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset) -> Output[pd.DataFrame]:
    partition_date = context.asset_partition_key_for_output()
    
    # Filter orders for the current partition
    orders_df = bronze_olist_orders_dataset[pd.to_datetime(bronze_olist_orders_dataset['order_purchase_timestamp']).dt.date == pd.to_datetime(partition_date).date()]
    
    # Merge datasets
    fact_sales_df = pd.merge(orders_df, bronze_olist_order_items_dataset, on="order_id")
    fact_sales_df = pd.merge(fact_sales_df, bronze_olist_order_payments_dataset, on="order_id")
    
    # Select relevant columns
    fact_sales_df = fact_sales_df[[
        'order_id',
        'customer_id',
        'order_status',
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date',
        'product_id',
        'seller_id',
        'price',
        'freight_value',
        'payment_sequential',
        'payment_type',
        'payment_installments',
        'payment_value'
    ]]
    
    return Output(
        fact_sales_df,
        metadata={
            "table": "fact_sales",
            "records count": len(fact_sales_df),
            "partition": partition_date,
        },
    )