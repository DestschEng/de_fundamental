from dagster import asset, Output, AssetIn
import pandas as pd

@asset(
    ins={
        "dim_products": AssetIn(key_prefix=["silver", "ecom"]),
        "fact_sales": AssetIn(key_prefix=["silver", "ecom"]),
    },
    io_manager_key="psql_io_manager",
    key_prefix=["gold", "ecom"],
    compute_kind="Python"
)
def sales_values_by_category(dim_products, fact_sales) -> Output[pd.DataFrame]:
    # Xử lý dữ liệu theo yêu cầu
    daily_sales_products = fact_sales[fact_sales["order_status"] == "delivered"].groupby(
        [pd.to_datetime(fact_sales["order_purchase_timestamp"]).dt.date, "product_id"]
    ).agg({
        "payment_value": "sum",
        "order_id": "nunique"
    }).reset_index()
    daily_sales_products.columns = ["daily", "product_id", "sales", "bills"]
    
    daily_sales_categories = daily_sales_products.merge(
        dim_products,
        on="product_id",
        how="left"
    )
    daily_sales_categories["monthly"] = pd.to_datetime(daily_sales_categories["daily"]).dt.strftime("%Y-%m")
    daily_sales_categories["values_per_bills"] = daily_sales_categories["sales"] / daily_sales_categories["bills"]
    
    result = daily_sales_categories.groupby(["monthly", "product_category_name_english"]).agg({
        "sales": "sum",
        "bills": "sum",
    }).reset_index()
    result["values_per_bills"] = result["sales"] / result["bills"]
    
    return Output(
        result,
        metadata={
            "table": "sales_values_by_category",
            "records count": len(result),
        },
    )