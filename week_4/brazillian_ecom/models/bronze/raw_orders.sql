select *
from {{ source('brazil_ecom', 'olist_orders_dataset') }}
