select *
from {{ source('brazil_ecom', 'olist_order_items_dataset') }}