select *
from {{ source('brazil_ecom', 'olist_order_payments_dataset') }}
