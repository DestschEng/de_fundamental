select *
from {{ source('brazil_ecom', 'olist_products_dataset') }}
