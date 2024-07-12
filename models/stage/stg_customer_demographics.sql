select 
    customer_type_id,
    customer_desc
from {{ source('northwind', 'customer_demographics') }}