select *
from {{ source('northwind', 'us_states') }}