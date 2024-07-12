select 
    territory_id,
    territory_description,
    region_id
from {{ source('northwind', 'territories') }}