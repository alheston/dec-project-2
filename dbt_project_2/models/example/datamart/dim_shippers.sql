with orders_data as (
    select
        distinct 
        ship_via,
        ship_name,
        ship_address,
        ship_city,
        ship_region,
        ship_postal_code,
        ship_country
    from {{ ref('stg_orders') }}
),

shippers_data as (
    select
        shipper_id,
        company_name as shipper_name,
        phone as shipper_phone
    from {{ ref('stg_shippers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['o.ship_via']) }} as shipper_sk,
    o.ship_via as shipper_id,
    s.shipper_name,
    s.shipper_phone,
    o.ship_name,
    o.ship_address,
    o.ship_city,
    o.ship_region,
    o.ship_postal_code,
    o.ship_country
from orders_data o
join shippers_data s
on o.ship_via = s.shipper_id
