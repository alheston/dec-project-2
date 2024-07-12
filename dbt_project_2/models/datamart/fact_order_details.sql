{{ config(
    materialized='table'
) }}

with orders_data as (
    select 
        order_id,
        customer_id,
        employee_id,
        order_date,
        required_date,
        shipped_date,
        ship_via as shipper_id
    from {{ ref('stg_orders') }}
),

order_details_data as (
    select
        order_id,
        product_id,
        unit_price,
        quantity,
        discount
    from {{ ref('stg_order_details') }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['od.order_id']) }} as order_sk,
    {{ dbt_utils.generate_surrogate_key(['od.product_id']) }} as product_sk,
    {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} as customer_sk,
    {{ dbt_utils.generate_surrogate_key(['o.employee_id']) }} as employee_sk,
    {{ dbt_utils.generate_surrogate_key(['o.shipper_id']) }} as shipper_sk,
    o.order_date,
    o.required_date,
    o.shipped_date,
    od.unit_price,
    od.quantity,
    od.discount
from order_details_data od
join orders_data o
on od.order_id = o.order_id
