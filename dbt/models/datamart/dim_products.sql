with product_data as (
    select 
        product_id,
        product_name,
        category_id,
        quantity_per_unit,
        unit_price,
        units_in_stock,
        units_on_order,
        reorder_level,
        ZEROIFNULL(discontinued) as discontinued
    from {{ ref('stg_products') }}
),

category_data as (
    select
        category_id,
        category_name,
        description as category_description
    from {{ ref('stg_categories') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_sk,
    p.product_id,
    p.product_name,
    p.category_id,
    c.category_name,
    c.category_description,
    p.quantity_per_unit,
    p.unit_price,
    p.units_in_stock,
    p.units_on_order,
    p.reorder_level,
    p.discontinued
from product_data p
left join category_data c
on p.category_id = c.category_id
