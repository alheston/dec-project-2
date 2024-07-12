with territories_data as (
    select
        territory_id,
        territory_description,
        region_id
    from {{ ref('stg_territories') }}
),

region_data as (
    select
        region_id,
        region_description
    from {{ ref('stg_region') }}
)

select 
    t.territory_id,
    t.territory_description,
    t.region_id,
    r.region_description
from territories_data t
join region_data r
on t.region_id = r.region_id
