with customer_data as (
    select 
        c.customer_id,
        c.company_name,
        c.contact_name,
        c.contact_title,
        c.address,
        c.city,
        c.region,
        c.postal_code,
        c.country,
        c.phone,
        c.fax
    from {{ ref('stg_customers') }} c
),

customer_demo as (
    select
        ccd.customer_id,
        cd.customer_desc
    from {{ ref('stg_customer_customer_demo') }} ccd
    join {{ ref('stg_customer_demographics') }} cd
    on ccd.customer_type_id = cd.customer_type_id
)

select 
    {{ dbt_utils.generate_surrogate_key(['cd.customer_id']) }} as customer_sk,
    cd.customer_id,
    cd.company_name,
    cd.contact_name,
    cd.contact_title,
    cd.address,
    cd.city,
    cd.region,
    cd.postal_code,
    cd.country,
    cd.phone,
    cd.fax,
    array_agg(cdem.customer_desc) as customer_descriptions
from customer_data cd
left join customer_demo cdem
on cd.customer_id = cdem.customer_id
group by 
    cd.customer_id,
    cd.company_name,
    cd.contact_name,
    cd.contact_title,
    cd.address,
    cd.city,
    cd.region,
    cd.postal_code,
    cd.country,
    cd.phone,
    cd.fax
