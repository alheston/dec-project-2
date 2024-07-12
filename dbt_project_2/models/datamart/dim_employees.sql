with employee_data as (
    select 
        employee_id,
        last_name,
        first_name,
        title,
        title_of_courtesy,
        birth_date,
        hire_date,
        address,
        city,
        region,
        postal_code,
        country,
        home_phone,
        extension,
        photo,
        notes,
        reports_to,
        photo_path
    from {{ ref('stg_employees') }}
),

employee_territories_data as (
    select 
        employee_id,
        array_agg(territory_id) as territory_ids
    from {{ ref('stg_employee_territories') }}
    group by employee_id
)

select
    {{ dbt_utils.generate_surrogate_key(['e.employee_id']) }} as employee_sk,
    e.employee_id,
    e.last_name,
    e.first_name,
    e.title,
    e.title_of_courtesy,
    e.birth_date,
    e.hire_date,
    e.address,
    e.city,
    e.region,
    e.postal_code,
    e.country,
    e.home_phone,
    e.extension,
    e.photo,
    e.notes,
    e.reports_to,
    e.photo_path,
    et.territory_ids
from employee_data e
left join employee_territories_data et
on e.employee_id = et.employee_id

 