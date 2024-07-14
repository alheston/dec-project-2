select 
    {{ dbt_utils.generate_surrogate_key(['state_id']) }} as state_sk,
    state_id,
    state_name,
    state_abbr,
    state_region
from {{ ref('stg_us_states') }}
