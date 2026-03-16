select
    {{ dbt_utils.generate_surrogate_key(['vehicle_type', 'vehicle_make', 'vehicle_model']) }} as dim_vehicle_id,
    vehicle_type,
    vehicle_make,
    vehicle_model
from (
    select distinct
        vehicle_type,
        vehicle_make,
        vehicle_model
    from {{ ref('crashed_vehicle') }}
) as vehicles
