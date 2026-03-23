{{ config(materialized='incremental', unique_key='vehicle_damage_id') }}

with source as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'collision_id', 'vehicle_id', 'state_registration',
            'vehicle_type', 'vehicle_make', 'vehicle_model'
        ]) }}           as crashed_vehicle_id,
        vehicle_damage,
        vehicle_damage_1,
        vehicle_damage_2,
        vehicle_damage_3
    from {{ ref('stg_vehicles_clean') }}
),
unpivoted as (
    select crashed_vehicle_id, 0 as damage_position, vehicle_damage   as damages from source where vehicle_damage   is not null
    union all
    select crashed_vehicle_id, 1,                    vehicle_damage_1        from source where vehicle_damage_1 is not null
    union all
    select crashed_vehicle_id, 2,                    vehicle_damage_2        from source where vehicle_damage_2 is not null
    union all
    select crashed_vehicle_id, 3,                    vehicle_damage_3        from source where vehicle_damage_3 is not null
)
select
    {{ dbt_utils.generate_surrogate_key(['crashed_vehicle_id', 'damage_position']) }} as vehicle_damage_id,
    crashed_vehicle_id,
    damages
from unpivoted
