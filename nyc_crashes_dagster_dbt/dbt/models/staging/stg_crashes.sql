{% set core_crashes = adapter.get_relation(
    database=target.database, schema='core', identifier='crashes') %}

with source_data as (
    select
        {{ dbt.safe_cast('collision_id', 'bigint') }}                       as collision_id,
        {{ dbt.safe_cast('crash_date', 'date') }}                           as crash_date,
        {{ dbt.safe_cast('crash_time', 'time') }}                           as crash_time,
        nullif(trim(lower(borough)), '')                                    as borough,
        nullif(trim(lower(zip_code)), '')                                   as zip_code,
        {{ dbt.safe_cast('latitude', 'numeric') }}                          as latitude,
        {{ dbt.safe_cast('longitude', 'numeric') }}                         as longitude,
        nullif(trim(lower(location)), '')                                   as location,
        nullif(trim(lower(on_street_name)), '')                             as on_street_name,
        nullif(trim(lower(cross_street_name)), '')                          as cross_street_name,
        nullif(trim(lower(off_street_name)), '')                            as off_street_name,
        nullif(trim(lower(contributing_factor_vehicle_1)), '')              as contributing_factor_vehicle_1,
        nullif(trim(lower(contributing_factor_vehicle_2)), '')              as contributing_factor_vehicle_2,
        nullif(trim(lower(contributing_factor_vehicle_3)), '')              as contributing_factor_vehicle_3,
        nullif(trim(lower(contributing_factor_vehicle_4)), '')              as contributing_factor_vehicle_4,
        nullif(trim(lower(contributing_factor_vehicle_5)), '')              as contributing_factor_vehicle_5,
        nullif(trim(lower(vehicle_type_code_1)), '')                        as vehicle_type_code_1,
        nullif(trim(lower(vehicle_type_code_2)), '')                        as vehicle_type_code_2,
        nullif(trim(lower(vehicle_type_code_3)), '')                        as vehicle_type_code_3,
        nullif(trim(lower(vehicle_type_code_4)), '')                        as vehicle_type_code_4,
        nullif(trim(lower(vehicle_type_code_5)), '')                        as vehicle_type_code_5,
        {{ dbt.safe_cast('number_of_persons_injured', 'integer') }}         as persons_injured,
        {{ dbt.safe_cast('number_of_persons_killed', 'integer') }}          as persons_killed,
        {{ dbt.safe_cast('number_of_pedestrians_injured', 'integer') }}     as pedestrians_injured,
        {{ dbt.safe_cast('number_of_pedestrians_killed', 'integer') }}      as pedestrians_killed,
        {{ dbt.safe_cast('number_of_cyclist_injured', 'integer') }}         as cyclist_injured,
        {{ dbt.safe_cast('number_of_cyclist_killed', 'integer') }}          as cyclist_killed,
        {{ dbt.safe_cast('number_of_motorist_injured', 'integer') }}        as motorist_injured,
        {{ dbt.safe_cast('number_of_motorist_killed', 'integer') }}         as motorist_killed,
        loaded_at,
        _src_file
    from {{ source('raw', 'crashes') }}
    {% if core_crashes is not none %}
    where loaded_at > coalesce(
        (select max(loaded_at) from {{ core_crashes }}),
        '1900-01-01'::timestamptz
    )
    {% endif %}
),

deduped as (
    select
        *,
        row_number() over (
            partition by collision_id
            order by loaded_at desc
        ) as rn
    from source_data
)

select
    collision_id, crash_date, crash_time, borough, zip_code,
    latitude, longitude, location,
    on_street_name, cross_street_name, off_street_name,
    contributing_factor_vehicle_1, contributing_factor_vehicle_2,
    contributing_factor_vehicle_3, contributing_factor_vehicle_4,
    contributing_factor_vehicle_5,
    vehicle_type_code_1, vehicle_type_code_2, vehicle_type_code_3,
    vehicle_type_code_4, vehicle_type_code_5,
    persons_injured, persons_killed,
    pedestrians_injured, pedestrians_killed,
    cyclist_injured, cyclist_killed,
    motorist_injured, motorist_killed,
    loaded_at,
    _src_file
from deduped
where rn = 1
