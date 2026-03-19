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
),

standardized as (
    select
        d.collision_id, d.crash_date, d.crash_time, d.borough, d.zip_code,
        d.latitude, d.longitude, d.location,
        {{ replace_street_suffix('d.on_street_name', 'sa_on') }}       as on_street_name,
        {{ replace_street_suffix('d.cross_street_name', 'sa_cross') }} as cross_street_name,
        {{ replace_street_suffix('d.off_street_name', 'sa_off') }}     as off_street_name,
        d.contributing_factor_vehicle_1, d.contributing_factor_vehicle_2,
        d.contributing_factor_vehicle_3, d.contributing_factor_vehicle_4,
        d.contributing_factor_vehicle_5,
        d.vehicle_type_code_1, d.vehicle_type_code_2, d.vehicle_type_code_3,
        d.vehicle_type_code_4, d.vehicle_type_code_5,
        d.persons_injured, d.persons_killed,
        d.pedestrians_injured, d.pedestrians_killed,
        d.cyclist_injured, d.cyclist_killed,
        d.motorist_injured, d.motorist_killed,
        d.loaded_at, d._src_file
    from deduped d
    left join {{ ref('street_type_abbreviations') }} sa_on
        on (regexp_match(d.on_street_name, '(\S+)$'))[1] = sa_on.abbreviation
    left join {{ ref('street_type_abbreviations') }} sa_cross
        on (regexp_match(d.cross_street_name, '(\S+)$'))[1] = sa_cross.abbreviation
    left join {{ ref('street_type_abbreviations') }} sa_off
        on (regexp_match(d.off_street_name, '(\S+)$'))[1] = sa_off.abbreviation
    where d.rn = 1
)

select * from standardized
