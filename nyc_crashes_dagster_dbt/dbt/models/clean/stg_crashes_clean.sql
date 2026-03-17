select
    *,
    {{ dbt_utils.generate_surrogate_key([
        'crash_date', 'crash_time', 'location', 'zip_code',
        'latitude', 'longitude',
        'on_street_name', 'cross_street_name', 'off_street_name'
    ]) }} as row_hash
from {{ ref('stg_crashes') }}
where
    collision_id is not null
    and crash_date is not null
    and crash_time is not null
    and (persons_injured     is null or (persons_injured     >= 0 and persons_injured     <= 1000))
    and (persons_killed      is null or (persons_killed      >= 0 and persons_killed      <= 1000))
    and (pedestrians_injured is null or (pedestrians_injured >= 0 and pedestrians_injured <= 1000))
    and (pedestrians_killed  is null or (pedestrians_killed  >= 0 and pedestrians_killed  <= 1000))
    and (cyclist_injured     is null or (cyclist_injured     >= 0 and cyclist_injured     <= 1000))
    and (cyclist_killed      is null or (cyclist_killed      >= 0 and cyclist_killed      <= 1000))
    and (motorist_injured    is null or (motorist_injured    >= 0 and motorist_injured    <= 1000))
    and (motorist_killed     is null or (motorist_killed     >= 0 and motorist_killed     <= 1000))
