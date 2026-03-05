select *
from {{ ref('stg_crashes') }}
where
    collision_id is null
    or crash_date is null
    or crash_time is null
    or (persons_injured     is not null and (persons_injured     < 0 or persons_injured     > 1000))
    or (persons_killed      is not null and (persons_killed      < 0 or persons_killed      > 1000))
    or (pedestrians_injured is not null and (pedestrians_injured < 0 or pedestrians_injured > 1000))
    or (pedestrians_killed  is not null and (pedestrians_killed  < 0 or pedestrians_killed  > 1000))
    or (cyclist_injured     is not null and (cyclist_injured     < 0 or cyclist_injured     > 1000))
    or (cyclist_killed      is not null and (cyclist_killed      < 0 or cyclist_killed      > 1000))
    or (motorist_injured    is not null and (motorist_injured    < 0 or motorist_injured    > 1000))
    or (motorist_killed     is not null and (motorist_killed     < 0 or motorist_killed     > 1000))
