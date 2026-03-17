{{ config(materialized='incremental', unique_key='collision_id') }}

select
    s.collision_id,
    s.crash_date,
    s.crash_time,
    s.location,
    s.zip_code,
    s.latitude,
    s.longitude,
    s.on_street_name,
    s.cross_street_name,
    s.off_street_name,
    s.row_hash,
    {% if is_incremental() %}
    coalesce(e.loaded_at, s.loaded_at) as loaded_at,
    case when e.collision_id is not null then s.loaded_at end as updated_at,
    {% else %}
    s.loaded_at,
    null::timestamptz as updated_at,
    {% endif %}
    s._src_file
from {{ ref('stg_crashes_clean') }} s
{% if is_incremental() %}
left join {{ this }} e on e.collision_id = s.collision_id
where s.row_hash != coalesce(e.row_hash, '')
{% endif %}
