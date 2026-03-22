select
    *,
    'person_type is not a valid value: ' || person_type as reason
from {{ ref('stg_persons') }}
where person_type is not null
  and person_type not in ('bicyclist', 'occupant', 'other motorized', 'pedestrian')
