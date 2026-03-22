select
    *,
    'person_type is not a valid value: ' || person_type as reason
from {{ ref('stg_persons') }}
where person_type is not null
  and person_type not in ('bicyclist', 'occupant', 'other motorized', 'pedestrian')

union all

select
    *,
    'person_injury is not a valid value: ' || person_injury as reason
from {{ ref('stg_persons') }}
where person_injury is not null
  and person_injury not in ('injured', 'killed')
