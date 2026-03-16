select
    age_band,
    age_min,
    age_max,
    sort_order
from (values
    ('unknown', null, null, 0),
    ('<18',        0,   17, 1),
    ('18-24',     18,   24, 2),
    ('25-34',     25,   34, 3),
    ('35-44',     35,   44, 4),
    ('45-54',     45,   54, 5),
    ('55-64',     55,   64, 6),
    ('65+',       65,  999, 7)
) as t(age_band, age_min, age_max, sort_order)
