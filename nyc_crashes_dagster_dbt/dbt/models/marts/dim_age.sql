select
    person_age,
    case
        when person_age < 18 then '<18'
        when person_age < 25 then '18-24'
        when person_age < 35 then '25-34'
        when person_age < 45 then '35-44'
        when person_age < 55 then '45-54'
        when person_age < 65 then '55-64'
        else                      '65+'
    end as age_band
from generate_series(0, 130) as t(person_age)
