select
    hour::int                                          as crash_hour,
    to_char(
        ('2000-01-01'::date + (hour || ' hours')::interval),
        'HH12 AM'
    )                                                  as hour_label,
    case
        when hour between 0  and 5  then 'Night'
        when hour between 6  and 11 then 'Morning'
        when hour between 12 and 17 then 'Afternoon'
        else                             'Evening'
    end                                                as time_of_day_band,
    (hour between 7 and 9 or hour between 16 and 18)   as rush_hour_flag
from generate_series(0, 23) as t(hour)
