{% test is_valid_zip(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} !~ '^[0-9]{5}$'
{% endtest %}
