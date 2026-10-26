{#
  A custom *generic* test: reusable and parameterised, applied from YAML like built-in tests.
  A test passes when its query returns zero rows.
#}
{% test non_negative(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}