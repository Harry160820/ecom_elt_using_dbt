{#
  Generic test: the given columns must be unique *together* (a composite key).
  We write it ourselves so the project needs no external packages (and no network at build time).
#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}