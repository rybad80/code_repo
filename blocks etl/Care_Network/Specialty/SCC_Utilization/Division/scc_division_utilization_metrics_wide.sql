{%- set agg_columns = [
    "RESERVED_ROOMS",
    "USED_ROOMS",
    "RESERVED_NOT_USED",
    "UTILIZED_PERCENT",
    "RNU_PERCENT"
    ] -%}
{%- set quarters = [
    1,
    2,
    3,
    4
    ] -%}

select
    department_name,
    specialty,
    location_name,
    site,
    fiscal_year,
    session_type,
    {%- for a in agg_columns%}
    {%- for q in quarters %}
    avg(case when fiscal_quarter = '{{q}}' then {{a}} end) as "Q{{q}}_{{a}}"
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
    from {{ref('scc_division_utilization_metrics')}}
group by
    department_name,
    specialty,
    location_name,
    site,
    fiscal_year,
    session_type
