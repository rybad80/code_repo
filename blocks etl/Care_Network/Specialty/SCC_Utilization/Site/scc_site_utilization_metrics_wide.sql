{%- set agg_columns = [
    "RESERVED_ROOMS",
    "USED_ROOMS",
    "RESERVED_NOT_USED",
    "UTILIZED_PERCENT",
    "RNU_PERCENT",
    "NOT_USED_BY_SITE_PERCENT"
    ] -%}
{%- set quarters = [
    1,
    2,
    3,
    4
    ] -%}

select
    location_name,
    site,
    total_rooms,
    fiscal_year,
    session_type,
    {%- for a in agg_columns%}
    {%- for q in quarters %}
    avg(case when fiscal_quarter = '{{q}}' then {{a}} end) as "Q{{q}}_{{a}}"
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
    from {{ref('scc_site_utilization_metrics')}}
group by
    location_name,
    site,
    total_rooms,
    fiscal_year,
    session_type
