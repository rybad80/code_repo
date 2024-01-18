with
grouped_naki_med_admins as (
    /* the granularity of this CTE needs to be one row per visit / per day / per med */
    select
        visit_key,
        date(administration_date) as action_date,
        ntmx_grouper
    from
        {{ ref('stg_naki_med_admins') }}
    group by
        visit_key,
        action_date,
        ntmx_grouper
)

/* a subset of meds need to counted for 7 days after administration toward exposure.
We'll fake it -- create admin rows for each of 7 days after the initial admin */
{% for days_to_add in [1, 2, 3, 4, 5, 6] %}
select
    visit_key,
    action_date + {{ days_to_add }} as action_date,
    ntmx_grouper
from
    grouped_naki_med_admins
where
    ntmx_grouper in (
        'cidofovir',
        'gadopentetate',
        'gadoextate',
        'iodixanol',
        'iohexol',
        'iopamidol',
        'ioversol'
    )
union
{% endfor %}

/* and union back in the original med list for the complete med list */
select
    visit_key,
    action_date,
    ntmx_grouper
from
    grouped_naki_med_admins
