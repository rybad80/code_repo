with stage as (
    select
        'Oncology Unit Admissions' as metric_name,
        postdatemonthyear as post_date_month_year,
        costcenterdescription as cost_center_description,
        substring(postdatefy from 4 for 4) as f_yyyy,
        {{
        dbt_utils.surrogate_key([
            'temporary_sl_statistics.postdatemonthyear',
            'temporary_sl_statistics.costcenterdescription'
            ])
        }} as primary_key
    from 
        {{ source('manual_ods', 'temporary_sl_statistics') }}
        inner join {{ ref('lookup_cost_center_service_line')}} as lookup_cost_center_service_line
            on  temporary_sl_statistics.costcenter = lookup_cost_center_service_line.cost_center_gl_id
    where
        lower(lookup_cost_center_service_line.service_line) = 'oncology'
        and lower(statistic) in ('admissions')
    group by
        post_date_month_year,
        cost_center_description,
        f_yyyy

),

budget as (
    select     
        temporary_sl_statistics.costcenterdescription as cost_center_description,       
        temporary_sl_statistics.postdatemonthyear as post_date_month_year, 
        sum(cast(stastisticmeasure as numeric)) as month_target
    from
        {{ source('manual_ods', 'temporary_sl_statistics') }} as temporary_sl_statistics
        inner join {{ ref('lookup_cost_center_service_line')}} as lookup_cost_center_service_line
            on  temporary_sl_statistics.costcenter = lookup_cost_center_service_line.cost_center_gl_id
    where
        lower(lookup_cost_center_service_line.service_line) = 'oncology'
        and lower(timeclass) = 'budget'
        and lower(statistic) in ('admissions')
    group by
        temporary_sl_statistics.costcenterdescription,
        temporary_sl_statistics.postdatemonthyear
),

actual as (
    select
        temporary_sl_statistics.costcenterdescription as cost_center_description,
        temporary_sl_statistics.postdatemonthyear as post_date_month_year,
        sum(cast(stastisticmeasure as numeric)) as admissions
    from
        {{ source('manual_ods', 'temporary_sl_statistics') }} as temporary_sl_statistics
        inner join {{ ref('lookup_cost_center_service_line')}} as lookup_cost_center_service_line
            on  temporary_sl_statistics.costcenter = lookup_cost_center_service_line.cost_center_gl_id
    where
        lower(lookup_cost_center_service_line.service_line) = 'oncology'
        and lower(timeclass) = 'actual'
        and lower(statistic) in ('admissions')
    group by
        temporary_sl_statistics.costcenterdescription,
        temporary_sl_statistics.postdatemonthyear
)

select
    stage.*,
    stage.cost_center_description as drill_down,
    date_trunc('month', stage.post_date_month_year) as visual_month,
    budget.month_target,
    actual.admissions,
    'count' as metric_type,
    'up' as desired_direction,
    'count' as metric_format
from
    stage
    left join budget
        on stage.post_date_month_year = budget.post_date_month_year
            and lower(stage.cost_center_description) = lower(budget.cost_center_description)
    left join actual
        on stage.post_date_month_year = actual.post_date_month_year
            and lower(stage.cost_center_description) = lower(actual.cost_center_description)
