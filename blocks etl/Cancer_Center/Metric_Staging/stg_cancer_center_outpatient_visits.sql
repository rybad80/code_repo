with stage as (
    select
        'Oncology Outpatient Visits' as metric_name,
        postdatemonthyear as post_date_month_year,
        costcentersitename,
        substring(postdatefy from 4 for 4) as f_yyyy,
        {{
        dbt_utils.surrogate_key([
            'temporary_sl_statistics.postdatemonthyear',
            'temporary_sl_statistics.costcentersitename'
            ])
        }} as primary_key
    from 
        {{ source('manual_ods', 'temporary_sl_statistics') }} as temporary_sl_statistics
        inner join {{ ref('lookup_cost_center_service_line')}} as lookup_cost_center_service_line
            on  temporary_sl_statistics.costcenter = lookup_cost_center_service_line.cost_center_gl_id
    where
        lower(lookup_cost_center_service_line.service_line) = 'oncology'
        and lower(statistic) in ('est outpatient physician visits','new outpatient physician visits','op visits')
    group by
        post_date_month_year,
        costcentersitename,
        f_yyyy

),

budget as (
    select     
        temporary_sl_statistics.costcentersitename,       
        temporary_sl_statistics.postdatemonthyear, 
        sum(cast(stastisticmeasure as numeric)) as month_target
    from
        {{ source('manual_ods', 'temporary_sl_statistics') }} as temporary_sl_statistics
        inner join {{ ref('lookup_cost_center_service_line')}} as lookup_cost_center_service_line
            on  temporary_sl_statistics.costcenter = lookup_cost_center_service_line.cost_center_gl_id
    where
        lower(lookup_cost_center_service_line.service_line) = 'oncology'
        and lower(timeclass) = 'budget'
        and lower(statistic) in ('est outpatient physician visits','new outpatient physician visits','op visits')
    group by
        temporary_sl_statistics.costcentersitename,
        postdatemonthyear
),

actual as (
    select
        temporary_sl_statistics.costcentersitename,
        temporary_sl_statistics.postdatemonthyear,
        sum(cast(stastisticmeasure as numeric)) as outpatient_visits
    from
        {{ source('manual_ods', 'temporary_sl_statistics') }} as temporary_sl_statistics
        inner join {{ ref('lookup_cost_center_service_line')}} as lookup_cost_center_service_line
            on  temporary_sl_statistics.costcenter = lookup_cost_center_service_line.cost_center_gl_id
    where
        lower(lookup_cost_center_service_line.service_line) = 'oncology'
        and lower(timeclass) = 'actual'
        and lower(statistic) in ('est outpatient physician visits','new outpatient physician visits','op visits')
    group by
        temporary_sl_statistics.costcentersitename,
        postdatemonthyear
)

select
    stage.*,
    stage.costcentersitename as drill_down,
    date_trunc('month', stage.post_date_month_year) as visual_month,
    budget.month_target,
    actual.outpatient_visits,
    'count' as metric_type,
    'up' as desired_direction,
    'count' as metric_format
from
    stage
    left join budget
        on stage.post_date_month_year = budget.postdatemonthyear
            and lower(stage.costcentersitename) = lower(budget.costcentersitename)
    left join actual
        on stage.post_date_month_year = actual.postdatemonthyear
            and lower(stage.costcentersitename) = lower(actual.costcentersitename)
