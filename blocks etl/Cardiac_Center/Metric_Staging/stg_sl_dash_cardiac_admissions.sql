with stage as (
    select
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
    where
        lower(temporary_sl_statistics.costcenterdescription) in ('cardiology', 'cardiac care unit', 'cicu')
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
    where
        lower(timeclass) = 'budget'
        and lower(temporary_sl_statistics.costcenterdescription) in ('cardiology', 'cardiac care unit', 'cicu')
        and lower(statistic) in ('admissions')
    group by
        cost_center_description,
        post_date_month_year
),

actual as (
    select
        temporary_sl_statistics.costcenterdescription as cost_center_description,
        temporary_sl_statistics.postdatemonthyear as post_date_month_year,
        sum(cast(stastisticmeasure as numeric)) as admissions
    from
        {{ source('manual_ods', 'temporary_sl_statistics') }} as temporary_sl_statistics
    where
        lower(timeclass) = 'actual'
        and lower(temporary_sl_statistics.costcenterdescription) in ('cardiology', 'cardiac care unit', 'cicu')
        and lower(statistic) in ('admissions')
    group by
        post_date_month_year,
        cost_center_description
)

select
    stage.*,
    stage.cost_center_description as drill_down,
    date_trunc('month', stage.post_date_month_year) as visual_month,
    budget.month_target,
    actual.admissions,
    'cardiac_unit_adm' as metric_id
from
    stage
    left join budget
        on stage.post_date_month_year = budget.post_date_month_year 
            and lower(stage.cost_center_description) = lower(budget.cost_center_description)
    left join actual
        on stage.post_date_month_year = actual.post_date_month_year 
            and lower(stage.cost_center_description) = lower(actual.cost_center_description)
