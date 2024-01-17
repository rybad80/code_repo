with fy_seq_num as (
    select distinct
        metric_id,
        fy,
        fy_seq_num
    from
        {{ ref('scorecard_metrics') }}
)

select
    stg_scorecard_data.metric_name,
    stg_scorecard_data.visual_month,
    stg_scorecard_data.fy,
    stg_scorecard_data.fy_qtr,
    stg_scorecard_data.cy,
    stg_scorecard_data.cy_qtr,
    stg_scorecard_data.domain,
    stg_scorecard_data.metric_type,
    case
        when stg_scorecard_data.drill_down_one = 'No drilldown available' then null
        else stg_scorecard_data.drill_down_one
    end as drill_down_one,
    case
        when stg_scorecard_data.drill_down_two = 'No drilldown available' then null
        else stg_scorecard_data.drill_down_two
    end as drill_down_two,
    stg_scorecard_data.num_calculation,
    stg_scorecard_data.denom_calculation,
    stg_scorecard_data.num,
    stg_scorecard_data.denom,
    fy_seq_num.fy_seq_num,
    stg_scorecard_data.primary_key,
    stg_scorecard_data.metric_id,
    stg_scorecard_data.metric_date
from
    {{ ref('stg_scorecard_data') }} as stg_scorecard_data
    inner join fy_seq_num
        on stg_scorecard_data.metric_id = fy_seq_num.metric_id
           and fy_seq_num.fy = stg_scorecard_data.fy
where
    drill_down_one != 'overall'
