with row_seq as (
    select
        visual_month,
        metric_id,
        row_number() over (partition by fy, metric_id order by visual_month desc) as fy_month_seq_num,
        row_number() over (partition by cy, metric_id order by visual_month desc) as cy_month_seq_num
    from
        {{ref('stg_scorecard_data_stage_up_to_date')}}
    group by
        visual_month,
        metric_id,
        fy,
        cy
),
fy_seq as (
    select
        fy,
        metric_id,
        row_number() over (partition by metric_id order by fy desc) as fy_seq_num
    from
         {{ref('stg_scorecard_data_stage_up_to_date')}}
    group by
        metric_id,
        fy
),
cy_seq as (
    select
        cy,
        metric_id,
        row_number() over (partition by metric_id order by cy desc) as cy_seq_num
    from
         {{ref('stg_scorecard_data_stage_up_to_date')}}
    group by
        metric_id,
        cy
)

select
    stg_scorecard_data_stage_up_to_date.*,
    row_seq.fy_month_seq_num,
    row_seq.cy_month_seq_num,
    fy_seq.fy_seq_num,
    cy_seq.cy_seq_num
from
    {{ref('stg_scorecard_data_stage_up_to_date')}} as stg_scorecard_data_stage_up_to_date
    inner join row_seq
        on stg_scorecard_data_stage_up_to_date.visual_month = row_seq.visual_month
            and stg_scorecard_data_stage_up_to_date.metric_id = row_seq.metric_id
    inner join fy_seq
        on stg_scorecard_data_stage_up_to_date.fy = fy_seq.fy
            and stg_scorecard_data_stage_up_to_date.metric_id = fy_seq.metric_id
    inner join cy_seq
        on stg_scorecard_data_stage_up_to_date.cy = cy_seq.cy
            and stg_scorecard_data_stage_up_to_date.metric_id = cy_seq.metric_id
