select distinct
    domain,
    metric_name,
    metric_type,
    drill_down_one,
    drill_down_two,
    visual_month,
    c_mm,
    cy,
    f_mm,
    fy,
    prev_fy,
    prev_cy,
    max(c_day) over (partition by domain,
                metric_name,
                metric_type,
                visual_month,
                c_mm,
                cy,
                prev_cy,
                metric_id) as c_day,
    max(f_day) over (partition by domain,
                metric_name,
                metric_type,
                visual_month,
                f_mm,
                fy,
                prev_fy,
                metric_id) as f_day,
    metric_id
from
    {{ ref('stg_scorecard_data_up_to_date') }}
/* Prevents empty rows created where drill_down_two = overall or drill_down_one */
where drill_down_two not in ('overall', 'drill_down_one')
