/*
stg_nursing_staff_p3_cc_jgrp
align the gains and losses together so the next SQL can roll it together
per cost center/job group for the net change upcoming metrics
*/

select
    incoming_fte.metric_dt_key,
    incoming_fte.cost_center_id,
    incoming_fte.job_group_id,
    incoming_fte.numerator as date_cc_job_group_fte
from
    {{ ref('stg_nursing_staff_p1_incoming_fte') }} as incoming_fte

union all

select
    transfer_vacancy_fte.metric_dt_key,
    transfer_vacancy_fte.cost_center_id,
    transfer_vacancy_fte.job_group_id,
    transfer_vacancy_fte.numerator as date_cc_job_group_fte
from
    {{ ref('stg_nursing_staff_p2_outgoing_fte') }} as transfer_vacancy_fte
