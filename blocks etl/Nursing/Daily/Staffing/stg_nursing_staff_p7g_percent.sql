/* stg_nursing_staff_p7g_percent
part 7 step g by cost center calculate the % new RNs */

select
    'RNLastYrNewHirePct' as metric_abbreviation,
    pct_denom.metric_dt_key,
    pct_denom.cost_center_id as cost_center_id,
    pct_denom.job_group_id,
    null as metric_grouper,
    coalesce(pct_numer.numerator, 0) as numerator,
    pct_denom.numerator as denominator,
    round(pct_numer.numerator
        / pct_denom.numerator, 3) as row_metric_calculation
from
    {{ ref('stg_nursing_staff_p7e_hire_total') }} as pct_denom
    left join {{ ref('stg_nursing_staff_p7e_hire_total') }} as pct_numer
        on pct_denom.cost_center_id = pct_numer.cost_center_id
        and pct_numer.metric_abbreviation = 'LastYrNewHireCnt'
        and pct_numer.job_group_id = 'RN'
where
    pct_denom.metric_abbreviation = 'RNcurrentCnt'
    and pct_denom.numerator > 0
