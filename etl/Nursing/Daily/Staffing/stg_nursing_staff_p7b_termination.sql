/* stg_nursing_staff_p7b_termination
part 7 step b counts and FTEs with indicators for Workday terminations for last completed
twelve months and the current month for RNs, UAPs, LPNs
*/

select
    nursing_worker.worker_id,
    nursing_worker.cost_center_id,
    nursing_pay_period.pp_end_dt_key as month_pp_end_dt_key,
    1 as month_term_cnt,
    nursing_worker.full_time_equivalent_percentage / 100 as month_term_fte,
    nursing_worker.term_last_thirty_days_ind,
    nursing_worker.term_in_last_year_ind,
    coalesce(nursing_worker.hire_in_last_year_ind, 0) as hire_in_last_year_ind,
    coalesce(worker_employment_protected.termination_involuntary_ind, 0) as termination_involuntary_ind,
    worker_employment_protected.term_category,
    nursing_worker.rn_job_ind,
    nursing_worker.nursing_category_abbreviation,
    case nursing_worker.rn_job_ind when 1
        then 'RN'
        else nursing_worker.nursing_category_abbreviation end as job_group_id
from
    {{ ref('nursing_worker') }} as nursing_worker
    left join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on nursing_worker.recent_termination_month = date_trunc('month', nursing_pay_period.pp_end_dt)
        and nursing_pay_period.final_pp_of_month_ind = 1
    left join {{ ref('worker_employment_protected') }} as worker_employment_protected
        on nursing_worker.worker_wid = worker_employment_protected.worker_wid
where
    (nursing_worker.term_in_last_year_ind = 1 or nursing_pay_period.final_pp_of_month_ind = 1)
    and (nursing_worker.rn_job_ind = 1
        or nursing_worker.nursing_category_abbreviation in ('UAP', 'LPN'))
