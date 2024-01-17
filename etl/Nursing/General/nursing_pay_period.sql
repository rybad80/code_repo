{{ config(meta = {
    'critical': true
}) }}
/* nursing_pay_period
Capture the pay periods and various labels and indicators so we know if
data is past/actual (considered prior once the Kronos data is approved) or
upcoming (not finalized).
Kronos data is considered final on the Tuesday after a Saturday which
happens to be a pay period end.  So the interim Sunday and Monday
are in a limbo state of prior by date but not final (still marked future/
upcoming for metric purposes) and on these two days only, that one row only
will have prior_not_final_ind = 1.
*/
with
next_schedule_period as (
    select
        min(schedule_end_dt_key) as next_schedule_period_end_dt_key
    from
        {{ ref('stg_nursing_pp_p2_pay_period') }}
    where schedule_start_dt
        > (select pp_end_dt from {{ ref('stg_nursing_pp_p2_pay_period') }}
            where current_working_pay_period_ind = 1)
)

select
    pp_and_schedule_rows.prior_fiscal_year_ind,
    pp_and_schedule_rows.current_fiscal_year_ind,
    pp_and_schedule_rows.next_fiscal_year_ind,
    pp_and_schedule_rows.fiscal_year,
    pp_and_schedule_rows.pp_fiscal_year_label,
    pp_and_schedule_rows.pay_period_number,
    pp_and_schedule_rows.pp_start_dt_key,
    pp_and_schedule_rows.pp_end_dt_key,
    pp_and_schedule_rows.pp_label,
    pp_and_schedule_rows.pp_label_with_fy,
    pp_and_schedule_rows.schedule_start_dt_key,
    pp_and_schedule_rows.schedule_end_dt_key,
    pp_and_schedule_rows.prior_pay_period_ind,
    pp_and_schedule_rows.prior_not_final_ind,
    pp_and_schedule_rows.future_pay_period_ind,
    case when pp_and_schedule_rows.recrank_latestpp in (1, 2, 3)
        and pp_and_schedule_rows.prior_pay_period_ind = 1 then 1
            else 0 end as latest_complete_3_pay_periods_ind,
    pp_and_schedule_rows.latest_pay_period_ind,
    pp_and_schedule_rows.current_working_pay_period_ind,
    case when pp_and_schedule_rows.recrank_upcomingpp in (1, 2, 3)
        and pp_and_schedule_rows.future_pay_period_ind = 1
            then 1 else 0 end as upcoming_3_pay_periods_ind,
    case when pp_and_schedule_rows.schedule_end_dt_key
        = next_sched.next_schedule_period_end_dt_key
        then 1 else 0 end as next_schedule_period_ind,
    pp_and_schedule_rows.pp_start_dt,
    pp_and_schedule_rows.pp_end_dt,
    pp_and_schedule_rows.schedule_start_dt,
    pp_and_schedule_rows.schedule_end_dt,
    pp_and_schedule_rows.schedule_fiscal_year,
    pp_and_schedule_rows.schedule_fiscal_year_label,
    pp_and_schedule_rows.recrank_schedperiod || '~'
        || pp_and_schedule_rows.schedule_end_dt as schedule_num_label,
    alphas.alpha_label || '~' || pp_and_schedule_rows.schedule_end_dt as schedule_alpha_label,
    to_char(pp_and_schedule_rows.schedule_start_dt, 'Mon') || '-'
        || to_char(pp_and_schedule_rows.schedule_end_dt, 'Mon') as schedule_month_label,
    schedule_month_label || ' '
        || to_char(pp_and_schedule_rows.schedule_end_dt, 'mm/dd')
            as schedule_month_label_with_end_date,
    schedule_month_label_with_end_date || ' '
        || pp_and_schedule_rows.schedule_fiscal_year_label
            as schedule_month_label_with_end_date_fy,
    pp_and_schedule_rows.schedule_date_range,
    pp_and_schedule_rows.prior_schedule_fy_ind,
    pp_and_schedule_rows.current_schedule_fy_ind,
    pp_and_schedule_rows.next_schedule_fy_ind,
    pp_and_schedule_rows.nursing_operations_window_ind,
    case /* take whatever (and only) future look nusing ops wants
              and all past pay periods selected for nccs */
        when nursing_operations_window_ind = 1
        then pp_and_schedule_rows.nccs_platform_window_ind
        when pp_and_schedule_rows.prior_pay_period_ind = 1
        then pp_and_schedule_rows.nccs_platform_window_ind
        else 0
        end as nccs_platform_window_ind,
    pp_and_schedule_rows.pp_quarter_label,
    pp_and_schedule_rows.pp_quarter_sort,
    pp_and_schedule_rows.pp_quarter_label_fy,
    pp_and_schedule_rows.pp_fy_month_num,
    pp_and_schedule_rows.pp_month_label,
    pp_and_schedule_rows.pp_month_sort,
    pp_and_schedule_rows.pp_month_label_fy,
    pp_and_schedule_rows.pp_month,
    pp_and_schedule_rows.pp_fy_yyyy_mm_nm,
    pp_and_schedule_rows.back_1_pp_ind,
    pp_and_schedule_rows.back_2_pp_ind,
    pp_and_schedule_rows.back_3_pp_ind,
    pp_and_schedule_rows.latest_26_pp_ind,
    pp_and_schedule_rows.latest_13_pp_ind,
    pp_and_schedule_rows.final_pp_of_month_ind,
    pp_and_schedule_rows.final_pp_of_year_ind,
    pp_and_schedule_rows.selected_pp_dt_label,
    pp_and_schedule_rows.pp_end_dt_label_short,
    pp_and_schedule_rows.fy_pp_end_dt_label,
    stg_nursing_pp_p3_holiday.holiday_count,
    stg_nursing_pp_p3_holiday.all_employee_holiday_count,
    stg_nursing_pp_p3_holiday.union_holiday_count,
    stg_nursing_pp_p3_holiday.pp_holiday_note,
    case
        when pp_and_schedule_rows.current_working_pay_period_ind = 1 then 'Current working pay period'
        when pp_and_schedule_rows.latest_pay_period_ind = 1 then 'Latest pay period'
        when pp_and_schedule_rows.back_1_pp_ind = 1 then 'Back 1 prior pay period'
        when pp_and_schedule_rows.back_2_pp_ind = 1 then 'Back 2 prior pay periods'
        when pp_and_schedule_rows.back_3_pp_ind = 1 then 'Back 3 prior pay periods'
        when pp_and_schedule_rows.prior_not_final_ind = 1 then 'Awaiting payroll approval'
        when pp_and_schedule_rows.future_pay_period_ind = 1
            and pp_and_schedule_rows.current_fiscal_year_ind = 1
            and pp_and_schedule_rows.current_working_pay_period_ind = 0
            then 'Pay period not completed yet'
        when pp_and_schedule_rows.future_pay_period_ind = 1
            and pp_and_schedule_rows.current_fiscal_year_ind = 0
            then 'Future'
        else ''
    end
    || case
        when (pp_and_schedule_rows.current_working_pay_period_ind + pp_and_schedule_rows.latest_pay_period_ind
            + pp_and_schedule_rows.back_1_pp_ind + pp_and_schedule_rows.back_2_pp_ind
            + pp_and_schedule_rows.back_3_pp_ind + pp_and_schedule_rows.prior_not_final_ind
            + pp_and_schedule_rows.future_pay_period_ind >= 1)
            and pp_and_schedule_rows.current_fiscal_year_ind != 1
        then ', '
        else ''
    end
    || case
        when pp_and_schedule_rows.current_fiscal_year_ind != 1
        then pp_and_schedule_rows.pp_fiscal_year_label
        else ''
    end
    || case
        when ((pp_and_schedule_rows.current_working_pay_period_ind + pp_and_schedule_rows.latest_pay_period_ind
            + pp_and_schedule_rows.back_1_pp_ind + pp_and_schedule_rows.back_2_pp_ind
            + pp_and_schedule_rows.back_3_pp_ind + pp_and_schedule_rows.prior_not_final_ind
            + pp_and_schedule_rows.future_pay_period_ind >= 1)
            or pp_and_schedule_rows.current_fiscal_year_ind != 1)
            and (stg_nursing_pp_p3_holiday.holiday_count >= 1)
        then ', '
        else ''
    end
    || coalesce(stg_nursing_pp_p3_holiday.pp_holiday_note, '')
    || case
        when ((pp_and_schedule_rows.current_working_pay_period_ind + pp_and_schedule_rows.latest_pay_period_ind
            + pp_and_schedule_rows.back_1_pp_ind + pp_and_schedule_rows.back_2_pp_ind
            + pp_and_schedule_rows.back_3_pp_ind + pp_and_schedule_rows.prior_not_final_ind
            + pp_and_schedule_rows.future_pay_period_ind >= 1)
            or pp_and_schedule_rows.current_fiscal_year_ind != 1
            or stg_nursing_pp_p3_holiday.holiday_count >= 1)
            and (pp_and_schedule_rows.final_pp_of_month_ind + pp_and_schedule_rows.final_pp_of_year_ind >= 1)
        then ', '
        else ''
    end
    || case
        when pp_and_schedule_rows.final_pp_of_month_ind = 1
        then 'Last PP of month'
        else ''
    end
    || case
        when ((pp_and_schedule_rows.current_working_pay_period_ind + pp_and_schedule_rows.latest_pay_period_ind
            + pp_and_schedule_rows.back_1_pp_ind + pp_and_schedule_rows.back_2_pp_ind
            + pp_and_schedule_rows.back_3_pp_ind + pp_and_schedule_rows.prior_not_final_ind
            + pp_and_schedule_rows.future_pay_period_ind + pp_and_schedule_rows.final_pp_of_month_ind >= 1)
            or (stg_nursing_pp_p3_holiday.holiday_count >= 1))
            and (pp_and_schedule_rows.final_pp_of_year_ind = 1)
        then ', '
        else ''
    end
    || case
        when pp_and_schedule_rows.final_pp_of_year_ind = 1
        then 'Last PP of FY'
        else ''
    end as pp_additional_note,
    pp_and_schedule_rows.selected_pp_dt_label
    || case
        when trim(coalesce(pp_additional_note, '')) > ''
        then ' (' || pp_additional_note || ')'
        else ''
    end as selected_pp_dt_label_long
from
    {{ ref('stg_nursing_pp_p2_pay_period') }} as pp_and_schedule_rows
left join next_schedule_period as next_sched
    on pp_and_schedule_rows.schedule_end_dt_key = next_sched.next_schedule_period_end_dt_key
left join {{ ref('lookup_number_alpha_convert') }} as alphas
    on pp_and_schedule_rows.recrank_schedperiod = alphas.number_to_convert
left join {{ ref('stg_nursing_pp_p3_holiday') }} as stg_nursing_pp_p3_holiday
    on pp_and_schedule_rows.pp_start_dt_key = stg_nursing_pp_p3_holiday.pp_start_dt_key
    and pp_and_schedule_rows.pp_end_dt_key = stg_nursing_pp_p3_holiday.pp_end_dt_key
where
    pp_and_schedule_rows.prior_pay_period_ind = 1
    or pp_and_schedule_rows.nursing_operations_window_ind = 1
/* order by pp_and_schedule_rows.pp_end_dt_key desc */
