{{ config(meta = {
    'critical': true
}) }}
/* stg_nursing_pp_p2_pay_period
for snippet efficiency set most INDs prepping for final nursing_pay_period
*/
with prior_next_sets as (
    select to_char(current_date - 3, 'yyyymmdd')::bigint as latest_approved_end_dt_key,
        f_yyyy as current_fiscal_year,
        f_yyyy + 1 as next_fiscal_year,
        f_yyyy - 1 as prior_fiscal_year
    from
        {{ source('cdw', 'master_date') }}
    where
        full_dt = current_date
)

    select
        case
            when master_pay_periods.fiscal_year = prior_next_sets.prior_fiscal_year
            then 1 else 0
            end as  prior_fiscal_year_ind,
        case
            when master_pay_periods.fiscal_year = prior_next_sets.current_fiscal_year
            then 1 else 0
            end as  current_fiscal_year_ind,
        case
            when master_pay_periods.fiscal_year = prior_next_sets.next_fiscal_year
            then 1 else 0
            end as  next_fiscal_year_ind,
        master_pay_periods.pay_period_key,
        master_pay_periods.fiscal_year,
        (substr(pp_ending_date.fy_yyyy, 1, 2) || chr(39)
            || substr(pp_ending_date.fy_yyyy, 6, 2)) as pp_fiscal_year_label,
        master_pay_periods.period as pay_period_number,
        master_pay_periods.start_dt_key as pp_start_dt_key,
        master_pay_periods.end_dt_key as pp_end_dt_key,
        'pp'  || master_pay_periods.period as pp_label,
        pp_label || ' ' || pp_fiscal_year_label as pp_label_with_fy,
        pp_fiscal_year_label || ' ' || pp_label as fy_pp_label,
        schedule_periods.schedule_start_dt_key,
        schedule_periods.schedule_end_dt_key,
        case when master_pay_periods.end_dt_key <= prior_next_sets.latest_approved_end_dt_key
            then 1 else 0 end as prior_pay_period_ind, /* must be finalized */
        case prior_pay_period_ind when 0 then case
            when current_date > pp_ending_date.full_dt then 1 else 0 end
            else 0 end as prior_not_final_ind, /* only occurs for Sun & Mon after a pp ends */
        case prior_pay_period_ind when 1 then 0
            else 1 end as future_pay_period_ind,
        master_date.full_dt as pp_start_dt,
        pp_ending_date.full_dt as pp_end_dt,
        'Q' || pp_ending_date.f_qtr as pp_quarter_label,
        (pp_ending_date.f_yyyy * 10) + pp_ending_date.f_qtr as pp_quarter_sort,
        pp_fiscal_year_label || ' q' || pp_ending_date.f_qtr as pp_quarter_label_fy,
        pp_ending_date.f_mm as pp_fy_month_num,
        replace(pp_ending_date.fy_yyyy_mm_nm, pp_ending_date.fy_yyyy || '-', '')
            as pp_month_label, /* (Jan, etc) */
        (pp_ending_date.f_yyyy  * 100) + pp_ending_date.f_mm as pp_month_sort,
        pp_fiscal_year_label || ' ' || pp_month_label as pp_month_label_fy, /*FY’yy mmm */
        pp_ending_date.month_nm as pp_month,
        pp_ending_date.fy_yyyy_mm_nm as pp_fy_yyyy_mm_nm, /* FY 2023-Sep */
        dense_rank() over (partition by pp_month_sort
            order by pp_end_dt_key desc) as recrank_latest_in_month,
        dense_rank() over (partition by master_pay_periods.fiscal_year
            order by pp_end_dt_key desc) as recrank_latest_in_fy,
        schedule_periods.schedule_start_dt,
        schedule_periods.schedule_end_dt,
        schedule_periods.schedule_fiscal_year,
        schedule_periods.schedule_fiscal_year_label,
        schedule_periods.schedule_date_range,
        schedule_periods.prior_fiscal_year_ind as prior_schedule_fy_ind,
        schedule_periods.current_fiscal_year_ind as current_schedule_fy_ind,
        schedule_periods.next_fiscal_year_ind as next_schedule_fy_ind,
        schedule_periods.nursing_operations_window_ind,
        schedule_periods.nccs_platform_window_ind,
        dense_rank() over (partition by future_pay_period_ind
            order by master_pay_periods.start_dt_key) as recrank_upcomingpp,
        dense_rank() over (partition by prior_pay_period_ind
            order by master_pay_periods.end_dt_key desc) as recrank_latestpp,
        dense_rank() over (partition by schedule_fiscal_year
            order by schedule_periods.schedule_end_dt_key) as recrank_schedperiod,
        case when current_date between pp_start_dt and pp_end_dt
            then 1 else 0 end as current_working_pay_period_ind,
        case when recrank_latestpp = 1 then 1 else 0 end as latest_pay_period_ind,
        case when recrank_latestpp = 2 then 1 else 0 end as back_1_pp_ind,
        case when recrank_latestpp = 3 then 1 else 0 end as back_2_pp_ind,
        case when recrank_latestpp = 4 then 1 else 0 end as back_3_pp_ind,
        case when recrank_latestpp <= 26 then 1 else 0 end as latest_26_pp_ind,
        case when recrank_latestpp <= 13 then 1 else 0 end as latest_13_pp_ind,
        case recrank_latest_in_month when 1 then 1 else 0 end as final_pp_of_month_ind,
        case recrank_latest_in_fy when 1 then 1 else 0 end as final_pp_of_year_ind,
        pp_label || ' ending ' ||  pp_ending_date.c_mm || '/' || pp_ending_date.day_of_mm
            || '/' || pp_ending_date.c_yy as selected_pp_dt_label,
        pp_label || ' ' ||  pp_ending_date.c_mm || '/' || pp_ending_date.day_of_mm
            || '/' || pp_ending_date.c_yy as pp_end_dt_label_short,
        fy_pp_label || ' ' ||  pp_ending_date.c_mm || '/' || pp_ending_date.day_of_mm
            || '/' || pp_ending_date.c_yy as fy_pp_end_dt_label
from
    {{ source('cdw', 'master_pay_periods') }} as master_pay_periods
    cross join prior_next_sets
    inner join {{ source('cdw', 'master_date') }} as master_date
        on master_pay_periods.start_dt_key = master_date.dt_key
    inner join {{ source('cdw', 'master_date') }} as pp_ending_date
        on master_pay_periods.end_dt_key = pp_ending_date.dt_key
    inner join {{ ref('stg_nursing_pp_p1_schedule_period') }} as schedule_periods
        on master_pay_periods.end_dt_key between schedule_periods.schedule_start_dt_key
            and schedule_periods.schedule_end_dt_key
/* order by master_pay_periods.end_dt_key */
