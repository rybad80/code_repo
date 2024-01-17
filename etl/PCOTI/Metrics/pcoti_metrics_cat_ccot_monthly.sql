with events as (
    select
        date_trunc('month', stg_pcoti_report_cat_ccot.event_start_date) as event_year_month,
        stg_pcoti_report_cat_ccot.campus_name,
        stg_pcoti_report_cat_ccot.department_group_name,
        sum(
            stg_pcoti_report_cat_ccot.ccot_cat_followup_36hrs_ind
        ) as numerator_ccot_cat_followup_36hrs,
        sum(
            stg_pcoti_report_cat_ccot.ccot_rnrt_cat_followup_36hrs_ind
        ) as numerator_ccot_rnrt_cat_followup_36hrs,
        sum(1) as denominator_cat_calls
    from
        {{ ref('stg_pcoti_report_cat_ccot') }} as stg_pcoti_report_cat_ccot
    group by
        date_trunc('month', stg_pcoti_report_cat_ccot.event_start_date),
        stg_pcoti_report_cat_ccot.campus_name,
        stg_pcoti_report_cat_ccot.department_group_name
)

select * from events
