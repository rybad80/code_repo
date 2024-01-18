/*
summary: unique patient in cohort, including patients listed in epic diabetes icd registry
who had at least one visit to endo for diabetes and had at least one key field from
icr flowsheets completed in a rolling 15-month.
exclusion: exclude inpatients (admitted hospital) or ed patients who admitted and diagnosed with diabetes
but have never seen in endo departments diabetes care center (dcc)
(these patients are included in diabetes_t1y1 if they had a valid documentation in t1y1 ip flo 9386)
granularity level: patient level + monthly reporting level
notes:flag all indicators based on active pop in monthly reporting point (1st date of every month since 07/01/2013)
    including t1y1 program indicator and t2y1 program indicator.

last updated: 8/31/22
*/

with cohort_endo_visit as ( --ignore enc type that hasn't been assigned with appt_stat
select
    pat_key,
    visit_key,
    endo_vis_dt,
    provider_nm,
    prov_type,
    visit_type_nm,
    enc_type,
    appt_stat,
    enc_rn,
    last_md_vis_dt,
    last_np_vis_dt,
    last_edu_vis_dt
from
    {{ref('diabetes_visit_cohort_historical')}}
where
    lower(appt_stat) in ('arrived', 'completed')
),

active_pop as (--define active patient population over every 15 months since the first reporting point.
    select
        --the monthly reporting point should cover patients in last 15 months with
        --icr flowsheets completed/edited:
        master_date.full_dt as report_point,
        stg_diabetes_icr_active_flowsheets.pat_key
    from
        {{ source('cdw', 'master_date')}} as master_date
        inner join {{ref('diabetes_icr_active_flowsheets_historical')}} as stg_diabetes_icr_active_flowsheets
            on stg_diabetes_icr_active_flowsheets.recorded_date
            between master_date.full_dt - interval('15 month') and master_date.full_dt - interval('1 day')
    where
        --icr flowhseets has launched since 2012, started reporting period since 2013, validated by the team:
        master_date.full_dt between '2013-07-01' and current_date
        and master_date.day_of_mm = 1 --pull first date of month
    group by
        report_point,
        stg_diabetes_icr_active_flowsheets.pat_key
),

most_recent_endo_vis as (    --most recent endo enc per report point, when enc_rn = 1
    select
        active_pop.report_point,
        active_pop.pat_key,
        row_number() over (partition by active_pop.report_point, active_pop.pat_key
            order by endo_vis_dt desc) as enc_rn,
        visit_type_nm,
        enc_type,
        provider_nm,
        prov_type,
        endo_vis_dt,
        last_md_vis_dt,
        last_np_vis_dt,
        last_edu_vis_dt
    from
        active_pop
        inner join {{ref('stg_cohort_endo_visit_historical')}} as cohort_endo_visit
            on active_pop.pat_key = cohort_endo_visit.pat_key
    where
        endo_vis_dt < report_point
        --only need the most recent visit info per reporting point (every 15 months):
        and endo_vis_dt >= report_point - interval('2 year')
),

stg_flowsheets as (--summary of key fields from the diabetes icr flowsheet that determined the patient denominator
    select
        cohort_endo_visit.pat_key,
        cohort_endo_visit.visit_key,
        date(recorded_date) as recorded_date,
        meas_val,
        fs_type
    from
        cohort_endo_visit
        inner join {{ref('diabetes_icr_active_flowsheets_historical')}} as stg_diabetes_icr_active_flowsheets
            on stg_diabetes_icr_active_flowsheets.visit_key = cohort_endo_visit.visit_key
),

diab_type as (    --type of diabetes
    select
        active_pop.report_point,
        active_pop.pat_key,
        row_number() over (partition by active_pop.report_point, active_pop.pat_key
            order by stg_flowsheets.recorded_date desc) as fs_rn,
        stg_flowsheets.recorded_date,
        meas_val as diab_type
    from
        active_pop
        inner join stg_flowsheets
            on active_pop.pat_key = stg_flowsheets.pat_key
            and recorded_date < report_point
            and recorded_date >= report_point - interval('2 year')
    where
        lower(fs_type) = 'diab_type'
),

last_diab_type as (    --most recent recorded diab_type
    select --noqa
        *
    from
        diab_type
    where
        fs_rn = 1
),

dx_date as (--date of diagnosis
    select
        active_pop.report_point,
        active_pop.pat_key,
        min(cast(date('1840-12-31') + cast(meas_val as int) as varchar(16))) as dx_date,
        year(dx_date) as dx_year,
        round(months_between(report_point, dx_date) / 12) as duration_year
    from
        active_pop
        inner join stg_flowsheets
            on active_pop.pat_key = stg_flowsheets.pat_key
            and recorded_date < report_point
            and recorded_date >= report_point - interval('2 year')
    where
        lower(fs_type) = 'dx date'
    group by
        active_pop.report_point,
        active_pop.pat_key
),

dx_year as (    --year of diagnosis
    select
        active_pop.report_point,
        active_pop.pat_key,
        min(cast(meas_val as int)) as dx_year,
        (year(report_point) - dx_year) as duration_year
    from
        active_pop
        inner join stg_flowsheets
            on active_pop.pat_key = stg_flowsheets.pat_key
            and recorded_date < report_point
            and recorded_date >= report_point - interval('2 year')
    where
        lower(fs_type) = 'endo date'
    group by
        active_pop.report_point,
        active_pop.pat_key
),

fs_prov as (    --primary diabetes provider (np)
    select
        active_pop.report_point,
        active_pop.pat_key,
        row_number() over (partition by active_pop.report_point, active_pop.pat_key
            order by stg_flowsheets.recorded_date desc) as fs_rn,
        meas_val as fs_prov
    from
        active_pop
        inner join stg_flowsheets
            on active_pop.pat_key = stg_flowsheets.pat_key
            and recorded_date < report_point
            and recorded_date >= report_point - interval('2 year')
    where
        lower(fs_type) = 'np'
),

fs_prov_last as (     --most recent np per patient
    select --noqa
        *
    from
        fs_prov
    where
        fs_rn = 1
),

fs_team as (
    select
        active_pop.report_point,
        active_pop.pat_key,
        row_number() over (partition by active_pop.report_point, active_pop.pat_key
            order by stg_flowsheets.recorded_date desc) as fs_rn,
        case
            when lower(meas_val) in ( 'philly- monday meerkats',
                            'philly- tuesday turtles',
                            'philly- wednesday wallabies',
                            'philly- thursday tigers')
                then 'buerger'
            when lower(meas_val) != 'team not assigned' and meas_val is not null
                then 'satellite'
            else 'team not assigned' end as team_group,
        case
            when lower(meas_val) != 'team not assigned' and meas_val is not null
                then meas_val
            else 'team not assigned' end as team_detail
    from
        active_pop
        inner join stg_flowsheets
            on active_pop.pat_key = stg_flowsheets.pat_key
            and recorded_date < report_point
            and recorded_date >= report_point - interval('2 year')
    where
        fs_type = 'team'
),

fs_team_last as (     --most recent team per patient
    select
        *
    from
        fs_team
    where
        fs_rn = 1
),

control_risk_score as (-- link risk rule_id back to unique patient (use pat_key, becasue visit_key is 0)
    select
        active_pop.report_point,
        registry_data_info.pat_key,
        metric_string_value as control_risk_score,
        row_number() over (partition by active_pop.report_point, registry_data_info.pat_key
            order by metric_last_upd_dt desc) as risk_rn,
        metric_last_upd_dt
    from
        active_pop
        inner join {{ source('cdw', 'registry_data_info')}} as registry_data_info
            on active_pop.pat_key = registry_data_info.pat_key
        inner join {{ source('cdw', 'registry_metric_history')}} as registry_metric_history
            on registry_data_info.record_key = registry_metric_history.record_key
    where
        mstr_chrg_edit_rule_key = 85014 --diabetes control risk score (rule_id == registry_metric_id = '1016517')
        and metric_last_upd_dt < report_point
        and metric_last_upd_dt >= report_point - interval('2 year')
),

complications_risk_score as (-- link risk rule_id back to unique patient
    select
        active_pop.report_point,
        registry_data_info.pat_key,
        metric_string_value as complications_risk_score,
        row_number() over (partition by active_pop.report_point, registry_data_info.pat_key
            order by metric_last_upd_dt desc) as risk_rn,
        metric_last_upd_dt
    from
        active_pop
        inner join {{ source('cdw', 'registry_data_info')}} as registry_data_info
            on active_pop.pat_key = registry_data_info.pat_key
        inner join {{ source('cdw', 'registry_metric_history')}} as registry_metric_history
            on registry_data_info.record_key = registry_metric_history.record_key
    where
        mstr_chrg_edit_rule_key = 78730
            --diabetes complications risk score (rule_id == registry_metric_id = '1015742')
        and metric_last_upd_dt < report_point
        and metric_last_upd_dt >= report_point - interval('2 year')
),

visit_indicators as (
    select
        active_pop.report_point,
        active_pop.pat_key,
        max(case when active_pop.report_point - interval('15 month') <= most_recent_endo_vis.last_md_vis_dt
                then 1 else 0 end) as last_15mo_md_visit_ind, --patient has an md visit in the past 15 months
        max(case when active_pop.report_point - interval('4 month') <= most_recent_endo_vis.last_md_vis_dt
                or active_pop.report_point - interval('4 month') <= most_recent_endo_vis.last_np_vis_dt
                then 1 else 0 end) as last_4mo_mdnp_visit_ind, --patient has an md/np visit in the past 4 months
        max(case when active_pop.report_point - interval('15 month') <= most_recent_endo_vis.last_edu_vis_dt
                then 1 else 0 end) as last_15mo_edu_visit_ind--patient has an education visit in the past 15 months
    from
        active_pop
        inner join most_recent_endo_vis
            on most_recent_endo_vis.pat_key = active_pop.pat_key
            and most_recent_endo_vis.report_point = active_pop.report_point
    where
        most_recent_endo_vis.enc_rn = 1
    group by
        active_pop.report_point,
        active_pop.pat_key
)
select
    active_pop.report_point,
    active_pop.pat_key,
    patient_all.mrn,
    patient_all.patient_name,
    patient_all.dob,
    most_recent_endo_vis.visit_type_nm as last_visit_type,
    most_recent_endo_vis.enc_type as last_enc_type,
    most_recent_endo_vis.provider_nm as last_prov,
    most_recent_endo_vis.prov_type as last_prov_type,
    most_recent_endo_vis.endo_vis_dt as last_visit_date,
    visit_indicators.last_15mo_md_visit_ind,
    visit_indicators.last_4mo_mdnp_visit_ind,
    visit_indicators.last_15mo_edu_visit_ind,
    case when (
            --onset as ip at chop:
            (diabetes_t1y1.ip_diag_ind = 1 and diabetes_t1y1.new_diabetes_dt
                between active_pop.report_point - interval('15 month') and active_pop.report_point - 1)
            or  --noqa
            --transferred to chop:
            (diabetes_t1y1.new_transfer_ind = 1 and date(dx_date.dx_date)
                between active_pop.report_point - interval('15 month') and active_pop.report_point - 1)
            )
            and lower(last_diab_type.diab_type) in ('antibody negative type 1',
                                            'antibody positive type 1',
                                            'type 1 unknown antibody status')
            then 1 else 0 end as t1y1_ind,
    case when (
            --onset as ip at chop:
            (diabetes_t1y1.ip_diag_ind = 1 and diabetes_t1y1.new_diabetes_dt
                between active_pop.report_point - interval('15 month') and active_pop.report_point - 1)
            or --noqa
            --transferred to chop:
            (diabetes_t1y1.new_transfer_ind = 1 and date(dx_date.dx_date)
                between active_pop.report_point - interval('15 month') and active_pop.report_point - 1)
            )
            and lower(last_diab_type.diab_type) in ('type 2')
            then 1 else 0 end as t2y1_ind,
    last_diab_type.diab_type as diab_type,    --last edit on report point
    case when date(dx_date.dx_date) > date(diabetes_t1y1.new_diabetes_dt)
        then diabetes_t1y1.new_diabetes_dt
        else date(dx_date.dx_date) end as first_dx_date,
    coalesce(dx_date.dx_year, dx_year.dx_year) as first_dx_year,
    coalesce(dx_date.duration_year, dx_year.duration_year) as dx_duration_year,
    fs_team_last.team_group as last_seen_team_group,
    fs_team_last.team_detail as last_seen_team_detail,
    fs_prov_last.fs_prov as last_seen_np,
--    fs_team_most_freq.team_group as most_freq_team_group,
--    fs_team_most_freq.team_detail as most_freq_team_detail,
--    fs_prov_most_freq.fs_prov as most_freq_np,
    control_risk_score,
    complications_risk_score,
    patient_all.payor_group --most recent
from
    active_pop
    --join cohort_endo_visit on cohort_endo_visit.pat_key = active_pop.pat_key
    --left join stg_flowsheets     on stg_flowsheets.visit_key = cohort_endo_visit.visit_key
    left join most_recent_endo_vis
        on most_recent_endo_vis.pat_key = active_pop.pat_key
        and most_recent_endo_vis.report_point = active_pop.report_point
        and most_recent_endo_vis.enc_rn = 1
    left join visit_indicators
        on visit_indicators.pat_key = active_pop.pat_key
        and visit_indicators.report_point = active_pop.report_point
    left join fs_prov_last
        on fs_prov_last.pat_key = active_pop.pat_key
        and fs_prov_last.report_point = active_pop.report_point
    left join fs_team_last
        on fs_team_last.pat_key = active_pop.pat_key
        and fs_team_last.report_point = active_pop.report_point
    left join last_diab_type
        on last_diab_type.pat_key = active_pop.pat_key
        and last_diab_type.report_point = active_pop.report_point
    left join dx_date
        on dx_date.pat_key = active_pop.pat_key
        and dx_date.report_point = active_pop.report_point
    left join dx_year
        on dx_year.pat_key = active_pop.pat_key
        and dx_year.report_point = active_pop.report_point
    left join control_risk_score
        on control_risk_score.pat_key = active_pop.pat_key
        and control_risk_score.report_point = active_pop.report_point
        and control_risk_score.risk_rn = 1
    left join complications_risk_score
        on complications_risk_score.pat_key = active_pop.pat_key
        and complications_risk_score.report_point = active_pop.report_point
        and complications_risk_score.risk_rn = 1
    left join {{ref('patient_all' ) }} as patient_all
        on patient_all.pat_key = active_pop.pat_key
    left join {{ref('diabetes_t1y1_historical')}} as diabetes_t1y1
        on diabetes_t1y1.pat_key = active_pop.pat_key   --patiet-level
