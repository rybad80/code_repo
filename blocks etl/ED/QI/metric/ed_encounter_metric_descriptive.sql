with
chief_complaint as (

select
    cohort.visit_key,
    master_reason_for_visit.rsn_nm
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
	inner join {{ source('cdw', 'visit_reason') }} as visit_reason
        on visit_reason.visit_key = cohort.visit_key
	inner join {{ source('cdw', 'master_reason_for_visit') }} as master_reason_for_visit
        on master_reason_for_visit.rsn_key = visit_reason.rsn_key
where
    visit_reason.seq_num = 1
),

team as (

select distinct
    visit_ed_area.visit_key,
    first_value(
        ed_area.ed_area_nm
    ) over (partition by visit_ed_area.visit_key order by visit_ed_area.seq_num) as arrival_team,
    first_value(
        ed_area.ed_area_nm
    ) over (partition by visit_ed_area.visit_key order by visit_ed_area.seq_num desc) as discharge_team
from
    {{ source('cdw', 'visit_ed_area') }} as visit_ed_area
    inner join {{ source('cdw', 'ed_area') }} as ed_area
        on visit_ed_area.ed_area_key = ed_area.ed_area_key
where
    lower(ed_area.ed_area_nm) not like 'chop ed waiting%'
),

revisit_detail as (

select
    cohort.pat_key,
    cohort.visit_key,
    cohort.arrive_ed_dt,
    cohort.disch_ed_dt,
    revisit.visit_key as revisit_key,
    revisit.arrive_ed_dt as revisit_arrive_ed_dt,
    days_between(cohort.disch_ed_dt, revisit.arrive_ed_dt) as days_to_revisit,
    case when stg_encounter_inpatient.visit_key is not null
    then 1 else 0 end as admitted_revisit_ind,
    case
        when stg_encounter_inpatient.visit_key is not null
        then days_between(cohort.disch_ed_dt, revisit.arrive_ed_dt)
    end as days_to_readmit
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
    inner join {{ref('stg_ed_encounter_cohort_all')}} as revisit    on revisit.pat_key = cohort.pat_key
    left join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
        on stg_encounter_inpatient.visit_key = revisit.visit_key
where
    revisit.arrive_ed_dt between cohort.disch_ed_dt
        and date(cohort.arrive_ed_dt) + cast('90 days' as interval)

),

revisit_summary as (

 select
    visit_key,
    max(case when days_to_revisit <= 3 then 1 else 0 end) as revisit_72_hr,
    max(case when days_to_readmit <= 3 then 1 else 0 end) as readmit_72_hr,
    max(case when days_to_revisit <= 7 then 1 else 0 end) as revisit_7_day,
    max(case when days_to_readmit <= 7 then 1 else 0 end) as readmit_7_day,
    max(case when days_to_revisit <= 14 then 1 else 0 end) as revisit_14_day,
    max(case when days_to_readmit <= 14 then 1 else 0 end) as readmit_14_day,
    max(case when days_to_revisit <= 30 then 1 else 0 end) as revisit_30_day,
    max(case when days_to_readmit <= 30 then 1 else 0 end) as readmit_30_day,
    max(case when days_to_revisit <= 90 then 1 else 0 end) as revisit_90_day,
    max(case when days_to_readmit <= 90 then 1 else 0 end) as readmit_90_day
 from
    revisit_detail
 group by
    visit_key,
    pat_key
),

pcp_visit_detail as (

select
    cohort.pat_key,
    cohort.visit_key,
    cohort.arrive_ed_dt,
    cohort.disch_ed_dt,
    pcp_visit.visit_key as pcp_visit_key,
    pcp_visit.appointment_date as pcp_arrive_dt,
    days_between(cohort.disch_ed_dt, pcp_visit.appointment_date) as days_to_visit
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
    inner join {{ ref('encounter_primary_care') }} as pcp_visit
        on pcp_visit.pat_key = cohort.pat_key
where
    pcp_visit.appointment_date between cohort.disch_ed_dt
        and date(cohort.arrive_ed_dt) + cast('90 days' as interval)

),

pcp_visit_summary as (

 select
    visit_key,
    max(case when days_to_visit <= 3 then 1 else 0 end) as pcp_visit_72_hr,
    max(case when days_to_visit <= 7 then 1 else 0 end) as pcp_visit_7_day,
    max(case when days_to_visit <= 14 then 1 else 0 end) as pcp_visit_14_day,
    max(case when days_to_visit <= 30 then 1 else 0 end) as pcp_visit_30_day,
    max(case when days_to_visit <= 90 then 1 else 0 end) as pcp_visit_90_day
 from
    pcp_visit_detail
 group by
    visit_key,
    pat_key
),

cohort_revisit_detail as (
select
    cohort.visit_key,
    cohort.pat_key,
    min(
        days_between(cohort.arrive_ed_dt, revisit_edqi.arrive_ed_dt)
    ) as cohort_days_to_revisit
    from
        {{ ref('ed_encounter_cohort_long') }} as ed_encounter_cohort_long
        inner join {{ref('stg_ed_encounter_cohort_all')}} as cohort
            on cohort.visit_key = ed_encounter_cohort_long.visit_key
        inner join {{ ref('ed_encounter_cohort_long') }} as revisit
            on revisit.pat_key = ed_encounter_cohort_long.pat_key
            and revisit.cohort = ed_encounter_cohort_long.cohort
        inner join {{ref('stg_ed_encounter_cohort_all')}} as revisit_edqi
            on revisit.visit_key = revisit_edqi.visit_key
    where
        revisit_edqi.arrive_ed_dt between cohort.disch_ed_dt
        and date(cohort.arrive_ed_dt) + cast('90 days' as interval)
        and revisit.cohort not in ('ED_SEEN', 'ED_ALL')
        and ed_encounter_cohort_long.cohort not in ('ED_SEEN', 'ED_ALL')
    group by
        cohort.visit_key,
        cohort.pat_key
),

cohort_revisit_summary as (
    select
        visit_key,
        max(case when cohort_days_to_revisit <= 3 then 1 else 0 end) as revisit_cohort_72_hr,
        max(case when cohort_days_to_revisit <= 7 then 1 else 0 end) as  revisit_cohort_7_day,
        max(case when cohort_days_to_revisit <= 14 then 1 else 0 end) as  revisit_cohort_14_day,
        max(case when cohort_days_to_revisit <= 30 then 1 else 0 end) as  revisit_cohort_30_day,
        max(case when cohort_days_to_revisit <= 90 then 1 else 0 end) as  revisit_cohort_90_day
    from
        cohort_revisit_detail
    group by
        visit_key,
        pat_key
),

first_ed_attend_raw as (
    select
        visit_ed_event.visit_key,
        visit_ed_event.event_dt,
        regexp_replace(visit_ed_event.event_cmt, 'assigned as Attending', '', 1, 1) as ed_attend,
        row_number() over (partition by visit_ed_event.visit_key order by visit_ed_event.event_dt,
                                                                          visit_ed_event.pat_event_id,
                                                                          visit_ed_event.seq_num
                                                                          ) as ed_prov
    from
        {{ source('cdw', 'visit_ed_event') }} as visit_ed_event
        inner join
            {{ source('cdw', 'master_event_type') }} as master_event_type    on
                visit_ed_event.event_type_key = master_event_type.event_type_key
    where
        master_event_type.event_id = 111
),

first_ed_attend as (
    select
        visit_key,
        initcap(regexp_extract(lower(ed_attend), '(.*?),\s[a-zA-z]')) as first_ed_attend
    from
        first_ed_attend_raw
    where
        ed_prov = 1
),

short_stay_admit as (

select
    encounter_ed.visit_key,
    case when encounter_ed.hospital_discharge_date is not null
        then round(
            extract(
                epoch from--noqa: L028
                    encounter_ed.hospital_discharge_date - encounter_ed.inpatient_admit_date)
                / 60.0 / 60.0,
            2
        )
        else
            round(
                (
                    (
                        extract(
                            epoch from timestamp(current_date) --noqa: L028
                            - encounter_ed.inpatient_admit_date
                        ) - 1
                    ) / 60.0
                ) / 60.0,
                2
            )
        end as ip_los_hrs,
    case
        when time(encounter_ed.inpatient_admit_date) between '00:00:00' and '06:00:00' then 1 else 0
    end as adm_night_ind,
    case when encounter_ed.hospital_discharge_date is not null
        then date(
            encounter_ed.hospital_discharge_date
        ) - date(encounter_ed.inpatient_admit_date) + adm_night_ind --noqa: L028
        else current_date - 1 - date(encounter_ed.inpatient_admit_date) + adm_night_ind --noqa: L028
        end as count_ip_nights,
    case
        when
            (
                ip_los_hrs <= 24 or count_ip_nights <= 1 --noqa: L028
            ) and encounter_ed.hospital_discharge_date is not null
        then 1 else 0 end as short_stay_admit_ind
from
    {{ ref('encounter_ed') }} as encounter_ed

)

select
    cohort.visit_key,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    fact_edqi.initial_ed_department_center_abbr as campus,
    case
        when lower(dict_acuity.dict_nm) in ('1 critical', 'sort 1') then '1 Critical'
        when lower(dict_acuity.dict_nm) in ('2 acute', 'sort 2') then '2 Acute'
        when lower(dict_acuity.dict_nm) in ('3 urgent', 'sort 3') then '3 Urgent'
        when lower(dict_acuity.dict_nm) in ('4 urgent', 'sort 4') then '4 Urgent'
        when lower(dict_acuity.dict_nm) in ('5 non-urgent', 'sort 5') then '5 Non-Urgent'
        else 'Indeterminate'
        end as patient_acuity,
    case when fact_edqi.age_at_visit < 0 then 0 else fact_edqi.age_at_visit end as age_at_visit,
    case when fact_edqi.age_days_at_visit < 0 then 0 else fact_edqi.age_days_at_visit end as age_days_at_visit,
    fact_edqi.ed_los,
    case when fact_edqi.edecu_los > 0 then fact_edqi.edecu_los else null end as edecu_los,
    fact_edqi.ed_disposition,
    fact_edqi.ed_general_disposition,
    fact_edqi.ed_resuscitation_rm_use_ind,
    coalesce(stg_encounter_ed.edecu_ind, 0) as edecu_ind,
    case when stg_encounter.patient_class_id = 5 --Observation
        then 1 else 0 end as observation_ind,
    coalesce(stg_encounter_ed.inpatient_ind, 0) as inpatient_ind,
    coalesce(stg_encounter_ed.icu_ind, 0) as icu_ind,
    stg_encounter_ed.admission_department_name,
    extract(
        epoch from stg_encounter_ed.hospital_discharge_date - stg_encounter_ed.inpatient_admit_date
    ) / 86400.0 as ip_los_days,
    coalesce(stg_encounter_ed.complex_chronic_condition_ind, 0) as complex_chronic_condition_ind,
    coalesce(revisit_72_hr, 0) as revisit_72_hr,
    coalesce(readmit_72_hr, 0) as readmit_72_hr,
    coalesce(pcp_visit_72_hr, 0) as pcp_visit_72_hr,
    coalesce(revisit_cohort_72_hr, 0) as revisit_cohort_72_hr,
    coalesce(revisit_7_day, 0) as revisit_7_day,
    coalesce(readmit_7_day, 0) as readmit_7_day,
    coalesce(pcp_visit_7_day, 0) as pcp_visit_7_day,
    coalesce(revisit_cohort_7_day, 0) as revisit_cohort_7_day,
    coalesce(revisit_14_day, 0) as revisit_14_day,
    coalesce(readmit_14_day, 0) as readmit_14_day,
    coalesce(pcp_visit_14_day, 0) as pcp_visit_14_day,
    coalesce(revisit_cohort_14_day, 0) as revisit_cohort_14_day,
    coalesce(revisit_30_day, 0) as revisit_30_day,
    coalesce(readmit_30_day, 0) as readmit_30_day,
    coalesce(pcp_visit_30_day, 0) as pcp_visit_30_day,
    coalesce(revisit_cohort_30_day, 0) as revisit_cohort_30_day,
    coalesce(revisit_90_day, 0) as revisit_90_day,
    coalesce(readmit_90_day, 0) as readmit_90_day,
    coalesce(pcp_visit_90_day, 0) as pcp_visit_90_day,
    coalesce(revisit_cohort_90_day, 0) as revisit_cohort_90_day,
    fact_edqi.pediatric_age_days_group,
    dict_arrvl_mode.dict_nm as arrival_mode,
    chief_complaint.rsn_nm as chief_complaint,
    team.arrival_team,
    team.discharge_team,
    stg_encounter.sex,
    stg_encounter_payor.payor_group,
    stg_encounter.patient_address_zip_code,
    stg_patient.race_ethnicity,
    stg_patient.preferred_language,
    stg_encounter_ed.ed_visit_language,
    stg_encounter_ed.ed_visit_language_comment,
    social_vulnerability_index.overall_category as svi_category,
    social_vulnerability_index.ses_category as svi_ses_category,
    first_ed_attend.first_ed_attend,
    short_stay_admit.short_stay_admit_ind,
    coalesce(stg_ed_encounter_metric_descriptive_rn_standing.rn_standing_standard_orders_placed,
             0) as rn_standing_standard_orders_placed,
    coalesce(stg_ed_encounter_metric_descriptive_rn_standing.rn_standing_standard_orders_signed,
             0) as rn_standing_standard_orders_signed,
    coalesce(stg_ed_encounter_metric_descriptive_rn_standing.rn_standing_triage_orders_placed,
             0) as rn_standing_triage_orders_placed,
    coalesce(stg_ed_encounter_metric_descriptive_rn_standing.rn_standing_triage_orders_signed,
             0) as rn_standing_triage_orders_signed,
    coalesce(stg_ed_encounter_metric_descriptive_rn_standing.rn_standing_pathway_orders_placed,
             0) as rn_standing_pathway_orders_placed,
    coalesce(stg_ed_encounter_metric_descriptive_rn_standing.rn_standing_pathway_orders_signed,
             0) as rn_standing_pathway_orders_signed,
    stg_encounter_ed.primary_care_location
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
    inner join {{ source('cdw_analytics', 'fact_edqi') }} as fact_edqi
        on fact_edqi.visit_key = cohort.visit_key
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.visit_key = cohort.visit_key
    left join {{ ref('stg_encounter_ed') }} as stg_encounter_ed
        on stg_encounter_ed.visit_key = cohort.visit_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_patient.pat_key = cohort.pat_key
    inner join {{ source('cdw', 'visit_addl_info') }} as visit_addl_info
        on visit_addl_info.visit_key = cohort.visit_key
    left join {{ source('cdw', 'cdw_dictionary') }} as dict_acuity
        on dict_acuity.dict_key = fact_edqi.dict_acuity_key
    left join {{ source('cdw', 'cdw_dictionary') }} as dict_arrvl_mode
        on dict_arrvl_mode.dict_key = visit_addl_info.dict_arrvl_mode_key
    left join chief_complaint
        on chief_complaint.visit_key = cohort.visit_key
    left join team on team.visit_key = cohort.visit_key
    left join {{ source('cdw', 'patient_geographical_spatial_info') }} as patient_geographical_spatial_info
        on stg_encounter.pat_key = patient_geographical_spatial_info.pat_key
        and patient_geographical_spatial_info.seq_num = stg_encounter.patient_address_seq_num
    left join
        {{ source('cdw', 'census_tract') }} as census_tract
            on patient_geographical_spatial_info.census_tract_key = census_tract.census_tract_key
    left join {{ source('cdc_ods', 'social_vulnerability_index') }} as social_vulnerability_index
        on census_tract.fips = social_vulnerability_index.fips
    left join revisit_summary
        on revisit_summary.visit_key = cohort.visit_key
    left join pcp_visit_summary
        on pcp_visit_summary.visit_key = cohort.visit_key
    left join cohort_revisit_summary
        on cohort_revisit_summary.visit_key = cohort.visit_key
    left join first_ed_attend
        on first_ed_attend.visit_key = cohort.visit_key
    left join short_stay_admit
        on short_stay_admit.visit_key = cohort.visit_key
    left join {{ref('stg_ed_encounter_metric_descriptive_rn_standing')}} as stg_ed_encounter_metric_descriptive_rn_standing --noqa: L016
      on cohort.visit_key = stg_ed_encounter_metric_descriptive_rn_standing.visit_key
    left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_payor.visit_key = stg_encounter.visit_key
