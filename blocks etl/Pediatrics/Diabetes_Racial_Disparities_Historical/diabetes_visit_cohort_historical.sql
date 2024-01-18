/*
summary: unique visit in cohort, including all endo visits for patients listed in epic diabetes icd registry
who had at least one visit to chop endo departments for diabetes.
time span: icr flowsheets has launched since 2012, including all encounters since 2011
exclusions:
    exclude visits for departments in inpatient (hospital) or ed
    exclude visits that have appointment status = 'canceled', 'cancelled', 'no show', or 'left without seen'
granularity level: visit level
notes: this is a fact table without any flags related to monthly reporting points
last updated: 6/20/22
*/

with last_visit as ( --rejoin to combine attributes for last md, np, and education visits
select distinct
    cohort.pat_key,
    cohort.visit_key,
    cohort.patient_name,
    cohort.endo_vis_dt,
    cohort.prov_type,
    cohort.last_endo_vis_dt,
     max(case when cohort.last_md_vis_dt < stg_cohort_endo_visit_last_md_vis.endo_vis_dt
        then cohort.last_md_vis_dt end) over (partition by cohort.pat_key) as last_md_vis_dt,
     max(case when cohort.last_np_vis_dt < stg_cohort_endo_visit_last_md_vis.endo_vis_dt
        then cohort.last_np_vis_dt end) over (partition by cohort.pat_key) as last_np_vis_dt,
    max(case when cohort.last_edu_vis_dt < stg_cohort_endo_visit_last_md_vis.endo_vis_dt
        then cohort.last_edu_vis_dt end) over (partition by cohort.pat_key) as last_edu_vis_dt,
    cohort.enc_rn
from
    {{ ref('stg_cohort_endo_visit_historical')}} as cohort
    inner join {{ ref('stg_cohort_endo_visit_last_md_vis_historical')}} as stg_cohort_endo_visit_last_md_vis
        on cohort.pat_key = stg_cohort_endo_visit_last_md_vis.pat_key
)

select
    cohort.pat_key,
    cohort.visit_key,
    cohort.mrn,
    cohort.patient_name,
    cohort.dob,
    cohort.age_years, --age at current visit
    --current encounter info:
    cohort.endo_vis_dt,
    cohort.provider_nm,
    cohort.dept_nm,
    cohort.prov_type,
    cohort.visit_type_nm,
    cohort.enc_type,
    cohort.specialty_care_ind,
    cohort.telehealth_ind,
    cohort.appt_stat,
    cohort.enc_rn,
    --last visit:
    cohort.last_endo_vis_dt,
    last_visit.last_md_vis_dt,
    last_visit.last_np_vis_dt,
    last_visit.last_edu_vis_dt
from
    {{ ref('stg_cohort_endo_visit_historical')}} as cohort
    left join last_visit as last_visit
        on last_visit.visit_key = cohort.visit_key
