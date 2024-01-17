with last_visit as ( --rejoin to combine attributes for last md, np, and education visits
    select
        stg_cohort_endo_visit.pat_key,
        stg_cohort_endo_visit.patient_key,
        stg_cohort_endo_visit.encounter_key,
        stg_cohort_endo_visit.patient_name,
        stg_cohort_endo_visit.endo_vis_dt,
        stg_cohort_endo_visit.prov_type,
        max(case when stg_cohort_endo_visit.last_md_vis_dt <= stg_cohort_endo_visit_last_md_vis.endo_vis_dt
            then stg_cohort_endo_visit.last_md_vis_dt end) as last_md_vis_dt,
        max(case when stg_cohort_endo_visit.last_np_vis_dt <= stg_cohort_endo_visit_last_md_vis.endo_vis_dt
            then stg_cohort_endo_visit.last_np_vis_dt end) as last_np_vis_dt,
        max(case when stg_cohort_endo_visit.last_edu_vis_dt <= stg_cohort_endo_visit_last_md_vis.endo_vis_dt
            then stg_cohort_endo_visit.last_edu_vis_dt end) as last_edu_vis_dt,
        stg_cohort_endo_visit.enc_rn
    from
        {{ ref('stg_cohort_endo_visit') }} as stg_cohort_endo_visit
        inner join {{ ref('stg_cohort_endo_visit_last_md_vis') }} as stg_cohort_endo_visit_last_md_vis
            on stg_cohort_endo_visit.patient_key = stg_cohort_endo_visit_last_md_vis.patient_key
    group by
        stg_cohort_endo_visit.pat_key,
        stg_cohort_endo_visit.patient_key,
        stg_cohort_endo_visit.encounter_key,
        stg_cohort_endo_visit.patient_name,
        stg_cohort_endo_visit.endo_vis_dt,
        stg_cohort_endo_visit.prov_type,
        stg_cohort_endo_visit.enc_rn
),

last_endo_visit_date as (--when more than endo encounters happened on the same day, extract distinct endo_vis_dt
                        --to avoid pull last visit with visit on the same encounter date
    select
        stg_cohort_endo_visit.pat_key,
        stg_cohort_endo_visit.patient_key,
        stg_cohort_endo_visit.endo_vis_dt,
        lead(stg_cohort_endo_visit.endo_vis_dt) over (
            partition by
                stg_cohort_endo_visit.pat_key
            order by
                stg_cohort_endo_visit.endo_vis_dt desc
        ) as last_endo_vis_dt
    from
        {{ ref('stg_cohort_endo_visit') }} as stg_cohort_endo_visit
    group by
        stg_cohort_endo_visit.pat_key,
        stg_cohort_endo_visit.patient_key,
        stg_cohort_endo_visit.endo_vis_dt
)

select
    stg_cohort_endo_visit.pat_key,
    stg_cohort_endo_visit.patient_key,
    stg_cohort_endo_visit.visit_key,
    stg_cohort_endo_visit.encounter_key,
    stg_cohort_endo_visit.mrn,
    stg_cohort_endo_visit.patient_name,
    stg_cohort_endo_visit.dob,
    stg_cohort_endo_visit.age_years, --age at current visit
    --current encounter info:
    stg_cohort_endo_visit.endo_vis_dt,
    stg_cohort_endo_visit.provider_nm,
    stg_cohort_endo_visit.department_name,
    stg_cohort_endo_visit.prov_type,
    stg_cohort_endo_visit.visit_type,
    stg_cohort_endo_visit.enc_type,
    stg_cohort_endo_visit.inpatient_ind, --indicate onset t1y1 admissions since cy23 new workflow
    stg_cohort_endo_visit.specialty_care_ind,
    stg_cohort_endo_visit.telehealth_ind,
    stg_cohort_endo_visit.appt_stat,
    stg_cohort_endo_visit.enc_rn,
    --last visit: 
    last_endo_visit_date.last_endo_vis_dt, -- the date when patient last seen at dcc
    last_visit.last_md_vis_dt,
    last_visit.last_np_vis_dt,
    last_visit.last_edu_vis_dt
from
    {{ ref('stg_cohort_endo_visit') }} as stg_cohort_endo_visit
    left join last_visit
        on last_visit.encounter_key = stg_cohort_endo_visit.encounter_key
    left join last_endo_visit_date
        on last_endo_visit_date.pat_key = stg_cohort_endo_visit.pat_key
            and last_endo_visit_date.endo_vis_dt = stg_cohort_endo_visit.endo_vis_dt
