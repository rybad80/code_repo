with most_recent_endo_vis as ( --endo visit fact. WHEN ENC_RN = 1 --most recent endo enc per report point.
    select
        stg_diabetes_population_active.diabetes_reporting_month,
        year(stg_diabetes_population_active.diabetes_reporting_month) as usnwr_submission_year,
        stg_diabetes_population_active.patient_key,
        row_number() over (
            partition by
                stg_diabetes_population_active.diabetes_reporting_month,
                stg_diabetes_population_active.patient_key
            order by
                diabetes_visit_cohort.endo_vis_dt desc
        ) as enc_rn,
        diabetes_visit_cohort.visit_type,
        diabetes_visit_cohort.enc_type,
        diabetes_visit_cohort.provider_nm,
        diabetes_visit_cohort.prov_type,
        diabetes_visit_cohort.endo_vis_dt,
        diabetes_visit_cohort.last_md_vis_dt,
        diabetes_visit_cohort.last_np_vis_dt,
        diabetes_visit_cohort.last_edu_vis_dt
    from
        {{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
        inner join {{ ref('diabetes_visit_cohort') }} as diabetes_visit_cohort
            on stg_diabetes_population_active.patient_key = diabetes_visit_cohort.patient_key
    where
        lower(diabetes_visit_cohort.appt_stat) in ('arrived', 'completed')
        and diabetes_visit_cohort.endo_vis_dt < stg_diabetes_population_active.diabetes_reporting_month
        --only need the most recent visit info per reporting point (every 15 months):
        and diabetes_visit_cohort.endo_vis_dt
            >= stg_diabetes_population_active.diabetes_reporting_month - interval('15 month')
),

visit_indicators as (
    select
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key,
        max(most_recent_endo_vis.last_md_vis_dt) as last_md_vis_dt,
        max(most_recent_endo_vis.last_np_vis_dt) as last_np_vis_dt,
        max(most_recent_endo_vis.last_edu_vis_dt) as last_edu_vis_dt,
        max(case
            when stg_diabetes_population_active.diabetes_reporting_month - interval('15 month')
                <= most_recent_endo_vis.last_md_vis_dt
            then 1 else 0 end)
        as last_15mo_md_visit_ind, --patient has an MD visit in the past 15 months
        max(case
            when stg_diabetes_population_active.diabetes_reporting_month - interval('4 month')
                <= most_recent_endo_vis.last_md_vis_dt
                or stg_diabetes_population_active.diabetes_reporting_month - interval('4 month')
                    <= most_recent_endo_vis.last_np_vis_dt
            then 1 else 0 end)
        as last_4mo_mdnp_visit_ind, --patient has an MD/NP visit in the past 4 months
        max(case
            when stg_diabetes_population_active.diabetes_reporting_month - interval('15 month')
                <= most_recent_endo_vis.last_edu_vis_dt
            then 1 else 0 end)
        as last_15mo_edu_visit_ind --patient has an Education visit in the past 15 months
    from
        {{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
        inner join most_recent_endo_vis
            on most_recent_endo_vis.patient_key = stg_diabetes_population_active.patient_key
                and most_recent_endo_vis.diabetes_reporting_month
                    = stg_diabetes_population_active.diabetes_reporting_month
    group by
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key
)

select
    most_recent_endo_vis.patient_key,
    most_recent_endo_vis.diabetes_reporting_month,
    most_recent_endo_vis.visit_type as last_visit_type,
	most_recent_endo_vis.enc_type as last_encounter_type,
	most_recent_endo_vis.provider_nm as last_prov,
	most_recent_endo_vis.prov_type as last_prov_type,
	most_recent_endo_vis.endo_vis_dt as last_visit_date,
	visit_indicators.last_15mo_md_visit_ind,
	visit_indicators.last_4mo_mdnp_visit_ind,
	visit_indicators.last_15mo_edu_visit_ind,
    most_recent_endo_vis.usnwr_submission_year
from
    {{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
	left join most_recent_endo_vis
        on most_recent_endo_vis.patient_key = stg_diabetes_population_active.patient_key
            and most_recent_endo_vis.diabetes_reporting_month
                = stg_diabetes_population_active.diabetes_reporting_month
            and most_recent_endo_vis.enc_rn = '1'
	left join visit_indicators
        on visit_indicators.patient_key = stg_diabetes_population_active.patient_key
            and visit_indicators.diabetes_reporting_month = stg_diabetes_population_active.diabetes_reporting_month
