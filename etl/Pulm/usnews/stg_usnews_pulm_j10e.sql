with stage as (
select
    stg_usnews_pulm_j10a.submission_year,
    stg_usnews_pulm_j10a.start_date,
    stg_usnews_pulm_j10a.end_date,
    date(stg_usnews_pulm_j10a.encounter_date - interval '1 year') as one_year_vis_lookback, -- noqa: L016
    stg_usnews_pulm_j10a.pat_key,
    stg_usnews_pulm_j10a.visit_key,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    stg_usnews_pulm_j10a.age_years,
    stg_usnews_pulm_j10a.csn,
    stg_usnews_pulm_j10a.encounter_date,
    max(date(sde.entered_dt)) as act_date,
    stg_usnews_pulm_j10a.provider_name,
    stg_usnews_pulm_j10a.provider_specialty,
    stg_usnews_pulm_j10a.department_name,
    stg_usnews_pulm_j10a.department_specialty
from
    {{ ref('stg_usnews_pulm_j10a')}} as stg_usnews_pulm_j10a
    inner join {{ source('cdw', 'smart_data_element_info')}} as sde
        on stg_usnews_pulm_j10a.pat_key = sde.pat_key
    inner join {{ source('cdw', 'clinical_concept')}} as cc
        on sde.concept_key = cc.concept_key
where
    stg_usnews_pulm_j10a.age_years >= 5 and stg_usnews_pulm_j10a.age_years < 21
    and lower(cc.concept_id) in (
            'medcin#302237',
            'chop#6509',
            'chop#6510',
            'chop#6511',
            'chop#6512',
            'chop#6513',
            'chop#6514',
            'chop#6515',
            'chop#6516',
            'chop#6517',
            'chop#6518')
    --includes act in care assistant and mychart: https://github.research.chop.edu/cqi/fy18-asthma-moc/blob/master/code/sql/fact_op_asthma.sql#l80 --noqa: L016
    and sde.entered_dt <= stg_usnews_pulm_j10a.end_date
    and sde.entered_dt >= one_year_vis_lookback
group by
    stg_usnews_pulm_j10a.submission_year,
    stg_usnews_pulm_j10a.start_date,
    stg_usnews_pulm_j10a.end_date,
    one_year_vis_lookback,
    stg_usnews_pulm_j10a.pat_key,
    stg_usnews_pulm_j10a.visit_key,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    stg_usnews_pulm_j10a.age_years,
    stg_usnews_pulm_j10a.csn,
    stg_usnews_pulm_j10a.encounter_date,
    stg_usnews_pulm_j10a.provider_name,
    stg_usnews_pulm_j10a.provider_specialty,
    stg_usnews_pulm_j10a.department_name,
    stg_usnews_pulm_j10a.department_specialty

union all

select
    stg_usnews_pulm_j10a.submission_year,
    stg_usnews_pulm_j10a.start_date,
    stg_usnews_pulm_j10a.end_date,
    date(stg_usnews_pulm_j10a.encounter_date - interval '1 year') as one_year_vis_lookback, --noqa: L016
    stg_usnews_pulm_j10a.pat_key,
    stg_usnews_pulm_j10a.visit_key,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    stg_usnews_pulm_j10a.age_years,
    stg_usnews_pulm_j10a.csn,
    stg_usnews_pulm_j10a.encounter_date,
    max(date(questionnaire_patient_assigned.submitted_qnr_date)) as act_date,
    stg_usnews_pulm_j10a.provider_name,
    stg_usnews_pulm_j10a.provider_specialty,
    stg_usnews_pulm_j10a.department_name,
    stg_usnews_pulm_j10a.department_specialty
from
    {{ ref('stg_usnews_pulm_j10a')}} as stg_usnews_pulm_j10a
    inner join {{ ref('questionnaire_patient_assigned')}} as questionnaire_patient_assigned --noqa: L016
        on stg_usnews_pulm_j10a.pat_key = questionnaire_patient_assigned.pat_key
        and lower(questionnaire_patient_assigned.submitted_qnr_status) like 'completed%'
where
    stg_usnews_pulm_j10a.age_years >= 5 and stg_usnews_pulm_j10a.age_years < 21
    and questionnaire_patient_assigned.form_id in
            (
            '104614', -- ASTHMA CONTROL TOOL (AC TOOL) QUESTIONNAIRE (WELCOME/MYCHOP)
            '500124', -- PULM ASTHMA CONTROL TOOL (AC TOOL) QUESTIONNAIRE (WELCOME/MYCHOP)
            '102620'  -- WEL CN ASTHMA CONTROL QUESTIONNAIRE W/SCORING
            )
    and questionnaire_patient_assigned.submitted_qnr_date <= stg_usnews_pulm_j10a.end_date
    and questionnaire_patient_assigned.submitted_qnr_date >= one_year_vis_lookback
group by
    stg_usnews_pulm_j10a.submission_year,
    stg_usnews_pulm_j10a.start_date,
    stg_usnews_pulm_j10a.end_date,
    one_year_vis_lookback,
    stg_usnews_pulm_j10a.pat_key,
    stg_usnews_pulm_j10a.visit_key,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    stg_usnews_pulm_j10a.age_years,
    stg_usnews_pulm_j10a.csn,
    stg_usnews_pulm_j10a.encounter_date,
    stg_usnews_pulm_j10a.provider_name,
    stg_usnews_pulm_j10a.provider_specialty,
    stg_usnews_pulm_j10a.department_name,
    stg_usnews_pulm_j10a.department_specialty
)

select
    submission_year,
    start_date,
    end_date,
    one_year_vis_lookback,
    pat_key,
    visit_key,
    mrn,
    patient_name,
    dob,
    age_years,
    csn,
    encounter_date,
    max(act_date) as max_act_date,
    provider_name,
    provider_specialty,
    department_name,
    department_specialty
from stage
group by
    submission_year,
    start_date,
    end_date,
    one_year_vis_lookback,
    pat_key,
    visit_key,
    mrn,
    patient_name,
    dob,
    age_years,
    csn,
    encounter_date,
    provider_name,
    provider_specialty,
    department_name,
    department_specialty
