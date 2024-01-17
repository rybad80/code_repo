select
    stg_usnwr_diabetes_primary_pop.domain, --noqa: L029
    stg_usnwr_diabetes_primary_pop.subdomain,
    stg_usnwr_diabetes_primary_pop.primary_key,
    stg_usnwr_diabetes_primary_pop.metric_date,
    stg_usnwr_diabetes_primary_pop.num,
    stg_usnwr_diabetes_primary_pop.metric_name,
    stg_usnwr_diabetes_primary_pop.metric_id,
    stg_usnwr_diabetes_primary_pop.submission_year,
    stg_usnwr_diabetes_primary_pop.patient_name,
    stg_usnwr_diabetes_primary_pop.mrn,
    stg_usnwr_diabetes_primary_pop.dob,
    stg_usnwr_diabetes_primary_pop.index_date,
    stg_usnwr_diabetes_primary_pop.question_number,
    stg_usnwr_diabetes_primary_pop.division,
    null as cpt_code,
    null as procedure_name,
    stg_usnwr_diabetes_primary_pop.denom,
    stg_usnwr_diabetes_primary_pop.encounter_key
from
    {{ref('stg_usnwr_diabetes_primary_pop')}} as stg_usnwr_diabetes_primary_pop
union all
select
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    stg_usnwr_diabetes_a1c_c35_1.primary_key,
    stg_usnwr_diabetes_a1c_c35_1.most_recent_a1c_date as metric_date,
    stg_usnwr_diabetes_a1c_c35_1.num,
    stg_usnwr_diabetes_a1c_c35_1.metric_name,
    stg_usnwr_diabetes_a1c_c35_1.metric_id,
    stg_usnwr_diabetes_a1c_c35_1.submission_year,
    stg_usnwr_diabetes_a1c_c35_1.patient_name,
    stg_usnwr_diabetes_a1c_c35_1.mrn,
    stg_usnwr_diabetes_a1c_c35_1.dob,
    stg_usnwr_diabetes_a1c_c35_1.most_recent_a1c_date as index_date,
    stg_usnwr_diabetes_a1c_c35_1.question_number,
    stg_usnwr_diabetes_a1c_c35_1.division,
    null as cpt_code,
    null as procedure_name,
    stg_usnwr_diabetes_a1c_c35_1.denom,
    null as encounter_key
from
    {{ref('stg_usnwr_diabetes_a1c_c35_1')}} as stg_usnwr_diabetes_a1c_c35_1
where
    (stg_usnwr_diabetes_a1c_c35_1.num is not null or stg_usnwr_diabetes_a1c_c35_1.denom is not null)
union all
select
    stg_usnwr_diabetes_cgm_interp.domain,
    stg_usnwr_diabetes_cgm_interp.subdomain,
    stg_usnwr_diabetes_cgm_interp.patient_key as primary_key,
    stg_usnwr_diabetes_cgm_interp.last_cgm_interpretated_date as metric_date,
    stg_usnwr_diabetes_cgm_interp.num,
    stg_usnwr_diabetes_cgm_interp.metric_name,
    stg_usnwr_diabetes_cgm_interp.metric_id,
    stg_usnwr_diabetes_cgm_interp.submission_year,
    stg_usnwr_diabetes_cgm_interp.patient_name,
    stg_usnwr_diabetes_cgm_interp.mrn,
    stg_usnwr_diabetes_cgm_interp.dob,
    stg_usnwr_diabetes_cgm_interp.last_cgm_interpretated_date as index_date,
    stg_usnwr_diabetes_cgm_interp.question_number,
    stg_usnwr_diabetes_cgm_interp.division,
    null as cpt_code,
    null as procedure_name,
    null as denom,
    null as encounter_key
from
    {{ref('stg_usnwr_diabetes_cgm_interp')}} as stg_usnwr_diabetes_cgm_interp
union all
select
    stg_usnwr_diabetes_c31.domain,
    stg_usnwr_diabetes_c31.subdomain,
    stg_usnwr_diabetes_c31.primary_key,
    stg_usnwr_diabetes_c31.metric_date,
    stg_usnwr_diabetes_c31.num,
    stg_usnwr_diabetes_c31.metric_name,
    stg_usnwr_diabetes_c31.metric_id,
    stg_usnwr_diabetes_c31.submission_year,
    stg_usnwr_diabetes_c31.patient_name,
    stg_usnwr_diabetes_c31.mrn,
    stg_usnwr_diabetes_c31.dob,
    stg_usnwr_diabetes_c31.metric_date as index_date,
    stg_usnwr_diabetes_c31.question_number,
    stg_usnwr_diabetes_c31.division,
    null as cpt_code,
    null as procedure_name,
    stg_usnwr_diabetes_c31.denom,
    null as encounter_key
from
    {{ref('stg_usnwr_diabetes_c31')}} as stg_usnwr_diabetes_c31
union all
select
    stg_usnwr_diabetes_depression_screening.domain,
    stg_usnwr_diabetes_depression_screening.subdomain,
    stg_usnwr_diabetes_depression_screening.primary_key,
    stg_usnwr_diabetes_depression_screening.metric_date,
    stg_usnwr_diabetes_depression_screening.num,
    stg_usnwr_diabetes_depression_screening.metric_name,
    stg_usnwr_diabetes_depression_screening.metric_id,
    stg_usnwr_diabetes_depression_screening.submission_year,
    stg_usnwr_diabetes_depression_screening.patient_name,
    stg_usnwr_diabetes_depression_screening.mrn,
    stg_usnwr_diabetes_depression_screening.dob,
    stg_usnwr_diabetes_depression_screening.metric_date as index_date,
    stg_usnwr_diabetes_depression_screening.question_number,
    stg_usnwr_diabetes_depression_screening.division,
    null as cpt_code,
    null as procedure_name,
    stg_usnwr_diabetes_depression_screening.denom,
    stg_usnwr_diabetes_depression_screening.encounter_key
from
    {{ref('stg_usnwr_diabetes_depression_screening')}} as stg_usnwr_diabetes_depression_screening
