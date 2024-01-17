with diabetes_screening as ( -- depression screen date in the last 12 months
    select distinct
        stg_usnwr_diabetes_primary_pop.primary_key,
        stg_usnwr_diabetes_primary_pop.patient_name,
        stg_usnwr_diabetes_primary_pop.mrn,
        stg_usnwr_diabetes_primary_pop.dob,
        round(months_between(current_date, stg_usnwr_diabetes_primary_pop.dob) / 12, 2) as current_age,
        stg_usnwr_diabetes_primary_pop.encounter_key,
        stg_usnwr_diabetes_primary_pop.new_transfer_ind,
        diabetes_depression_screening.patient_key,
        diabetes_depression_screening.depression_screened_dt,
        diabetes_depression_screening.depression_screened_ind,
        diabetes_depression_screening.positive_depression_ind,
        diabetes_depression_screening.positive_suicide_ind,
        diabetes_depression_screening.depression_action_taken_ind,
        stg_usnwr_diabetes_primary_pop.submission_year
    from
        {{ref('stg_usnwr_diabetes_primary_pop')}} as stg_usnwr_diabetes_primary_pop
        inner join {{ref('diabetes_depression_screening')}} as diabetes_depression_screening
            on diabetes_depression_screening.patient_key = stg_usnwr_diabetes_primary_pop.primary_key
                and diabetes_depression_screening.depression_screened_dt between
                    stg_usnwr_diabetes_primary_pop.start_date and stg_usnwr_diabetes_primary_pop.end_date
),

last_department_encounter as (
    select
        encounter_specialty_care.patient_key,
        encounter_specialty_care.department_name,
        row_number() over(
            partition by
                encounter_specialty_care.patient_key
            order by
                encounter_specialty_care.encounter_date desc
        ) as encounter_num
    from
        {{ref('encounter_specialty_care')}} as encounter_specialty_care
    where
        lower(encounter_specialty_care.specialty_name) like 'endocrinology'
)

select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    diabetes_screening.primary_key,
    diabetes_screening.depression_screened_dt as metric_date,
    case
        when diabetes_screening.depression_screened_ind = '1'
        then diabetes_screening.primary_key
        else null
    end as num,
    diabetes_screening.primary_key as denom,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.metric_id,
    diabetes_screening.submission_year,
    diabetes_screening.patient_name,
    diabetes_screening.mrn,
    diabetes_screening.dob,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.division,
    diabetes_screening.depression_screened_ind,
    diabetes_screening.positive_depression_ind,
    diabetes_screening.depression_action_taken_ind,
    round(months_between(current_date, diabetes_screening.dob) / 12, 2) as current_age,
    diabetes_screening.encounter_key,
    diabetes_screening.new_transfer_ind,
    last_department_encounter.department_name
from
    diabetes_screening
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on usnews_metadata_calendar.question_number = 'c34'
    left join last_department_encounter
        on last_department_encounter.patient_key = diabetes_screening.primary_key
            and last_department_encounter.encounter_num = '1'
where
    (current_age > '13' and current_age < '19')
