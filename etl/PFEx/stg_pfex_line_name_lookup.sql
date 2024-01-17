with demo_question as (
    select
        pg_survey_demographics.survey_id,
        max(case
            when lower(pg_survey_demographics.varname) = 'itservty'
            then pg_survey_demographics.value end) as surv_line_id,
        max(case
            when lower(pg_survey_demographics.varname) in('itunique', 'ituniq')
            then pg_survey_demographics.value end) as patient_csn
        from {{source('ods', 'pg_survey_demographics')}} as pg_survey_demographics
    group by
        pg_survey_demographics.survey_id
),

    demo_question2 as (
        select
        demo_question.survey_id,
        case when demo_question.surv_line_id = 'CH0101U' then 'PD0101'
            when demo_question.surv_line_id = 'CH0101UE' then 'PD0101E'
            when demo_question.surv_line_id = 'CH0102UE' then 'PD0102E'
            else demo_question.surv_line_id
            end as survey_line_id,
           cast(demo_question.patient_csn as numeric(14, 3)) as csn
       from demo_question
)

    select distinct
        demo_question2.survey_line_id,
        stg_encounter.department_name,
        stg_encounter.department_id,
             case
        when demo_question2.survey_line_id in (
            'AS0101',
            'AS0101E')
            then 'Day Surgery'
        when demo_question2.survey_line_id in (
            'AS0102',
            'AS0102E')
            then 'Day Surgery - CPRU'
        when demo_question2.survey_line_id in (
            'CH0101U',
            'CH0101UE',
            'CH0102UE',
            'PD0101',
            'PD0101E',
            'PD0102E')
            then 'Inpatient Pediatric'
        when demo_question2.survey_line_id in (
            'IZ0101U',
            'IZ0101UE')
            then 'SDU'
        when demo_question2.survey_line_id in (
            'IZ0102U',
            'IZ0102UE')
            then 'SDU'
        when demo_question2.survey_line_id in (
            'MD0101',
            'MD0101E',
            'MD0103E',
            'MT0101CE')
            and stg_encounter.department_name like '%NTWK%'
                or stg_encounter.department_name like '%NETWORK%'
                or stg_encounter.department_id in ('89296012', '71309012', '99252512')
            then 'Primary Care'
        when demo_question2.survey_line_id in (
            'MD0101',
            'MD0101E',
            'MD0103E',
            'MT0101CE')
            and stg_encounter.department_name not like '%NTWK%'
                or stg_encounter.department_name not like '%NETWORK%'
                or stg_encounter.department_id not in ('89296012', '71309012', '99252512', '101012066')
            then 'Specialty Care'
        when demo_question2.survey_line_id in (
            'MD0102',
            'MD0102E',
            'MT0102CE')
            then 'Adult Specialty Care'
        when demo_question2.survey_line_id in (
            'NC0101',
            'NC0101E')
            then 'NICU'
        when demo_question2.survey_line_id in (
            'ON0101',
            'ON0101E')
            then 'Outpatient Oncology'
        when demo_question2.survey_line_id in (
            'OU0101',
            'OU0101E',
            'OV0101',
            'OV0101E')
            then 'Outpatient Services'
        when demo_question2.survey_line_id in (
            'OY0101',
            'OY0101E',
            'BT0101',
            'BT0101E')
            then 'Outpatient Behavioral Health'
        when demo_question2.survey_line_id in (
            'PE0101',
            'PE0101E')
            then 'Pediatric ED'
        when demo_question2.survey_line_id in (
            'RH0101',
            'RH0101E')
            then 'Inpatient Rehabilitation'
        when demo_question2.survey_line_id in (
            'UC0101',
            'UC0101E',
            'UT0101E')
            then 'Urgent Care'
        when demo_question2.survey_line_id in (
            'HH0101',
            'HH0101E')
            then 'Home Care'
        when demo_question2.survey_line_id in (
            'SP0101',
            'SP0101E')
            then 'Specialty Pharmacy'
        when demo_question2.survey_line_id in (
            'AS0103',
            'AS0103E')
            then 'Sedation'
        else demo_question2.survey_line_id
    end as survey_line_name,
            case when survey_line_id not like '%PE'
            and survey_line_id like '%E'
            then 0 else 1
            end as paper_survey_ind,
            case when survey_line_id in ('CH0102UE', 'PD0102E', 'MD0103E')
            then 1 else 0
            end as intl_survey_ind,
            case when survey_line_id in (
        'MT0101CE',
        'MT0102CE',
        'UT0101E',
        'BT0101',
        'BT0101E',
        'OV0101',
        'OV0101E')
        then 1 else 0
    end as telehealth_survey_ind
from
    demo_question2
inner join {{ref('stg_encounter')}} as stg_encounter
    on demo_question2.csn = stg_encounter.csn
where stg_encounter.encounter_date >= '2016-01-01'
