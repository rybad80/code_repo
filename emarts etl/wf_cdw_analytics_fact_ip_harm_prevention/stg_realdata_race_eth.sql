with patient_race as (
    select
        patient_race_ethnicity.pat_key,
        max(lower(cdw_dictionary.dict_nm)) as pat_race,
        -- get count of distinct race values, which will be used to define
        -- "multi-racial" in the next step
        count(distinct lower(cdw_dictionary.dict_nm)) as count_race_values
    from
        {{ source('cdw', 'patient_race_ethnicity') }} as patient_race_ethnicity
        inner join {{ source('cdw', 'cdw_dictionary') }} as cdw_dictionary
            on patient_race_ethnicity.dict_race_ethnic_key = cdw_dictionary.dict_key
    where
        patient_race_ethnicity.race_ind = 1
    group by
        patient_race_ethnicity.pat_key
),

patient_ethnicity as (
    select
        patient_race_ethnicity.pat_key,
        max(lower(cdw_dictionary.dict_nm)) as pat_ethnicity
    from
        {{ source('cdw', 'patient_race_ethnicity') }} as patient_race_ethnicity
        inner join {{ source('cdw', 'cdw_dictionary') }} as cdw_dictionary
            on patient_race_ethnicity.dict_race_ethnic_key = cdw_dictionary.dict_key
    where
        patient_race_ethnicity.ethnic_ind = 1
    group by
        patient_race_ethnicity.pat_key
    having
        -- having multiple ethnicities is not meaningful since, at present, the
        -- only options are hispanic and non-hispanic; restrict only to patients
        -- with 1 ethnicity value
        count(distinct lower(cdw_dictionary.dict_nm)) <= 1
)

select
    coalesce(patient_race.pat_key, patient_ethnicity.pat_key) as pat_key,
    case
        -- multi-racial
        when
            patient_race.count_race_values > 1
            then 'multi_racial'
        -- hispanic
        when
            patient_race.pat_race = 'black or african american'
            and patient_ethnicity.pat_ethnicity = 'hispanic or latino'
            then 'hispanic_black'
        when
            patient_race.pat_race = 'white'
            and patient_ethnicity.pat_ethnicity = 'hispanic or latino'
            then 'hispanic_white'
        when
            patient_ethnicity.pat_ethnicity = 'hispanic or latino'
            then 'hispanic_latino'
        -- non-hispanic
        when
            patient_race.pat_race = 'black or african american'
            then 'non_hispanic_black'
        when
            patient_race.pat_race = 'white'
            then 'non_hispanic_white'
        when
            patient_race.pat_race in ('asian', 'indian')
            then 'asian'
        -- all other values
        when
            patient_race.pat_race = 'refused'
            then 'refused'
        when
            patient_race.pat_race is null
            then 'blank'
        -- due to small numbers, the following values are combined into "Other":
        -- 1) American Indian or Alaska Native
        -- 2) Native Hawaiian or Other Pacific Islander
        else 'other'
    end as pat_race_ethnicity
from
    patient_race
    full outer join patient_ethnicity
        on patient_race.pat_key = patient_ethnicity.pat_key