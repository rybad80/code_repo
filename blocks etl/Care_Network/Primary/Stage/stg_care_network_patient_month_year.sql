select
    stg_patient_ods.pat_id,
    stg_encounter_outpatient.encounter_date,
    stg_encounter_outpatient.visit_key,
    stg_encounter_outpatient.encounter_key,
    stg_patient_ods.mrn,
    stg_encounter_outpatient.pat_key,
    stg_patient_ods.patient_key,
    stg_patient_ods.dob,
    1 as completed_ind,
    stg_encounter_outpatient.dept_key as department_visit,
    stg_encounter_outpatient.prov_key,
    stg_encounter_outpatient.provider_key,
    stg_encounter_outpatient.age_years as patient_age_years,
    stg_encounter_outpatient.well_visit_ind,
    row_number() over(
        partition by
            stg_encounter_outpatient.patient_key
        order by stg_encounter_outpatient.encounter_date
    ) as visit_seq_pat,
    /*age category based on payor stratification*/
    case
        when
            stg_encounter_outpatient.age_months < 15
            then 'less than 15 months'
        when
                stg_encounter_outpatient.age_months >= 15
                and stg_encounter_outpatient.age_months < 30
                then 'between 15 and 30 months'
        when
            stg_encounter_outpatient.age_months >= 30
            and stg_encounter_outpatient.age_months < 36
            then 'between 30 and 36 months'
        when
            stg_encounter_outpatient.age_months >= 36 then cast(
                patient_age_years as varchar(50)
            ) || ' years'
    end as age_category_enc,
    /*need for later to pull dept/pcp at most recent visit as of
    first of each month:*/
    date_trunc(
            'month', stg_encounter_outpatient.encounter_date
        ) + cast('1 month' as interval)
    as month_year,
    row_number() over(
        partition by stg_encounter_outpatient.patient_key, month_year
        order by stg_encounter_outpatient.encounter_date desc
    ) as visit_seq_month,
    /*getting counts of well visits*/
    row_number() over(
        partition by stg_encounter_outpatient.patient_key,
            stg_encounter_outpatient.well_visit_ind
        order by stg_encounter_outpatient.encounter_date
    ) as well_count_tmp,
    case
        when stg_encounter_outpatient.well_visit_ind = 1
            then well_count_tmp end as well_count,
    /*getting date of last well visit*/
    case
        when stg_encounter_outpatient.well_visit_ind = 1
            then stg_encounter_outpatient.encounter_date
    end as last_well_date
from
    {{ ref('stg_encounter_outpatient') }} as stg_encounter_outpatient
inner join {{ ref('stg_patient_ods')}} as stg_patient_ods
    on stg_patient_ods.patient_key = stg_encounter_outpatient.patient_key
where
    primary_care_ind = 1
    --encounter types of office visits/appointments
    and encounter_type_id in (101, 50)
    --src_id's for statuses of 'completed', 'arrived'
    and appointment_status_id in (2, 6)
    and stg_encounter_outpatient.physician_service_level_ind = 1 --billed visits only
    and stg_encounter_outpatient.encounter_date < current_date
    and stg_encounter_outpatient.mrn != 'INVALID'
