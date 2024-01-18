-- purpose: get patient info for inpatient visits
-- granularity: one row per visit

select
    encounter_inpatient.visit_key,
    encounter_inpatient.pat_key,
    dict_pat_stat.dict_nm as patient_status,
    encounter_inpatient.hsp_acct_patient_class,
    floor(
        encounter_inpatient.age_years
    ) as patient_age_years,
    cast(
        months_between(
            encounter_inpatient.hospital_admit_date, encounter_inpatient.dob
        ) as smallint
    ) as patient_age_months,
    case
        when patient_age_months <= 1              then '1: 0Mo - 1Mo'
        when patient_age_months between 1 and 2   then '2: 1Mo - 2Mo'
        when patient_age_months between 2 and 6   then '3: 2Mo - 6Mo'
        when patient_age_months between 6 and 12  then '4: 6Mo - 12Mo'
        when patient_age_years  between 1 and 3   then '5: 1Y - 3Y'
        when patient_age_years  between 3 and 5   then '6: 3Y - 5Y'
        when patient_age_years  between 5 and 8   then '7: 5Y - 8Y'
        when patient_age_years  between 8 and 12  then '8: 8Y - 12Y'
        when patient_age_years  between 12 and 18 then '9: 12Y - 18Y'
        when patient_age_years > 18               then '10: >18Y'
    end as patient_age_category,
    max(encounter_inpatient.admission_department)             as adt_department_admit,
    max(encounter_inpatient.admission_department_center_abbr) as adt_department_center_admit,
    max(encounter_inpatient.discharge_department)             as adt_department_discharge,
    max(encounter_inpatient.discharge_department_center_abbr) as adt_department_center_discharge

from
    {{ref('encounter_inpatient')}} as encounter_inpatient
    inner join {{source('cdw', 'hospital_account')}} as hospital_account
        on encounter_inpatient.hsp_acct_key = hospital_account.hsp_acct_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_pat_stat
        on hospital_account.dict_pat_stat_key = dict_pat_stat.dict_key

where
    -- exclude non-inpatient encounters
    encounter_inpatient.hsp_acct_patient_class not in ('Emergency', 'Outpatient')
    and (
        date(encounter_inpatient.hospital_discharge_date) >= '2013-07-01'
            or (
                encounter_inpatient.hospital_admit_date is not null
                and encounter_inpatient.hospital_discharge_date is null
            )
    )
    and (
        -- only include these types of patients if they have been here for at least 23 hours
        encounter_inpatient.hsp_acct_patient_class not in (
            'Day Surgery',
            'Admit After Surgery-OBS',
            'Admit After Surgery'
        )
        or (
            encounter_inpatient.hospital_discharge_date - encounter_inpatient.hospital_admit_date
        ) >= interval('23 hours')
        or (
            encounter_inpatient.hospital_discharge_date is null
            and ((current_timestamp - encounter_inpatient.hospital_admit_date) >= interval('23 hours'))
        )
    )

group by
    encounter_inpatient.visit_key,
    encounter_inpatient.pat_key,
    patient_status,
    encounter_inpatient.hsp_acct_patient_class,
    encounter_inpatient.age_years,
    patient_age_months,
    patient_age_category
