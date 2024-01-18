with unique_verification as (
    select
        verification.enc_csn,
        max(verification.last_verif_datetime) as last_verif_datetime
    from
        {{source('clarity_ods', 'verification')}} as verification
    where
        verification.verification_type_c = 8
        and verification.verif_status_c = 1
    group by
        verification.enc_csn
),

financial_clearance_encounters as (
    select
        financial_clearance_encounter.mrn,
        financial_clearance_encounter.csn,
        financial_clearance_encounter.encounter_date,
        financial_clearance_encounter.appointment_date,
        clarity_dep.specialty,
        case
            when encounter_inpatient.visit_key is not null
                then encounter_inpatient.discharge_department_center_abbr
            else financial_clearance_encounter.department_center_abbr
        end as center,
        to_char(financial_clearance_encounter.encounter_date, 'yyyy-mm') as month_year,
        case
            when extract(month from financial_clearance_encounter.encounter_date) between 7 and 12
                then extract(year from financial_clearance_encounter.encounter_date) + 1
            else
                extract(year from financial_clearance_encounter.encounter_date)
        end as fiscal_year,
        financial_clearance_encounter.financial_clearance_ind
    --from
        --{{source('clarity_ods', 'pat_enc')}} as pat_enc
    --inner join
        --{{source('clarity_ods', 'patient')}} as patient
            --on patient.pat_id = pat_enc.pat_id
    from
        {{ref('financial_clearance_encounter')}} as financial_clearance_encounter
    inner join
        {{source('clarity_ods', 'clarity_dep')}} as clarity_dep
            on clarity_dep.department_id = financial_clearance_encounter.department_id
    left join
        {{ref('encounter_inpatient')}} as encounter_inpatient
            on financial_clearance_encounter.csn = encounter_inpatient.csn
    where
        extract(month from financial_clearance_encounter.appointment_date) >= 4
        and extract(year from financial_clearance_encounter.appointment_date) >= 2021
        and financial_clearance_encounter.appointment_date < current_date
        and financial_clearance_encounter.appointment_status_id in (1, 2, 6)
        and financial_clearance_encounter.appointment_date
            >= add_months(date_trunc('month', current_date), -25)
),

verified_encounters as (
    select
        csn,
        1 as benefits_collected,
        case
            when last_verif_datetime >= appointment_date - interval '3 days'
            and last_verif_datetime <= appointment_date
                then 1
        end as benefits_collected_72_hrs
    from
        financial_clearance_encounters
    inner join
        unique_verification as verification
            on verification.enc_csn = financial_clearance_encounters.csn
)

select
    financial_clearance_encounters.mrn,
    financial_clearance_encounters.csn,
    financial_clearance_encounters.encounter_date as contact_date,
    financial_clearance_encounters.appointment_date,
    financial_clearance_encounters.specialty,
    financial_clearance_encounters.center,
    financial_clearance_encounters.month_year,
    financial_clearance_encounters.fiscal_year,
    coalesce(verified_encounters.benefits_collected, 0) as benefits_collected_ind,
    coalesce(verified_encounters.benefits_collected_72_hrs, 0) as benefits_collected_72_hrs_ind
from
    financial_clearance_encounters
left join
    verified_encounters
        on verified_encounters.csn = financial_clearance_encounters.csn
where
    financial_clearance_encounters.financial_clearance_ind = 1
