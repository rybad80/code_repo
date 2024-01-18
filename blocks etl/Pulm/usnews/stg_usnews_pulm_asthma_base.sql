-- RUNTIME_S   QH_ESTCOST  LOG_COST    QH_SNIPPETS QH_ESTMEM   N_CHAR
-- 00:00:04    75392       4.877325    6           42728       2924

select
    enc.pat_key,
    enc.visit_key,
    enc.mrn,
    enc.patient_name,
    enc.dob,
    enc.age_years,
    enc.csn,
    enc.encounter_date,
    enc.sex,
    enc.provider_name,
    group_concat(prov_spec.spec_nm, ';') as provider_specialty,
    enc.department_name,
    enc.specialty_name as department_specialty,
    enc.encounter_type,
    enc.visit_type,
    enc.appointment_status,
--    enc.telehealth_ind, -- will join to stg_encounter_telehealth later for j10d
    dx.icd10_code,
    dx.diagnosis_name,
    case
        when pat_enc_dx.primary_dx_yn = 'Y'
            and enc.visit_type_id not in ('3133', '7207', '4120', '4108', '2331', '8221', '8224', '7203', '8227', '9976', '8220', '8226', '4109', '7206', '4119', '4107', '3213', '2533', '2755', '2754', '8225', '3135', -- exclude sleep visit --noqa: L016
                                          '3704', '2550', '3723', '3722', '3724') -- exclude aerodigestive visit
        then 1
        else 0
        end as primary_dx_ind
from
    {{ref('stg_encounter')}} as enc
    inner join {{source('cdw', 'provider_specialty')}} as prov_spec
        on enc.provider_id = prov_spec.prov_id
    inner join {{ref('diagnosis_encounter_all')}} as dx
        on enc.visit_key = dx.visit_key
    inner join {{source('clarity_ods', 'pat_enc_dx')}} as pat_enc_dx
        on enc.csn = pat_enc_dx.pat_enc_csn_id
        and pat_enc_dx.dx_id = dx.diagnosis_id
        and dx.visit_diagnosis_ind = 1 -- only consider visit diagnosis
where
    lower(dx.icd10_code) like 'j45.%'
    and enc.encounter_type_id in ('101', '50') -- office visit, appointment
    and enc.appointment_status_id in (2, 6) -- completed, arrived
    and (lower(enc.specialty_name) = 'pulmonary' -- any appointments with Pulm department
        or (lower(prov_spec.spec_nm) = 'pulmonary' and enc.visit_type_id in ('1515', '1516')) -- PAPA clinic appointments with Pulm provider --noqa: L016
        )
    and enc.department_id not in ('101022016', '101001610') -- remove virtua sleep lab and bgr pulmonary function departments --noqa: L016
group by
    enc.pat_key,
    enc.visit_key,
    enc.mrn,
    enc.patient_name,
    enc.dob,
    enc.age_years,
    enc.csn,
    enc.encounter_date,
    enc.sex,
    enc.provider_name,
    enc.department_name,
    department_specialty,
    enc.encounter_type,
    enc.visit_type,
    enc.appointment_status,
    dx.icd10_code,
    dx.diagnosis_name,
    primary_dx_ind
