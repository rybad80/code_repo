with
    all_femur_fx as (
        select distinct
            usnews_billing.submission_year,
            usnews_billing.division,
            usnews_billing.question_number,
            encounter_ed.visit_key,
            encounter_ed.mrn,
            encounter_ed.patient_name,
            encounter_ed.dob,
            stg_encounter.hospital_admit_date as index_date,
            usnews_billing.provider_specialty
        from
            {{ ref('usnews_billing') }} as usnews_billing
            inner join {{ ref('stg_encounter') }} as stg_encounter
              on stg_encounter.pat_key = usnews_billing.pat_key
            inner join {{ ref('encounter_ed') }} as encounter_ed
              on encounter_ed.visit_key = stg_encounter.visit_key
        where
            lower(usnews_billing.question_number) = 'i26'
            and usnews_billing.service_date
                between date(stg_encounter.hospital_admit_date) and date(stg_encounter.hospital_discharge_date)
            and usnews_billing.age_years >= usnews_billing.age_gte
            and usnews_billing.age_years < usnews_billing.age_lt
            and lower(usnews_billing.provider_specialty) != 'ane'
            and usnews_billing.dx_inclusion_ind = 1
            and usnews_billing.proc_inclusion_ind = 1
            and encounter_ed.icu_ind = 0
),

    polytrauma as (
        select all_femur_fx.visit_key
        from
            all_femur_fx
            inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
                on diagnosis_encounter_all.visit_key = all_femur_fx.visit_key
        where
            (diagnosis_encounter_all.visit_diagnosis_ind = 1 or diagnosis_encounter_all.pb_transaction_ind = 1)
            and substring(lower(diagnosis_encounter_all.icd10_code), 1, 1) = 's' --is a traumatic injury
            and substring(lower(diagnosis_encounter_all.icd10_code), 1, 2) != 's7'
            and ( --but is another fracture or poly injury
                ( -- has a fracture
                    lower(diagnosis_encounter_all.diagnosis_name) like '%fracture%'
                    or lower(diagnosis_encounter_all.diagnosis_name) like '%fx%')
                -- head injury; --injury to abdomen, lower back, lumbar spine, pelvis and external genitals
                or substring(lower(diagnosis_encounter_all.icd10_code), 1, 2) in ('s0', 's3')
            )
        group by
            all_femur_fx.visit_key
),

first_surgery as (--region
        select
            all_femur_fx.visit_key,
            min(
                case when case_times.src_id = 5 then or_log_case_times.event_in_dt end
            ) as in_room -- wheels into the OR
        from
            all_femur_fx
            inner join {{ ref('surgery_encounter') }} as surgery_encounter
                on surgery_encounter.visit_key = all_femur_fx.visit_key
            inner join {{ source('cdw', 'or_log_case_times') }} as or_log_case_times
                on or_log_case_times.log_key = surgery_encounter.log_key
            inner join {{ source('cdw', 'cdw_dictionary') }} as case_times
                on case_times.dict_key = or_log_case_times.dict_or_pat_event_key
        group by
            all_femur_fx.visit_key
)

select
    all_femur_fx.submission_year,
    all_femur_fx.division,
    all_femur_fx.question_number,
    all_femur_fx.visit_key,
    all_femur_fx.mrn,
    all_femur_fx.patient_name,
    all_femur_fx.dob,
    all_femur_fx.index_date,
    extract(epoch from in_room - all_femur_fx.index_date) / 60.0 / 60 as n_hours, --noqa: PRS
    case when n_hours < 18 then 'femur_fx_pct_lt_18hr' else 'femur_fx_pct_gt_18hr' end as metric_name,
    1 as numerator,
    1 as denominator
from
    all_femur_fx
    inner join first_surgery
        on first_surgery.visit_key = all_femur_fx.visit_key
    left join polytrauma
        on polytrauma.visit_key = all_femur_fx.visit_key
where
    polytrauma.visit_key is null
order by
    all_femur_fx.submission_year
