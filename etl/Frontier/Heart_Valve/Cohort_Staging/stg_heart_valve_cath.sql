select
    cardiac_cath.surgery_visit_key as visit_key,
    cardiac_cath.mrn,
    stg_patient_ods.patient_name,
    cardiac_cath.surgery_csn,
    cardiac_cath.procedure_type,
    cardiac_cath.study_date as surgery_date,
    cardiac_cath.hsp_stat
from
{{ ref('cardiac_valve_center') }} as cardiac_valve_center
left join {{ ref('stg_patient_ods') }} as stg_patient_ods
    on cardiac_valve_center.mrn = stg_patient_ods.mrn
inner join {{ ref('cardiac_cath') }} as cardiac_cath
    on cardiac_valve_center.mrn = cardiac_cath.mrn
where
    year(add_months(cardiac_cath.study_date, 6)) > '2020'
    and lower(cardiac_cath.procedure_type) = 'diagnostic r&l heart'
