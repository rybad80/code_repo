select
    asp_ip_cap_cohort.visit_key,
    'Allergy' as history_type,
    case when regexp_like(
            patient_allergy.alrg_desc,
            '^CEF|^CEPH'
        ) then 'cephalosporin'
        when regexp_like(
            patient_allergy.alrg_desc,
            'AMOXICILLIN|AMPICILLIN|AUGMENTIN|DICLOXACILLIN|NAFCILLIN|OXACILLIN|PENICILLIN'
        )
        then 'penicillin'
        when regexp_like(
            patient_allergy.alrg_desc,
            'AZTREONAM|CARBACEPHEM|CARBAPENEM|CLAVAM|DORIPENEM|ERTAPENEM|IMIPENEM|MEROPENEM'
        )
        then 'other beta-lactam'
        end as history_description,
    nvl2(history_description, 1, 0) as active_visit_ind
from
    {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
    inner join {{source('cdw','patient_allergy')}} as patient_allergy
        on asp_ip_cap_cohort.pat_key = patient_allergy.pat_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as cdw_dictionary
        on patient_allergy.dict_stat_key = cdw_dictionary.dict_key
where
    cdw_dictionary.src_id = 1 --currently active
    or (
        cdw_dictionary.src_id = 2 --deleted after encounter
        and patient_allergy.entered_dt > asp_ip_cap_cohort.hospital_admit_date
    )
group by
    asp_ip_cap_cohort.visit_key,
    history_description

union all

select
    asp_ip_cap_cohort.visit_key,
    'Culture' as history_type,
    regexp_extract(
        lower(procedure_order_result_clinical.result_component_name),
        'mrsa|cult|legion|mycoplasma|pseud|aerug|staph.*aur|methicillin'
    ) as history_description,
    case when asp_ip_cap_cohort.visit_key = procedure_order_result_clinical.visit_key
        then 1 else 0 end as active_visit_ind
from
    {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
    inner join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
        on asp_ip_cap_cohort.pat_key = procedure_order_result_clinical.pat_key
where
    procedure_order_result_clinical.result_date < asp_ip_cap_cohort.hospital_admit_date + interval('7 days')
    and procedure_order_result_clinical.placed_date < asp_ip_cap_cohort.hospital_discharge_date
    and (
        lower(procedure_order_result_clinical.result_value) like '%final%'
        or lower(procedure_order_result_clinical.result_lab_status) like '%final%'
    )
    and regexp_like(lower(procedure_order_result_clinical.procedure_name), 'cult|mrsa|legion|mycoplasma')
    and (
        regexp_like(
            lower(procedure_order_result_clinical.result_component_name),
            'culture.*blood|blood.*culture|mrsa|mycoplasma.*pcr'
        )
        or regexp_like(
            lower(procedure_order_result_clinical.result_component_name),
            'respiratory'
        )
        and regexp_like(
            lower(procedure_order_result_clinical.result_value),
            'pseud|aerug|staph.*aur|methicillin|mrsa'
        )
    )
    --positive test results only
    and procedure_order_result_clinical.abnormal_result_ind = 1
group by
    asp_ip_cap_cohort.visit_key,
    history_description,
    active_visit_ind
