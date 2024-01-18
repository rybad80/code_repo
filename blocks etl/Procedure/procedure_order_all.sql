select
    stg_procedure_order_all_combos.visit_key,
    stg_patient.pat_key,
    coalesce(stg_procedure_order_all_billing.billing_service_date_key, -1) as billing_service_date_key,
    stg_procedure_order_all_combos.proc_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    procedure.proc_nm as procedure_name,
    procedure.cpt_cd as cpt_code,
    case
        when procedure_group.proc_grp_nm = 'NOT APPLICABLE' then procedure.proc_cat
        else procedure_group.proc_grp_nm
    end as procedure_group_name,
    stg_procedure_order_all_billing.billing_service_date,
    trim( --noqa: PRS
        trailing ',' from
            case when stg_procedure_order_all_clinical.procedure_order_ind = 1
                then 'procedure_order,'
                else ''
            end
        || case when stg_procedure_order_all_billing.pb_transaction_ind = 1 then 'pb_transaction,' else '' end)
    as source_summary,
    coalesce(stg_procedure_order_all_clinical.procedure_order_ind, 0) as procedure_order_ind,
    coalesce(stg_procedure_order_all_billing.pb_transaction_ind, 0) as pb_transaction_ind,
    stg_procedure_order_all_billing.billing_department_name,
    coalesce(stg_procedure_order_all_billing.billing_service_provider_key, 0) as billing_service_provider_key,
    coalesce(stg_procedure_order_all_billing.billing_department_key, 0) as billing_department_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    coalesce(
        stg_procedure_order_all_clinical.upd_dt, stg_procedure_order_all_billing.upd_dt
    ) as block_last_update_date
from
    {{ref('stg_procedure_order_all_combos')}} as stg_procedure_order_all_combos
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = stg_procedure_order_all_combos.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = stg_procedure_order_all_combos.pat_key
    inner join {{source('cdw', 'procedure')}} as procedure --noqa: L029
        on procedure.proc_key = stg_procedure_order_all_combos.proc_key
    inner join {{source('cdw', 'procedure_group')}} as procedure_group
        on procedure_group.proc_grp_key = procedure.proc_grp_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_procedure_order_all_clinical')}} as stg_procedure_order_all_clinical
        on stg_procedure_order_all_clinical.proc_key = stg_procedure_order_all_combos.proc_key
            and stg_procedure_order_all_clinical.visit_key = stg_procedure_order_all_combos.visit_key
            and stg_procedure_order_all_clinical.encounter_date = stg_procedure_order_all_combos.join_date
            and stg_procedure_order_all_clinical.pat_key = stg_procedure_order_all_combos.pat_key
    left join {{ref('stg_procedure_order_all_billing')}} as stg_procedure_order_all_billing
        on stg_procedure_order_all_billing.proc_key = stg_procedure_order_all_combos.proc_key
            and stg_procedure_order_all_billing.pat_key = stg_procedure_order_all_combos.pat_key
            and stg_procedure_order_all_billing.billing_service_date
            = stg_procedure_order_all_combos.billing_service_date
            and stg_procedure_order_all_billing.visit_key = stg_procedure_order_all_combos.visit_key
            and stg_procedure_order_all_billing.billing_service_provider_key
            = stg_procedure_order_all_billing.billing_service_provider_key
            and stg_procedure_order_all_billing.billing_department_key
            = stg_procedure_order_all_billing.billing_department_key
where
    {{ limit_dates_for_dev(ref_date = 'stg_procedure_order_all_billing.billing_service_date') }}
