{{ config(
    materialized='table', dist='hospital_account_key',
    meta={
        'critical': true
    }
) }}

with all_hospital_accounts as (
    select hsp_account_id, line from {{source('clarity_ods','hsp_acct_dx_list')}}
    union
    select hsp_account_id, line from {{source('clarity_ods','hsp_acct_cpt_codes')}}
    union
    select hsp_account_id, line from {{source('clarity_ods','hsp_acct_px_list')}}
),

hospital_account_px as (
select
    hsp_account_id,
    line,
    proc_date,
    proc_perf_prov_id,
    icd_px_name,
    procedure_name,
    short_proc_name,
    ref_bill_code,
    ref_bill_code_set_c
from
    {{source('clarity_ods','hsp_acct_px_list')}} as hsp_acct_px_list
    inner join {{source('clarity_ods','cl_icd_px')}} as cl_icd_px
        on hsp_acct_px_list.final_icd_px_id = cl_icd_px.icd_px_id
)

select
    all_hospital_accounts.hsp_account_id,
    {{
        dbt_utils.surrogate_key([
            'all_hospital_accounts.hsp_account_id',
            "'CLARITY'"
        ])
    }} as hospital_account_key,
    all_hospital_accounts.line,
    case
        when hsp_acct_dx_list.dx_id is not null
        then
            {{
                dbt_utils.surrogate_key([
                    'hsp_acct_dx_list.dx_id',
                    "'CLARITY'"
                ])
            }} 
        else 0
    end as diagnosis_key,
    hsp_acct_dx_list.dx_id as diagnosis_id,
    hsp_acct_cpt_codes.cpt_code,
    hsp_acct_cpt_codes.cpt_code_date,
    hsp_acct_cpt_codes.cpt_perf_prov_id as cpt_performing_provider_id,
    hsp_acct_cpt_codes.cpt_modifiers,
    hospital_account_px.proc_date as procedure_date,
    hospital_account_px.proc_perf_prov_id as procedure_performing_provider_id,
    hospital_account_px.icd_px_name as icd_procedure_name,
    hospital_account_px.procedure_name,
    hospital_account_px.short_proc_name as short_procedure_name,
    hospital_account_px.ref_bill_code as icd_code,
    zc_dx_poa.name as final_diagnosis_present_at_admission,
    case
        when hospital_account_px.ref_bill_code_set_c = 2
        then 1
        else 0
    end as icd10_ind
from all_hospital_accounts
left join {{source('clarity_ods','hsp_acct_dx_list')}} as hsp_acct_dx_list
    on all_hospital_accounts.hsp_account_id = hsp_acct_dx_list.hsp_account_id
    and all_hospital_accounts.line = hsp_acct_dx_list.line
left join {{source('clarity_ods','hsp_acct_cpt_codes')}} as hsp_acct_cpt_codes
    on all_hospital_accounts.hsp_account_id = hsp_acct_cpt_codes.hsp_account_id
    and all_hospital_accounts.line = hsp_acct_cpt_codes.line
left join hospital_account_px
    on all_hospital_accounts.hsp_account_id = hospital_account_px.hsp_account_id
    and all_hospital_accounts.line = hospital_account_px.line
left join {{source('clarity_ods','zc_dx_poa')}} as zc_dx_poa
    on zc_dx_poa.dx_poa_c = hsp_acct_dx_list.final_dx_poa_c
