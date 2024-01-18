{{ config(meta = {
    'critical': true
}) }}

with interim_payor as (
    select
        payor.payor_id,
        payor.payor_nm
    from
        {{source('cdw', 'payor')}} as payor
    where
        payor.payor_id in (1001, 1002, 1003, 1004, 1005, 1006, 1140, 1148, 1013,
        1199, 1165, 1034, 1054, 1061, 1062, 1021, 1029, 1019, 1030, 1028, 1059,
        1069, 1065, 1071, 1097, 1074, 1181, 1085,
        1080, 1083, 1108, 1113, 1007, 1008, 1073, 1098, 1170,
        1171, 1172, 1173, 1174, 1195)
)

select
    row_number() over (partition by hospital_account.hsp_acct_id,
        benefit_plan.bp_nm
        order by bdc_info.bdc_id asc) as bdc_seq_num,
    bdc_info.bdc_id,
    liability_bucket.liab_bkt_key,
    bdc_info.bucket_id,
    hospital_account.hsp_acct_id as hospital_account_id,
    hospital_account.hsp_acct_key as hospital_account_key,
    hospital_account.hsp_acct_nm as hospital_account_name,
    hospital_account_visit.visit_key,
    stg_patient.mailing_state,
    stg_patient.mailing_zip,
    hospital_account.tot_acct_bal as total_account_balance,
    hospital_account.hb_pat_mrn,
    pat_class.dict_nm as patient_account_class,
    department.dept_nm as department_name,
    coalesce(department.specialty, provider_specialty.spec_nm) as specialty,
    hospital_account.adm_dt as admit_date,
    hospital_account.disch_dt as discharge_date,
    last_day(date_trunc('month', bdc_info.bdc_receive_date)) as denial_month,
    bdc_info.bdc_receive_date,
    bdc_info.bdc_create_date,
    case
        when interim_payor.payor_nm is not null then 'y'
        else 'n'
    end as interim_payor,
    case
        when (
            case
                when hospital_account.disch_dt is null
                    then cast(extract(epoch from current_date - hospital_account.adm_dt) / 60 / 60 / 24 as integer)
                else cast(extract(epoch from hospital_account.disch_dt - hospital_account.adm_dt)
                / 60 / 60 / 24 as integer)
            end
            ) > 30 and lower(pat_class.dict_nm) = 'inpatient' and interim_payor.payor_nm is not null then '1'
            else '0'
        end as interim_payment_ind,
    -- added as a validation check for interim_payment
    case
        when hospital_account.disch_dt is null then
            cast(extract(epoch from current_date - hospital_account.adm_dt) / 60 / 60 / 24 as integer)
        else cast(extract(epoch from hospital_account.disch_dt - hospital_account.adm_dt)
            / 60 / 60 / 24 as integer)
    end as stay_length_days,
    payor.payor_id,
    case when payor.payor_id = pri_payor.payor_id then '1' else '0' end as primary_payor_ind,
    payor.payor_nm as bucket_payer,
    benefit_plan.bp_nm as bucket_plan,
    bdc_info.is_initial_denial_yn,
    dim_owning_area.owning_area_nm,
    dim_owning_area.owning_area_abbr as owning_area_abbreviation,
    bdc_info.remit_code_id,
    bdc_info.remit_code_id || ' - ' || master_remittance.remit_cd_nm  as remit_nm_id,
    master_remittance.remit_cd_nm as remit_code_name,
    denial_status.dict_nm as denial_status,
    denial_type.dict_nm as denial_type,
    zc_root_cause.name as root_cause,
    hsp_bdc_denied_dates.denied_start_date,
    hsp_bdc_denied_dates.denied_end_date,
    hsp_bdc_denial_data.line_billed_amount,
    hsp_bdc_denial_data.line_allwd_amt as line_allowed_amount,
    hsp_bdc_denial_data.line_paid_amt as line_paid_amount,
    hsp_bdc_denial_data.line_denied_amt as line_denied_amount,
    case
        when to_char(bdc_info.bdc_receive_date, 'yyyymmdd') between 20200701 and 20210630
            and payor.payor_id = '1074' and bdc_info.remit_code_id = 29 then 1
        when payor.payor_id = 1013 and bdc_info.remit_code_id in (5095, 5251) then 1
        when payor.payor_id = 1080 and bdc_info.remit_code_id in (5253, 5254, 5257, 5262, 5263, 5286) then 1
        when payor.payor_id = 1083 and bdc_info.remit_code_id in (285, 5252) then 1
        when payor.payor_id = 1181 and bdc_info.remit_code_id in (5095, 5251) then 1
        else 0
    end as omit_filter
from
    {{source('clarity_ods', 'bdc_info')}} as bdc_info
inner join
    {{source('clarity_ods', 'hsp_bdc_payor')}} as hsp_bdc_payor
        on hsp_bdc_payor.bdc_id = bdc_info.bdc_id
left join
    {{source('clarity_ods', 'hsp_bdc_denial_data')}} as hsp_bdc_denial_data
        on hsp_bdc_denial_data.bdc_id = bdc_info.bdc_id
            and hsp_bdc_denial_data.line_on_eob = -1 and bdc_info.is_initial_denial_yn = 'y'
inner join
    {{source('cdw', 'payor')}} as payor
        on hsp_bdc_payor.payor_id = payor.payor_id
left join
    {{source('cdw', 'dim_owning_area')}} as dim_owning_area
        on dim_owning_area.owning_area_id = bdc_info.owning_area_c
inner join
    {{source('cdw', 'liability_bucket')}} as liability_bucket
        on liability_bucket.bkt_id = bdc_info.bucket_id
left join
    {{source('cdw', 'benefit_plan')}} as benefit_plan
        on benefit_plan.bp_key = liability_bucket.bp_key
inner join
    {{source('cdw', 'liability_bucket_account_xref')}} as liability_bucket_account_xref
        on liability_bucket_account_xref.liab_bkt_key = liability_bucket.liab_bkt_key
inner join
    {{source('cdw', 'hospital_account')}} as hospital_account
        on hospital_account.hsp_acct_key = liability_bucket_account_xref.hsp_acct_key
left join
    {{source('cdw', 'hospital_account_visit')}} as hospital_account_visit
        on hospital_account_visit.hsp_acct_key = hospital_account.hsp_acct_key
inner join
    {{source('cdw', 'payor')}} as pri_payor
        on hospital_account.pri_payor_key = pri_payor.payor_key
inner join
    {{source('cdw', 'department')}} as department
        on department.dept_key = hospital_account.disch_dept_key
left join
    {{source('cdw', 'provider_specialty')}} as provider_specialty
        on provider_specialty.prov_key = hospital_account.attend_prov_key
            and provider_specialty.line = 1
left join
    {{source('clarity_ods', 'hsp_bdc_denied_dates')}} as hsp_bdc_denied_dates
        on hsp_bdc_denied_dates.bdc_id = bdc_info.bdc_id
left join
    {{source('clarity_ods', 'zc_root_cause')}} as zc_root_cause
        on zc_root_cause.root_cause_c = bdc_info.root_cause_c
left join
    {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = hospital_account.pat_key
left join
    interim_payor
        on interim_payor.payor_id = payor.payor_id
left join
    {{source('cdw', 'cdw_dictionary')}} as pat_class
        on hospital_account.dict_acct_basecls_key = pat_class.dict_key
left join
    {{source('cdw', 'cdw_dictionary')}} as denial_status
        on denial_status.dict_key = liability_bucket.dict_bkt_stat_key
inner join
    {{source('cdw', 'master_remittance')}} as master_remittance
        on master_remittance.remit_key = bdc_info.remit_code_id
left join
    {{source('cdw', 'cdw_dictionary')}} as denial_type
        on master_remittance.dict_remit_cd_grp_two_key = denial_type.dict_key
where
    bdc_info.record_type_c = 1
    and bdc_info.record_status_c not in (99)
    and denial_type.src_id not in (6, 10, 11)
    and bdc_info.remit_code_id not in ('101', '23', '59')
    and bdc_info.bdc_receive_date between add_months(date_trunc('month', current_date), -25)
        and last_day(current_date)
    and denial_status.src_id in (5, 7)
