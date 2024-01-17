{{ config(meta = {
    'critical': true
}) }}
with hospital_account_ods as (
    select
        hsp_acct_pat_csn.pat_enc_csn_id,
        max(hsp_account.hsp_account_id) as hsp_account_id,
        max(zc_pat_class.name) as hsp_acct_patient_class
    from
        {{source('clarity_ods','hsp_acct_pat_csn')}} as hsp_acct_pat_csn
        inner join {{source('clarity_ods','hsp_account')}} as hsp_account
            on hsp_account.hsp_account_id = hsp_acct_pat_csn.hsp_account_id
        inner join {{source('clarity_ods','zc_pat_class')}} as zc_pat_class
            on zc_pat_class.adt_pat_class_c = hsp_account.acct_class_ha_c
    where
        hsp_account.acct_billsts_ha_c != 40
    group by
        hsp_acct_pat_csn.pat_enc_csn_id
)

select
    {{
        dbt_utils.surrogate_key([
            'floor(visit.enc_id)',
            'visit.pat_id',
            'visit.create_by'
        ])
    }} as encounter_key,
    {{
        dbt_utils.surrogate_key([
            'hospital_account_ods.hsp_account_id',
            "'CLARITY'"
        ])
    }} as hospital_account_key,
	hospital_account_ods.pat_enc_csn_id,
	hospital_account_ods.hsp_account_id,
	hospital_account_ods.hsp_acct_patient_class,
	visit.visit_key,
	hospital_account.hsp_acct_key

from
	hospital_account_ods
	inner join {{source('cdw','visit')}} as visit
    	on visit.enc_id = hospital_account_ods.pat_enc_csn_id
    inner join {{source('cdw','hospital_account')}} as hospital_account
        on hospital_account.hsp_acct_id = hospital_account_ods.hsp_account_id
