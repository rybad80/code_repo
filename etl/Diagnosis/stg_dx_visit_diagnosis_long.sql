with hsp_account_xwalk as (
        select
            hsp_acct_pat_csn.hsp_account_id,
            hsp_acct_pat_csn.pat_enc_csn_id,
            -- 34 accounts have duplicate primary CSNs this is a long standing issue
            row_number() over(partition by hsp_acct_pat_csn.pat_enc_csn_id
                            order by hsp_account_id desc
            ) as rownum
        from
             {{source('clarity_ods','hsp_acct_pat_csn')}} as hsp_acct_pat_csn
			inner join {{source('clarity_ods','hsp_account')}} as hsp_account
				on hsp_account.hsp_account_id = hsp_acct_pat_csn.hsp_account_id
				and hsp_account.prim_enc_csn_id = hsp_acct_pat_csn.pat_enc_csn_id

),

hsp_account_dx as (
    select
        hsp_account_id,
        admit_dx_id as dx_id,
        line,
        'HSP_ACCT_ADMIT_DX' as src
    from
        {{source('clarity_ods','hsp_acct_admit_dx')}}
    union all

    select
        hsp_account_id,
        dx_id,
        line,
        'HSP_ACCT_DX_LIST' as src
    from
        {{source('clarity_ods','hsp_acct_dx_list')}}

    union all

    select
        hsp_account_id,
        ext_injury_dx_id as dx_id,
        line,
        'HSP_ACCT_EXTINJ_CD' as src
    from
        {{source('clarity_ods','hsp_acct_extinj_cd')}}
),

fastrack as (
	select
		pat_diag.patient_number,
		patientm.medical_record_number,
		pat_diag.seq,
		min(pat_diag.seq) over (partition by pat_diag.patient_number, pat_diag.socdate) as min_seq,
		pat_diag.socdate,
        pat_diag.code,
		case
            when patientm.medical_record_number is not null and length(patientm.medical_record_number) != 8
            then '-1'
            else cast(coalesce(stg_patient_ods.pat_id, '0') as character varying(254))
        end as pat_id
	from
		{{source('fastrack_ods','pat_diag')}} as pat_diag
		left join {{source('fastrack_ods','patientm')}} as patientm
			on patientm.patient_number = pat_diag.patient_number
		left join {{ref('stg_patient_ods')}} as stg_patient_ods
			on stg_patient_ods.mrn = patientm.medical_record_number
)

select
    pat_enc_csn_id,
    dx_id,
    line,
    'HSP_ADMIT_DIAG' as src,
    null as dx_status
from
    {{source('clarity_ods','hsp_admit_diag')}}

union all

select
    hsp_account_xwalk.pat_enc_csn_id,
    hsp_account_dx.dx_id,
    hsp_account_dx.line,
    hsp_account_dx.src,
    null as dx_status
from
    hsp_account_dx
    inner join hsp_account_xwalk
        on hsp_account_xwalk.hsp_account_id = hsp_account_dx.hsp_account_id
        and rownum = 1

union all

select
    pat_enc_csn_id,
    dx_id,
    line,
    'PAT_ENC_DX' as src,
    dx_status
from
    {{ref('stg_pat_enc_dx')}}

union all

select
	visit.enc_id as pat_enc_csn_id,
    max(coalesce(icd9.dx_id, icd10.dx_id)) as dx_id,
	fastrack.seq as line,
    'FASTRACK' as src,
	case when fastrack.seq > fastrack.min_seq then 'Visit Other' else 'Visit Primary' end as dx_status
from
	fastrack
left join {{source('cdw','visit')}} as visit
	on visit.pat_id = fastrack.pat_id
	and socdate = visit.eff_dt
	and create_by = 'FASTRACK'
left join {{source('clarity_ods','clarity_edg')}} as icd9
	on icd9.ref_bill_code = fastrack.code
	and icd9.record_state_c is null
	and icd9.ref_bill_code_set_c in (1, 2)
     and icd9.record_type_c in (2, 3)
     and fastrack.socdate < '2015-10-01'
left join {{source('clarity_ods','clarity_edg')}} as icd10
	on icd10.ref_bill_code = fastrack.code
	and icd10.record_state_c is null
	and icd10.ref_bill_code_set_c in (1, 2)
     and icd10.record_type_c in (2, 3)
     and fastrack.socdate >= '2015-10-01'
 group by
	visit.enc_id,
	seq,
	min_seq

union all

select
    pat_enc_csn_id,
    dx_id,
    line,
    'IDX' as src,
    dx_status

from
    {{source('manual_ods','idx_visit_diagnosis')}}


union all

select
    pat_enc_csn_id,
    dx_id,
    line,
    'WELLSOFT' as src,
    dx_status

from
    {{source('manual_ods','wellsoft_visit_diagnosis')}}
