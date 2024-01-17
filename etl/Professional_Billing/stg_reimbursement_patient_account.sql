select
    arpb_transactions.tx_id,
    account.account_name as guarantor,
    patient.pat_name as patient_name,
    patient.birth_date,
    patient.pat_mrn_id as mrn,
    referral.pre_cert_num,
    referral.auth_num
from
    {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions
left join {{ source('clarity_ods', 'patient') }} as patient on
    patient.pat_id = arpb_transactions.patient_id
left join {{ source('clarity_ods', 'account') }} as account on
    account.account_id = arpb_transactions.account_id
left join {{ source('clarity_ods', 'referral') }} as referral
    on referral.referral_id = arpb_transactions.referral_id
where
    arpb_transactions.tx_type_c = 1
    and arpb_transactions.void_date is null
