select
    referral_date,
    drill_down,
    null as converted_ind,
    primary_key
from
    {{ ref('stg_cancer_center_bmt_referrals')}}
union all
select
    referral_date,
    drill_down,
    converted_ind,
    primary_key
from
    {{ ref('stg_cancer_center_op_referrals')}}
union all
select
    old_create_date as referral_date,
    drill_down,
    converted_ind,
    primary_key
from
    {{ ref('stg_cancer_center_gps_referrals')}}
union all
select
    referraldate as referral_date,
    drill_down,
    converted_ind,
    primary_key
from
    {{ ref('stg_cancer_center_radonc_referrals')}}
