select distinct
    date(referraldate) as referraldate,
    patientname,
    mrn,
    treatmentstatus,
    case 
        when
            lower(treatmentstatus) like ('%treatment complete%') or  lower(treatmentstatus) like ('%on treatment%')
            then 1
        else 0
    end as converted_ind,
    'Radiation Oncology' as drill_down,
    {{
    dbt_utils.surrogate_key([
        'mrn',
        'patientname',
        'referraldate',
        'drill_down'
        ])
    }} as primary_key
from 
    {{ source('manual_ods', 'radonc_referrals')}}
where 
    referraldate >= '2020-07-01'
