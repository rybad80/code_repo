with homecare_sourcedata as (
select
'HDMS' as homecare_source_system,
'100' as entity_code,
case when ar.hdms_provider_identifier in (1, 3)
    then '14050'
when ar.hdms_provider_identifier = 2
    then '14025'
else 'UNKNOWN' end as department_code,
case when ar.hdms_provider_identifier in (1, 3)
    then 'ST020007-0000'
when ar.hdms_provider_identifier = 2
    then 'ST020003-0000'
else 'UNKNOWN'  end as account_code,
ar.hdms_transaction_date as start_date,
'CS_051' as cost_center_site_id,
sum(
    case when ar.hdms_provider_identifier in (1, 3)
        then ar.therapy_days
    when ar.hdms_provider_identifier = 2
        then ar.rentals end
) as amount,
sum(ar.rentals) as rentals,
sum(ar.therapy_days) as therapy_days
from
{{ ref('hdms_ar_transactions') }} as ar
group by 1, 2, 3, 4, 5, 6

union all

select
'FASTRACK' as homecare_source_system,
'100' as entity_code,
cc.cost_cntr_id as department_code,
case when cc.cost_cntr_id = '14050' then 'ST020007-0000' else 'ST020003-0000' end as account_code,
md.full_dt as start_date,
case when md.full_dt between '07/01/2019' and '08/31/2019' then 'CS_025' else 'CS_051' end as cost_center_site_id,
sum(
    case when cc.cost_cntr_id = '14050'
        then fs.therapy_days
    else (
            case when cc.cost_cntr_id = '14025'
            then fs.rentals end
        )
    end
    ) as amount,
sum(
    case when cc.cost_cntr_id = '14050'
        then fs.therapy_days
    else 0 end
    ) as therapy_days,
sum(
    case when cc.cost_cntr_id = '14025'
        then fs.rentals
    else 0
    end
    ) as rentals
from {{ source('cdw', 'fastrack_home_care_stats') }} as fs
left join {{ source('cdw', 'cost_center') }} as cc on fs.cost_cntr_key = cc.cost_cntr_key
left join {{ source('cdw', 'master_date') }} as md on fs.post_dt_key = md.dt_key
group by 1, 2, 3, 4, 5, 6
)
select
homecare_source_system,
entity_code,
department_code,
account_code,
start_date,
cost_center_site_id,
amount,
therapy_days,
rentals,
current_timestamp as update_date
from homecare_sourcedata
