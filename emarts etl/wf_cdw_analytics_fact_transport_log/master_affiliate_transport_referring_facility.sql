with inbound_referring_facilities as (
    select
        fact_transport_log.referring_facility,
        fact_transport_log.create_by,
    case
        when fact_transport_log.create_by != 'CLARITY' then 1
        else 0
    end as pre_epic_ind
    from {{ source('cdw', 'fact_transport_log') }} as fact_transport_log
    where lower(fact_transport_log.transport_type) like 'inbound%'
    and lower(fact_transport_log.final_status) = 'completed'
    group by 1, 2
),

affiliate_referring_facilities as (
    select
        referring_facility,
        pre_epic_ind,
        case
            when referring_facility like 'Abington%' then 5
            when referring_facility like 'Atlanticare%' then 10
            when referring_facility like 'Chester County%' then 35
            when referring_facility like 'Doylestown%' then 45
            when referring_facility like 'Einstein Medical Center%' then 85
            when referring_facility like 'Einstein Montgomery Hosp%' then 85
            when referring_facility like 'Grandview%' then 15
            when referring_facility like 'Holy Redeemer%' then 50
            when referring_facility like 'Hosp of Univ of Penn' then 75
            when referring_facility like 'Hospital Of University Of Penn%' then 75
            when referring_facility like 'Lancaster Gen%' then 40
            when referring_facility like 'Pennsylvania Hosp%' then 60
            when referring_facility like 'Saint Mary Medical%' then 70
            when referring_facility like 'Saint Mary Med Ctr%' then 70
            when referring_facility like 'Univ Med Ctr Princeton%' then 20
            when referring_facility like 'University Medical Center of Princeton%' then 20
            when referring_facility in (
                'Virtua Berlin',
                'Virtua Camden',
                'Virtua Marlton',
                'Virtua Voorhees',
                'Virtua Urgent Care'
                ) then 25
            when referring_facility like 'Virtua Memorial%' then 30
            when referring_facility like 'Virtua Willingboro%' then 30
            when referring_facility like 'Virtua West Jersey%' then 25
            when referring_facility like '%Our Lady Of Lourdes%' then 90
            when referring_facility like 'Riddle%' then 95
            when referring_facility like 'Paoli%' then 100
            when referring_facility like 'Lankenau%' then 105
            when referring_facility like 'Bryn Mawr%' then 110
            else 0
        end as mstr_affiliate_key,
        1 as affiliate_referring_ind
    from inbound_referring_facilities
    where mstr_affiliate_key != 0
)


select
    affiliate_referring_facilities.referring_facility,
    affiliate_referring_facilities.mstr_affiliate_key,
    master_affiliate.affiliate_nm,
    affiliate_referring_ind,
    pre_epic_ind,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from affiliate_referring_facilities
left join {{ ref('master_affiliate') }} as master_affiliate
    on master_affiliate.mstr_affiliate_key = affiliate_referring_facilities.mstr_affiliate_key
