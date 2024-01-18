with base as (
select
    pat_key,
    visit_key,
    encounter_date,
    entered_date,
    concept_description,
    concept_id,
    cast(element_value as varchar(20)) as element_value,
    sde_entered_employee as entered_employee
from {{ref('smart_data_element_all')}}
where
    epic_source_location = 'SmartForm 501'
    and encounter_date >= '2018-01-01'
),


final as (
select
    visit_key,
    pat_key,
    encounter_date,
    max(case when concept_description like '%CSSRS%' or concept_description like '%C-SSRS%'
            then entered_date end) as cssrs_entered_date,
    max(case when concept_description like '%CSSRS%' or concept_description like '%C-SSRS%'
            then entered_employee end) as entered_employee,
    cast(max(case when concept_id = 'CHOP#1396' then element_value end) as integer) as si_q1_slv,
    cast(max(case when concept_id = 'CHOP#1397' then element_value end) as integer) as si_q2_slv,
    cast(max(case when concept_id = 'CHOP#1398' then element_value end) as integer) as si_q3_slv,
    cast(max(case when concept_id = 'CHOP#1399' then element_value end) as integer) as si_q4_slv,
    cast(max(case when concept_id = 'CHOP#1400' then element_value end) as integer) as si_q5_slv,
    cast(max(case when concept_id = 'CHOP#1444' then element_value end) as integer) as si_q1_lifetime,
    cast(max(case when concept_id = 'CHOP#1445' then element_value end) as integer) as si_q1_past_month,
    cast(max(case when concept_id = 'CHOP#1446' then element_value end) as integer) as si_q2_lifetime,
    cast(max(case when concept_id = 'CHOP#1447' then element_value end) as integer) as si_q2_past_month,
    cast(max(case when concept_id = 'CHOP#1448' then element_value end) as integer) as si_q3_lifetime,
    cast(max(case when concept_id = 'CHOP#1449' then element_value end) as integer) as si_q3_past_month,
    cast(max(case when concept_id = 'CHOP#1450' then element_value end) as integer) as si_q4_lifetime,
    cast(max(case when concept_id = 'CHOP#1451' then element_value end) as integer) as si_q4_past_month,
    cast(max(case when concept_id = 'CHOP#1452' then element_value end) as integer) as si_q5_lifetime,
    cast(max(case when concept_id = 'CHOP#1453' then element_value end) as integer) as si_q5_past_month,
    cast(max(case when concept_id = 'CHOP#1465' then element_value end) as integer) as sb_q1_lifetime,
    cast(max(case when concept_id = 'CHOP#1466' then element_value end) as integer) as sb_q1_past_3_months,
    cast(max(case when concept_id = 'CHOP#1467' then element_value end) as integer) as sb_q2_lifetime,
    cast(max(case when concept_id = 'CHOP#1468' then element_value end) as integer) as sb_q2_past_3_months,
    cast(max(case when concept_id = 'CHOP#1469' then element_value end) as integer) as sb_q3_lifetime,
    cast(max(case when concept_id = 'CHOP#1470' then element_value end) as integer) as sb_q3_past_3_months,
    cast(max(case when concept_id = 'CHOP#1471' then element_value end) as integer) as sb_q4_lifetime,
    cast(max(case when concept_id = 'CHOP#1472' then element_value end) as integer) as sb_q4_past_3_months,
    cast(max(case when concept_id = 'CHOP#1481' then element_value end) as integer) as sb_q1_nssi_lifetime,
    cast(max(case when concept_id = 'CHOP#1482' then element_value end) as integer) as sb_q1_injury_lifetime,
    cast(max(case when concept_id = 'CHOP#1484' then element_value end) as integer) as sb_q1_nssi_past_3_months,
    cast(max(case when concept_id = 'CHOP#1485' then element_value end) as integer) as sb_q1_injury_past_3_months,
    cast(max(case when concept_id = 'CHOP#1517' then element_value end) as integer) as sb_q1_slv,
    cast(max(case when concept_id = 'CHOP#1519' then element_value end) as integer) as sb_q1_slv_injury,
    cast(max(case when concept_id = 'CHOP#1520' then element_value end) as integer) as sb_q1_slv_nssi,
    cast(max(case when concept_id = 'CHOP#1522' then element_value end) as integer) as sb_q2_slv,
    cast(max(case when concept_id = 'CHOP#1525' then element_value end) as integer) as sb_q3_slv,
    cast(max(case when concept_id = 'CHOP#1528' then element_value end) as integer) as sb_q4_slv,
    cast(max(case when concept_id = 'CHOPBH#242' then element_value end) as integer) as qs_q1,
    cast(max(case when concept_id = 'CHOPBH#243' then element_value end) as integer) as qs_q2,
    cast(max(case when concept_id = 'CHOPBH#244' then element_value end) as integer) as qs_q3,
    max(case when concept_id in ('CHOP#2271', 'CHOP#2272', 'CHOP#2273')
                             or (concept_id = 'CHOP#1570' and element_value = 'Not Assessed')
        then 1 end) as cssrs_declined_ind,
    cast(max(
        case when concept_id in ('CHOPBH#468', 'CHOPBH#467') then element_value end
    ) as integer) as cssrs_noncompliant_ind
from base
group by visit_key, pat_key, encounter_date
)

select
    visit_key,
    pat_key,
    encounter_date,
    cssrs_entered_date,
    entered_employee,
    si_q1_slv,
    si_q2_slv,
    si_q3_slv,
    si_q4_slv,
    si_q5_slv,
    si_q1_lifetime,
    si_q1_past_month,
    si_q2_lifetime,
    si_q2_past_month,
    si_q3_lifetime,
    si_q3_past_month,
    si_q4_lifetime,
    si_q4_past_month,
    si_q5_lifetime,
    si_q5_past_month,
    sb_q1_lifetime,
    sb_q1_past_3_months,
    sb_q2_lifetime,
    sb_q2_past_3_months,
    sb_q3_lifetime,
    sb_q3_past_3_months,
    sb_q4_lifetime,
    sb_q4_past_3_months,
    sb_q1_nssi_lifetime,
    sb_q1_injury_lifetime,
    sb_q1_nssi_past_3_months,
    sb_q1_injury_past_3_months,
    sb_q1_slv,
    sb_q1_slv_injury,
    sb_q1_slv_nssi,
    sb_q2_slv,
    sb_q3_slv,
    sb_q4_slv,
    qs_q1,
    qs_q2,
    qs_q3,
    cssrs_declined_ind,
    cssrs_noncompliant_ind,
    case when si_q1_slv  is null and si_q2_slv  is null
        and si_q3_slv  is null
        and si_q4_slv  is null
        and si_q5_slv  is null
        and sb_q1_slv  is null
        and sb_q1_slv_injury  is null
        and sb_q1_slv_nssi  is null
        and sb_q2_slv  is null
        and sb_q3_slv  is null
        and sb_q4_slv  is null  then 0 else 1 end as slv_given_ind,
    case when si_q1_lifetime is null
        and si_q1_past_month is null
        and si_q2_lifetime is null
        and si_q2_past_month is null
        and si_q3_lifetime is null
        and si_q3_past_month is null
        and si_q4_lifetime is null
        and si_q4_past_month is null
        and si_q5_lifetime is null
        and si_q5_past_month is null
        and sb_q1_lifetime is null
        and sb_q1_past_3_months is null
        and sb_q2_lifetime is null
        and sb_q2_past_3_months is null
        and sb_q3_lifetime is null
        and sb_q3_past_3_months is null
        and sb_q4_lifetime is null
        and sb_q4_past_3_months is null
        and sb_q1_nssi_lifetime is null
        and sb_q1_injury_lifetime is null
        and sb_q1_nssi_past_3_months is null
        and sb_q1_injury_past_3_months is null then 0 else 1 end as lifetime_month_given_ind,
    case when qs_q1 is null
        and qs_q2 is null
        and qs_q3 is null then 0 else 1 end as qs_given_ind
from final
