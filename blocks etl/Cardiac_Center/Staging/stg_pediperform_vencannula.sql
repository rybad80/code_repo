with perfusion_sde as (
select
          anes_visit_key,
          log_key,
          seq_num,
          concept_id,
          concept_description,
          element_value --select *

 from
      {{ref('cardiac_perfusion_smart_data_element')}}
where
     concept_id in ('CHOP#2876', 'CHOP#2877', 'CHOP#2878', 'CHOP#2881', 'CHOP#2882', 'CHOP#2883',
                    'CHOP#2884', 'CHOP#2885', 'CHOP#2886', 'CHOPANES#036', 'CHOPANES#037', 'CHOPANES#038')
),

vencantyp1 as (
select
     log_key,
     case when concept_id = 'CHOP#2876' then element_value end as vencantyp
from
    perfusion_sde
where
     concept_id = 'CHOP#2876'
),

vencanstyle1 as (
select
     log_key,
     case when concept_id in ('CHOP#2877', 'CHOP#2878') then element_value end as vencanstyle_raw
from
    perfusion_sde
where
     concept_id in ('CHOP#2877', 'CHOP#2878')
),

vencansz1 as (
select
     log_key,
     case when concept_id in ('CHOP#2877', 'CHOP#2878', 'CHOPANES#036') then element_value end as vencansz
from
    perfusion_sde
where
     concept_id in ('CHOP#2877', 'CHOP#2878', 'CHOPANES#036')
),

vencan1_all as (
select distinct
     vencantyp1.log_key,
     1 as sortnum,
     vencantyp,
     vencanstyle_raw,
     cast(coalesce(regexp_replace(vencanstyle_raw, '[' || chr(40) || ',' || chr(41) || ',' || chr(65) || '-'
          || chr(122) || ']', ''), regexp_replace(vencansz, '[' || chr(40) || ',' || chr(41) || ',' || chr(65)
          || '-' || chr(122) || ']', '')) as varchar(64)) as vencansz

from
    vencantyp1
    left join vencanstyle1 on vencantyp1.log_key = vencanstyle1.log_key
    left join vencansz1 on vencantyp1.log_key = vencansz1.log_key
),

vencan1 as (
select
      log_key,
      sortnum,
      row_number() over (partition by log_key order by vencansz) as vencansort,
      vencantyp,
      vencanstyle_raw,
      vencansz
from
     vencan1_all
),

vencantyp2 as (
select
     log_key,
     case when concept_id = 'CHOP#2881' then element_value end as vencantyp
from
    perfusion_sde
where
     concept_id = 'CHOP#2881'
),

vencanstyle2 as (
select
     log_key,
     case when concept_id in ('CHOP#2882', 'CHOP#2883', 'CHOPANES#037') then element_value end as vencanstyle_raw
from
    perfusion_sde
where
     concept_id in ('CHOP#2882', 'CHOP#2883', 'CHOPANES#037')
),

vencansz2 as (
select
     log_key,
     case when concept_id in ('CHOP#2882', 'CHOP#2883') then element_value end as vencansz
from
    perfusion_sde
where
     concept_id in ('CHOP#2882', 'CHOP#2883')
),

vencan2_all as (
select distinct
     vencantyp2.log_key,
     2 as sortnum,
     vencantyp,
     vencanstyle_raw,
     cast(coalesce(regexp_replace(vencanstyle_raw, '[' || chr(40) || ',' || chr(41) || ',' || chr(65) || '-'
          || chr(122) || ']', ''), regexp_replace(vencansz, '[' || chr(40) || ',' || chr(41) || ',' || chr(65)
          || '-' || chr(122) || ']', '')) as varchar(64)) as vencansz
from
    vencantyp2
    left join vencanstyle2 on vencantyp2.log_key = vencanstyle2.log_key
    left join vencansz2 on vencantyp2.log_key = vencansz2.log_key
),

vencan2 as (
select
      log_key,
      sortnum,
      row_number() over (partition by log_key order by vencansz) as vencansort,
      vencantyp,
      vencanstyle_raw,
      vencansz
from
     vencan2_all
),

vencantyp3 as (
select
     log_key,
     case when concept_id = 'CHOP#2884' then element_value end as vencantyp
from
    perfusion_sde
where
     concept_id = 'CHOP#2884'
),

vencanstyle3 as (
select
     log_key,
     case when concept_id in ('CHOP#2885', 'CHOP#2886') then element_value end as vencanstyle_raw
from
    perfusion_sde
where
     concept_id in ('CHOP#2885', 'CHOP#2886')
),

vencansz3 as (
select
     log_key,
     case when concept_id in ('CHOP#2885', 'CHOP#2886', 'CHOPANES#038') then element_value end as vencansz
from
    perfusion_sde
where
     concept_id in ('CHOP#2885', 'CHOP#2886', 'CHOPANES#038')
),

vencan3_all as (
select distinct
     vencantyp3.log_key,
     3 as sortnum,
     vencantyp,
     vencanstyle_raw,
     cast(coalesce(regexp_replace(vencanstyle_raw, '[' || chr(40) || ',' || chr(41) || ',' || chr(65) || '-'
          || chr(122) || ']', ''), regexp_replace(vencansz, '[' || chr(40) || ',' || chr(41) || ',' || chr(65)
          || '-' || chr(122) || ']', '')) as varchar(64)) as vencansz
from
    vencantyp3
    left join vencanstyle3 on vencantyp3.log_key = vencanstyle3.log_key
    left join vencansz3 on vencantyp3.log_key = vencansz3.log_key
),

vencan3 as (
select
      log_key,
      sortnum,
      row_number() over (partition by log_key order by vencansz) as vencansort,
      vencantyp,
      vencanstyle_raw,
      vencansz
from
     vencan3_all
),

vencan_all as (
select * from vencan1
union all
select * from vencan2
union all
select * from vencan3
)

select
      cases.casenumber,
      case when vencantyp = 'None' then 3495
            when vencantyp = 'Medtronic' then 3496
            when vencantyp = 'Maquet' then 3497
            when vencantyp = 'LivaNova' then 3498
            when vencantyp = 'Edwards' then 3499
            else 3500 end as vencantyp,
      case when lower(vencanstyle_raw) like '%angle%' then 3502
           when vencanstyle_raw like '%St%' then 3501
           when lower(vencanstyle_raw) like '%fem%' then 3503
           when lower(vencanstyle_raw) like '%fr%' then 5439
           else null end as vencanstyle,
      case when vencansz = '10' then 5477
            when vencansz = '12' then 5478
            when vencansz = '14' then 5479
            when vencansz = '15' then 5480
            when vencansz = '16' then 5481
            when vencansz = '17' then 5482
            when vencansz = '18' then 5483
            when vencansz = '19' then 5484
            when vencansz = '20' then 5485
            when vencansz = '21' then 5486
            when vencansz = '22' then 5487
            when vencansz = '23' then 5488
            when vencansz = '24' then 5489
            when vencansz = '25' then 5490
            when vencansz = '26' then 5491
            when vencansz = '27' then 5492
            when vencansz = '28' then 5493
            when vencansz = '29' then 5494
            when vencansz = '30' then 5495
            when vencansz = '31' then 5496
            when vencansz = '32' then 5497
            when vencansz = '34' then 5498
            when vencansz = '36' then 5499
            when vencansz = '38' then 5500
            when vencansz = '40' then 5501
            when vencansz = '29/29' then 5502
            when vencansz = '29/37' then 5503
            when vencansz = '32/46' then 5504
            when vencansz = '34/46' then 5505
            when vencansz = '36/51' then 5506
            when vencansz = '36/52' then 5507
            when vencansz = '29/29/29' then 5508
            when vencansz is not null then 5509
            else null end as vencansz,
      row_number() over (partition by log_id order by vencan_all.sortnum, vencansz) as sortnum
  from
      {{ref('surgery_encounter')}} as surgery_encounter
      inner join vencan_all on surgery_encounter.log_key = vencan_all.log_key
      inner join {{source('ccis_ods', 'centripetus_cases')}} as cases
            on cases.caselinknum = surgery_encounter.log_id
where
    vencanstyle is not null
    and vencansz is not null
    and surgery_encounter.surgery_date >= '2021-10-01'
