with perfusion_sde as (
select
          anes_visit_key,
          log_key,
          seq_num,
          concept_id,
          concept_description,
          element_value
 from
      {{ref('cardiac_perfusion_smart_data_element')}}
where
     concept_id in ('CHOP#4835', 'CHOP#2867', 'CHOP#2868', 'CHOP#2866', 'CHOP#2870',
                    'CHOP#2871', 'CHOP#2869', 'CHOPANES#034', 'CHOPANES#035')

),

artcantyp1 as (
select
     log_key,
     case when concept_id = 'CHOP#2866' then element_value end as artcantyp
from
    perfusion_sde
where
     concept_id = 'CHOP#2866'
),


artcansz1 as (
select
     log_key,
     case when concept_id in ('CHOP#2867', 'CHOP#2868', 'CHOP#4835', 'CHOPANES#034')
          then element_value end as artcansz_raw
from
    perfusion_sde
where
     concept_id in ('CHOP#2867', 'CHOP#2868', 'CHOP#4835', 'CHOPANES#034')
),

artcan1_all as (
select distinct
     artcantyp1.log_key,
     1 as sortnum,
     artcantyp,
     artcansz_raw,
     regexp_replace(artcansz_raw, '[' || chr(65) || '-' || chr(122) || ']', '') as artcansz
from
    artcantyp1
    left join artcansz1 on artcantyp1.log_key = artcansz1.log_key
),

artcan1 as (
select
      log_key,
      sortnum,
      row_number() over (partition by log_key order by artcansz) as artcansort,
      artcantyp,
      artcansz_raw,
      artcansz
from
     artcan1_all
),

artcantyp2 as (
select
     log_key,
     case when concept_id = 'CHOP#2869' then element_value end as artcantyp
from
    perfusion_sde
where
     concept_id = 'CHOP#2869'
),


artcansz2 as (
select
     log_key,
     case when concept_id in ('CHOP#2870', 'CHOP#2871', 'CHOPANES#035') then element_value end as artcansz_raw
from
    perfusion_sde
where
     concept_id in ('CHOP#2870', 'CHOP#2871', 'CHOPANES#035')
),

artcan2_all as (
select distinct
     artcantyp2.log_key,
     2 as sortnum,
     artcantyp,
     artcansz_raw,
     regexp_replace(artcansz_raw, '[' || chr(65) || '-' || chr(122) || ']', '') as artcansz
from
    artcantyp2
    left join artcansz2 on artcantyp2.log_key = artcansz2.log_key
),

artcan2 as (
select
      log_key,
      sortnum,
      row_number() over (partition by log_key order by artcansz) as artcansort,
      artcantyp,
      artcansz_raw,
      artcansz
from
     artcan2_all
),

artcan_all as (
select * from artcan1

union all

select * from artcan2
)

select
      coalesce(emrlinkid.casenumber, cases.casenumber) as casenumber,
      case when artcantyp like '%Medtronic%Biomedicus%' then 5465
            when artcantyp like '%Medtronic%DLP%' then 5466
            when artcantyp = 'Medtronic - NextGen' then 5467
            when artcantyp = 'LivaNova - Curved Tip Aortic Arch Cannula' then 5468
            when artcantyp = 'LivaNova - Straight Tip Aortic Arch Cannula' then 5469
            when artcantyp = 'LivaNova - 135 Curved Tip Aortic Arch Cannula' then 5470
            when artcantyp = 'LivaNova - Beveled Tip Polyurethane Cannula' then 5471
            when artcantyp = 'LivaNova - Straight Tip Polyurethane Cannula' then 5472
            when artcantyp like '%Edwards%' then 5473
            else 5476 end as artcantyp,
      case when artcansz = '5' then 5445
            when artcansz = '6' then 5446
            when artcansz = '8' then 5447
            when artcansz = '9' then 5448
            when artcansz = '10' then 5449
            when artcansz = '11' then 5450
            when artcansz = '12' then 5451
            when artcansz = '14' then 5452
            when artcansz = '15' then 5453
            when artcansz = '16' then 5454
            when artcansz = '17' then 5455
            when artcansz = '18' then 5456
            when artcansz = '19' then 5457
            when artcansz = '20' then 5458
            when artcansz = '21' then 5459
            when artcansz = '22' then 5460
            when artcansz = '23' then 5461
            when artcansz = '24' then 5462
            when artcansz = '25' then 5463
            else 5464 end as artcansz,
      row_number() over (partition by log_id order by artcan_all.sortnum, artcansz) as sortnum
  from
       {{ref('surgery_encounter')}} as surgery_encounter
       inner join artcan_all on surgery_encounter.log_key = artcan_all.log_key
       left join {{source('ccis_ods', 'centripetus_cases')}} as cases
          on cases.caselinknum = surgery_encounter.log_id
       left join {{source('ccis_ods', 'centripetus_emrlinkid')}} as emrlinkid
          on emrlinkid.emreventid = surgery_encounter.log_id
where
       surgery_encounter.surgery_date >= '2021-10-01'
