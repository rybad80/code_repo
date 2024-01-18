with perfusion_sde as (
 select
        anes_visit_key,
        log_key,
        concept_id,
        concept_description,
        element_value
 from
      {{ref('cardiac_perfusion_smart_data_element')}}
where
      concept_id in ('CHOP#2865', 'CHOP#2855')
),

ac_vc_size_raw as (
select
     log_key,
     substring(element_value, 1, instr(element_value, ' x ', 1) - 1) as artlinesz_raw,
     substring(element_value, instr(element_value, ' x ', 1) + 3, length(element_value)
       - instr(element_value, ' x ', 1) + 2) as venlinesz_raw
from
    perfusion_sde
where
     concept_id = 'CHOP#2865'
),

ac_vc_size as (
select
     log_key,
     artlinesz_raw as artlinesz,
     venlinesz_raw as venlinesz
from
    ac_vc_size_raw
),

oxygenator as (
select
     log_key,
     element_value as oxygenatorty
from
    perfusion_sde
where
     concept_id = 'CHOP#2855'
),

circuit as (
select
     surgery.log_key,
     ac_vc_size.artlinesz,
     ac_vc_size.venlinesz,
     oxygenator.oxygenatorty

 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      inner join {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
      inner join ac_vc_size on ac_vc_size.log_key = surgery.log_key
      inner join oxygenator on oxygenator.log_key = surgery.log_key
),

fluids_union as (
select
      or_log.log_id,
      5518 as primefluid,
      case when circuit.oxygenatorty = 'FX05' and artlinesz = '1/8' and venlinesz = '3/16' then 115
           when circuit.oxygenatorty = 'FX05' and artlinesz = '3/16' and venlinesz = '3/16' then 135
           when circuit.oxygenatorty = 'FX05' and artlinesz = '3/16' and venlinesz = '1/4' then 155
           when circuit.oxygenatorty = 'FX05' and artlinesz = '1/4' and venlinesz = '1/4' then 175
           when circuit.oxygenatorty = 'FX15' and artlinesz = '1/4' and venlinesz = '3/8' then 340
           when circuit.oxygenatorty = 'FX15' and artlinesz = '3/8' and venlinesz = '3/8' then 650
           when circuit.oxygenatorty = 'FX25' and artlinesz = '3/8' and venlinesz = '3/8' then 800
           when circuit.oxygenatorty = 'FX25' and artlinesz = '3/8' and venlinesz = '1/2' then 950
      end as primefluidvol,
      1 as sortnum
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      inner join {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
      inner join {{source('cdw','or_log')}} as or_log on or_log.log_key = surgery.log_key
      inner join circuit on circuit.log_key = surgery.log_key
where
      perfusion_date >= '2021-10-01'

union all

select
      or_log.log_id,
      5510 as primefluid,
      50 as primefluidvol,
      2
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      inner join {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
      inner join {{source('cdw','or_log')}} as or_log on or_log.log_key = surgery.log_key
where
      perfusion_date >= '2021-10-01'
)


select
       coalesce(emrlinkid.casenumber, cases.casenumber) as casenumber,
       primefluid,
       primefluidvol,
       sortnum
from
      fluids_union
      left join {{source('ccis_ods', 'centripetus_cases')}} as cases
          on cases.caselinknum = fluids_union.log_id
      left join {{source('ccis_ods', 'centripetus_emrlinkid')}} as emrlinkid
          on emrlinkid.emreventid = fluids_union.log_id
where
      primefluidvol is not null
