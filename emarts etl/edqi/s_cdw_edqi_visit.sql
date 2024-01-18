{{ config(database=env_var('CDW_STG_DB', 'CDW_STG_UAT')) }}
with observation as (
    select
      s_cdw_edqi_visit_ed_event.visit_key,
      min(event_dt) as first_observation_order_dt
    from
      {{source('cdw', 'visit_ed_event')}} as visit_ed_event
      inner join {{source('cdw', 'master_event_type')}} as master_event_type on master_event_type.event_type_key = visit_ed_event.event_type_key
      inner join {{ref('s_cdw_edqi_visit_ed_event')}} as s_cdw_edqi_visit_ed_event on s_cdw_edqi_visit_ed_event.visit_key = visit_ed_event.visit_key
    where
      master_event_type.event_id = 300234
      and visit_ed_event.event_stat is null
      and visit_ed_event.event_dt < coalesce(s_cdw_edqi_visit_ed_event.depart_ed_dt, current_date)
    group by
      s_cdw_edqi_visit_ed_event.visit_key
), -- select * from observation limit 100; 

-- Cleaning hospital account information to account for duplicates in source data
primary_hsp_account_clean as (
     select distinct
      vw_ed.visit_key as primary_hsp_acct_visit_key,
      first_value(hospital_account.hsp_acct_key) over(partition by vw_ed.visit_key
                                                      order by days_between(coalesce(vw_ed.arrive_ed_dt, '19000101'), hospital_account.adm_dt) asc,
                                                               hospital_account.tot_chrgs desc
                                                      ) as primary_hsp_acct_key,
      first_value(hospital_account.disch_dept_key) over(partition by vw_ed.visit_key
                                                       order by days_between(coalesce(vw_ed.arrive_ed_dt, '19000101'), hospital_account.adm_dt) asc,
                                                               hospital_account.tot_chrgs desc
                                                       ) as disch_dept_key
    from
      {{ref('s_cdw_edqi_visit_ed_event')}} as vw_ed
      inner join {{source('cdw', 'hospital_account_visit')}} as hospital_account_visit on vw_ed.visit_key = hospital_account_visit.visit_key
      left join {{source('cdw', 'hospital_account')}} as hospital_account on hospital_account_visit.hsp_acct_key = hospital_account.hsp_acct_key
    where
      hospital_account_visit.pri_visit_ind = 1
),

admission_department as (
     select
          s_cdw_edqi_visit_ed_event.visit_key,
          visit_event.dept_key as admit_dept_key,
          department.dept_nm as admit_dept_nm,
          row_number() over (partition by s_cdw_edqi_visit_ed_event.visit_key 
          order by eff_event_dt, visit_event.visit_event_key) as row_num
     from
          {{ref('s_cdw_edqi_visit_ed_event')}} as s_cdw_edqi_visit_ed_event
          inner join {{source('cdw','visit_event')}} as visit_event 
               on visit_event.visit_key = s_cdw_edqi_visit_ed_event.visit_key
          inner join {{source('cdw','cdw_dictionary')}} as adt_event_type 
               on adt_event_type.dict_key = visit_event.dict_adt_event_key
          inner join {{source('cdw','cdw_dictionary')}} as adt_event_subtype
               on adt_event_subtype.dict_key = visit_event.dict_event_subtype_key
          inner join {{source('cdw','cdw_dictionary')}} as pat_class
               on pat_class.dict_key = visit_event.dict_pat_class_key
          inner join {{source('cdw','department')}} as department
               on department.dept_key = visit_event.dept_key
     where -- MAIN ED, KOPH ED, MAIN TRANSPORT
          department.dept_id not in (10292012, 101003001, 101001032) 
          and adt_event_type.src_id = 3 --Transfer In
          and adt_event_subtype.src_id != 2 -- Canceled
          and pat_class.src_id != 4 -- Day Surgery
)

select
  visit.visit_key,
  visit.pat_key,
  coalesce(primary_hsp_account_clean.primary_hsp_acct_key, 0)::bigint as hsp_acct_key,
  visit.enc_id,
  visit.age,
  visit.age_days,
  case
     when visit.age_days < 30 then '1 Neonate(<30 days)'
     when visit.age_days >= 30 and visit.age < 1 then '2 Infancy(>=30 days and <1 year)'
     when visit.age >= 1 and visit.age < 5 then '3 Early Childhood(>=1 year and <5 years)'
     when visit.age >= 5 and visit.age < 13 then '4 Late Childhood(>=5 years and <13 years)'
     when visit.age >= 13 and visit.age < 18 then '5 Adolescence(>=13 years and <18 years)'
     when visit.age >= 18 and visit.age < 30 then '6 Adult(>=18 years and <30)'
     when visit.age >= 30 then '7 Adult(>=30 years)'
     else '8 Invalid'
  end as pediatric_age_days_group,
  case
      when s_cdw_edqi_adt.edecu_bed_ind = 1  -- Must have EDECU bed
        then coalesce(first_observation_order_dt, visit_addl_info.edecu_arrvl_dt)
  end as edecu_arrvl_dt, -- Note: This is referenced in cases below (hence no table qualification)
  visit_addl_info.dict_acuity_key,
  visit_addl_info.dict_dspn_key,
  admission_department.admit_dept_key,
  admission_department.admit_dept_nm,
  -- This had a bunch of logic, but all of it evaluated to 1 else 1 (so just the 0 is relevant)
  case
     when lower(disposition.dict_nm) like 'lwbs%' then 0
     else 1
  end as ed_patients_seen_ind,
  case
     when (visit_addl_info.dict_dspn_key in (-1, 0, -2)
           or lower(disposition.dict_nm) = 'error'
          )
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is null then 'Indeterminate'
     when (visit_addl_info.dict_dspn_key in (-1, 0, -2)
           or lower(disposition.dict_nm) = 'error'
          )
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is not null then 'Admit'
     when (visit_addl_info.dict_dspn_key in (-1, 0, -2)
           or lower(disposition.dict_nm) = 'error'
          )
          and edecu_arrvl_dt is not null
          and admission_department.admit_dept_nm is null then 'EDECU - Discharge'
     when (visit_addl_info.dict_dspn_key in (-1, 0, -2)
           or lower(disposition.dict_nm) = 'error'
          )
          and edecu_arrvl_dt is not null
          and admission_department.admit_dept_nm is not null then 'EDECU - Admit'
     when lower(disposition.dict_nm) = 'transfered to another facility(not from triage)'
          and edecu_arrvl_dt is not null then 'EDECU - Transfered to Another Facility(Not from Triage)'
     when edecu_arrvl_dt is not null
          and lower(disch_dept.dept_abbr) in ('edec', 'ed', 'ked') then 'EDECU - Discharge'
     when edecu_arrvl_dt is not null
          and lower(disch_dept.dept_abbr) <> 'edec'
          and admission_department.admit_dept_nm is null then 'EDECU - Admit'
     when edecu_arrvl_dt is not null
          and admission_department.admit_dept_nm is not null then 'EDECU - Admit'
     when edecu_arrvl_dt is not null
          and admission_department.admit_dept_nm is null then 'EDECU - Discharge'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is not null
          and lower(disch_dept.dept_abbr) <> 'edecu' then 'EDECU - Admit'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is not null
          and lower(disch_dept.dept_abbr) = 'edecu' then 'EDECU - Discharge'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is null
          and lower(disch_dept.dept_abbr) = 'periop' then 'Admit'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is null then 'Discharge'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is not null then 'Admit'
     when lower(disposition.dict_nm) = 'hacu'
          and admission_department.admit_dept_nm is null then 'Transfer from Triage to HACU'
     when lower(disposition.dict_nm) = 'hacu'
          and admission_department.admit_dept_nm is not null then 'Admit'
     when admission_department.admit_dept_nm is not null then 'Admit'
     else disposition.dict_nm
  end as ed_disposition,
  -- Some logic below is redundant, but keeping it to more easily pivot in alignment with ed_disposition
  case
     when (visit_addl_info.dict_dspn_key in (-1, 0, -2)
           or lower(disposition.dict_nm) = 'error'
          )
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is null then 'DISCHARGE'
     when (visit_addl_info.dict_dspn_key in (-1, 0, -2)
           or lower(disposition.dict_nm) = 'error'
          )
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is not null then 'ADMIT'
     when (visit_addl_info.dict_dspn_key in (-1, 0, -2)
           or lower(disposition.dict_nm) = 'error'
          )
          and edecu_arrvl_dt is not null
          and admission_department.admit_dept_nm is null then 'EDECU'
     when (visit_addl_info.dict_dspn_key in (-1, 0, -2)
           or lower(disposition.dict_nm) = 'error'
          )
          and edecu_arrvl_dt is not null
          and admission_department.admit_dept_nm is not null then 'EDECU'
     when lower(disposition.dict_nm) = 'transfered to another facility(not from triage)'
          and edecu_arrvl_dt is not null then 'EDECU'
     when edecu_arrvl_dt is not null
          and lower(disch_dept.dept_abbr) in ('edec', 'ed', 'ked') then 'EDECU'
     when edecu_arrvl_dt is not null
          and lower(disch_dept.dept_abbr) <> 'edec'
          and admission_department.admit_dept_nm is null then 'EDECU'
     when edecu_arrvl_dt is not null
          and admission_department.admit_dept_nm is not null then 'EDECU'
     when edecu_arrvl_dt is not null
          and admission_department.admit_dept_nm is null then 'EDECU'
     when observation.first_observation_order_dt is not null
          and s_cdw_edqi_adt.edecu_bed_ind = 1  -- Must have EDECU bed
       then 'EDECU'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is not null
          and lower(disch_dept.dept_abbr) <> 'edecu' then 'EDECU'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is not null
          and lower(disch_dept.dept_abbr) = 'edecu' then 'EDECU'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is null
          and lower(disch_dept.dept_abbr) = 'periop' then 'ADMIT'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is null then 'DISCHARGE'
     when lower(disposition.dict_nm) in ('admit', 'or', 'edecu')
          and edecu_arrvl_dt is null
          and admission_department.admit_dept_nm is not null then 'ADMIT'
     when lower(disposition.dict_nm) = 'hacu'
          and admission_department.admit_dept_nm is null then 'TRANSFER FROM TRIAGE'
     when lower(disposition.dict_nm) = 'hacu'
          and admission_department.admit_dept_nm is not null then 'ADMIT'
     when lower(disposition.dict_nm) like '%eloped%' then 'DISCHARGE'
     when lower(disposition.dict_nm) like 'lwbs%' then 'LWBS'
     when lower(disposition.dict_nm) like 'transfered%' then 'TRANSFER NOT FROM TRIAGE'
     when lower(disposition.dict_nm) like 'transfer %' then 'TRANSFER FROM TRIAGE'
     when lower(disposition.dict_nm) like 'dece%' then 'DECEASED'
     when admission_department.admit_dept_nm is not null then 'ADMIT'
     else upper(disposition.dict_nm)
  end as ed_general_disposition,
  -- This had a bunch of logic, but all of it evaluated to 1...
  1 as ed_patients_presenting_ind,
  case
     when lower(visit_addl_info.cuml_room_nm) like '%ed res%' then 1  -- Main ED
     when lower(visit_addl_info.cuml_room_nm) like '%kedres%' then 1  -- KOPH ED
     else 0
  end as ed_resuscitation_rm_use_ind
from
  {{ref('s_cdw_edqi_visit_ed_event')}} as vw_ed
  left join {{ref('s_cdw_edqi_adt')}} as s_cdw_edqi_adt on vw_ed.visit_key = s_cdw_edqi_adt.visit_key
  left join {{source('cdw', 'visit')}} as visit on vw_ed.visit_key = visit.visit_key
  left join {{source('cdw', 'visit_addl_info')}} as visit_addl_info on vw_ed.visit_key = visit_addl_info.visit_key
  left join {{source('cdw', 'cdw_dictionary')}} as encpatacuity on visit_addl_info.dict_acuity_key = encpatacuity.dict_key
  left join {{source('cdw', 'cdw_dictionary')}} as disposition on visit_addl_info.dict_dspn_key = disposition.dict_key
  left join admission_department on admission_department.visit_key = vw_ed.visit_key and admission_department.row_num = 1
  left join primary_hsp_account_clean on vw_ed.visit_key = primary_hsp_account_clean.primary_hsp_acct_visit_key
  left join {{source('cdw', 'department')}} as disch_dept on primary_hsp_account_clean.disch_dept_key = disch_dept.dept_key
  left join {{source('cdw', 'department')}} as dep on visit_addl_info.last_dept_key = dep.dept_key
  left join {{source('cdw', 'location')}} as loc on dep.rev_loc_key = loc.loc_key
  left join observation on vw_ed.visit_key = observation.visit_key
where
  (vw_ed.arrive_ed_dt >= '01/01/2011' and (vw_ed.arrive_ed_dt <= now() ) or vw_ed.arrive_ed_dt is null)
  and visit_addl_info.ed_cancelled_visit_ind <> 1
  and loc.loc_id in (1001.000,  -- CHILDREN''S HOSPITAL OF PHILADELPHIA RL
                     1003.000   -- KING OF PRUSSIA RL
                    )
