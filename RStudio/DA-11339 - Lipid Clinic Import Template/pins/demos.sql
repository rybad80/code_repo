with lipid_demos as (
select
      mrn --select *
 from 
     cdw_ods..redcap_lipids_v2
where 
    removal_flag is null
    and demographics_complete is not null       
)     

select
      distinct
     encounter_all.mrn as "mrn",
     initcap(last_nm) as "lname",
     initcap(first_nm) as "fname",
     date(encounter_all.dob) as "dob",
     case when encounter_all.sex = 'M'
          then '1'
          when encounter_all.sex = 'F'
          then '2'
		  else '3'
          end as "sex",
     encounter_date,
     '2' as "demographics_complete"
from 
    chop_analytics..encounter_all
    inner join cdwprd..patient on encounter_all.pat_key = patient.pat_key
    left join lipid_demos on lipid_demos.mrn = patient.pat_mrn_id
where 
    (lower(department_name) in ('main lipid heart', 'bgr lipid heart')
    or lower(visit_type) like '%lipid%'
    or (lower(department_name) = 'bgr cardiology' and lower(visit_type) like '%video%visit%' and provider_name in ('Brothers, Julie', 'Shustak, Rachel J'))
    or (lower(department_name) = 'main cardiology' and lower(visit_type) like '%video%visit%' and provider_name in ('Brothers, Julie', 'Shustak, Rachel J'))
    or (lower(department_name) = 'virtua cardiology' and lower(visit_type) like '%lipid%' and provider_name = ('Lee, Hae-rhi')) 
    )
    and encounter_date >= '2023-01-02'
    and appointment_status = 'SCHEDULED'
    and lipid_demos.mrn is null