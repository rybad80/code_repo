with lipid_clinic_appts as (         
select
    mrn,
    min(date(encounter_date)) as encounter_date --select *
from 
    chop_analytics..encounter_all
where 
    (lower(department_name) in ('main lipid heart', 'bgr lipid heart')
    or lower(visit_type) like '%lipid%'
    or (lower(department_name) = 'bgr cardiology' and lower(visit_type) like '%video%visit%' and provider_name in ('Brothers, Julie', 'Shustak, Rachel J'))
    or (lower(department_name) = 'main cardiology' and lower(visit_type) like '%video%visit%' and provider_name in ('Brothers, Julie', 'Shustak, Rachel J'))
    or (lower(department_name) = 'virtua cardiology' and lower(visit_type) like '%lipid%' and provider_name = ('Lee, Hae-rhi')) 
    )
    and encounter_date >= date(now())
    and appointment_status = 'SCHEDULED'
group by
    mrn
),

lipid_results as (
select 
     lipid_clinic_appts.mrn,
     coalesce(date(specimen_taken_date), date(result_date)) as specimen_taken_date,
     max(case when lower(result_component_external_name) like 'cholesterol%' then round(result_value_numeric,0) end) as tc,
     max(case when lower(result_component_name) like 'hdl%cholesterol%' then round(result_value_numeric,0) end) as hdl,
     max(case when lower(result_component_name) like 'ldl%chol%' then round(result_value_numeric,0) end) as ldl,
     max(case when lower(result_component_external_name) like 'triglyceride%' then round(result_value_numeric,0) end) as  tg
from
    lipid_clinic_appts
    inner join chop_analytics..procedure_order_result_clinical as lab_rslt
      on lipid_clinic_appts.mrn = lab_rslt.mrn
where
     lower(procedure_name) like '%lipid%'  
     and result_value <> 'CANCELED'
group by
    lipid_clinic_appts.mrn,
    coalesce(date(specimen_taken_date), date(result_date))     
) ,

other_results as (
select 
     lipid_clinic_appts.mrn,
     coalesce(date(specimen_taken_date), date(result_date)) as specimen_taken_date,
     max(case when lower(result_component_external_name) = 'thyroxine' then round(result_value_numeric,2)  end) as t4,
     max(case when lower(procedure_name) = 'thyroid stimulating hormone' then round(result_value_numeric,2)  end) as tsh,
     max(case when lower(result_component_external_name) = 'alanine aminotransferase' then round(result_value_numeric,0)  end) as alt,
     max(case when lower(result_component_external_name) = 'aspartate aminotransferase' then round(result_value_numeric,0)  end) as ast,
     max(case when lower(result_component_external_name) = 'creatine kinase' then round(result_value_numeric,0)  end) as ck,
     max(case when lower(result_component_external_name) = 'glucose' then round(result_value_numeric,0)  end) as glucose,
     max(case when lower(result_component_external_name) = 'insulin' then round(result_value_numeric,0)  end) as insulin,
     max(case when lower(result_component_external_name) like 'h%b%a1c%' then round(result_value_numeric,1)  end) as hba1c,
     max(case when lower(result_component_external_name) like 'vitamin%d%' then round(result_value_numeric,0)  end) as vitamin_d,
     max(case when lower(result_component_external_name) like 'lipoprotein%a%' then round(result_value_numeric,1)  end) as lpa,
     max(case when lower(result_component_external_name) like 'lipoprotein%b%' then round(result_value_numeric,0)  end) as lpb,
     max(case when lower(result_component_external_name) = 'hgb' then round(result_value_numeric,0)  end) as hgb,
     max(case when lower(result_component_external_name) like 'vitamin%a%' then round(result_value_numeric,0)  end) as vit_a,
     max(case when lower(result_component_external_name) like 'vitamin%e%' then round(result_value_numeric,0)  end) as vit_e,
     max(case when lower(result_component_external_name) like 'vitamin%k%' then round(result_value_numeric,0)  end) as vit_k
from
    lipid_clinic_appts
    inner join chop_analytics..procedure_order_result_clinical as lab_rslt
      on lipid_clinic_appts.mrn = lab_rslt.mrn 
where
     lower(result_component_external_name) in ('thyroxine', 'thyroid stimulating hormone', 'ast','creatine kinase','glucose', 'insulin', 'hgb','alanine aminotransferase', 
     'aspartate aminotransferase' )
     or lower(result_component_name) = 'alanine aminotransferase'
     or lower(result_component_external_name) like 'vitamin%a%'
     or lower(result_component_external_name) like 'vitamin%e%'
     or lower(result_component_external_name) like 'vitamin%k%'
     or lower(result_component_external_name) like 'h%b%a1c%'
     or lower(result_component_external_name) like 'vitamin%d%'
     or lower(result_component_external_name) like 'lipoprotein%a%'
     or lower(result_component_external_name) like 'lipoprotein%b%'
     or lower(procedure_name) = 'thyroid stimulating hormone'
group by
    lipid_clinic_appts.mrn,
    coalesce(date(specimen_taken_date), date(result_date))
),

redcap_labs as (
select
      mrn,
      redcap_repeat_instance,
      lab_date,
      age_labs,
      tc,
      hdl,
      ldl,
      tg,
      nonhdl,
      t4,
      tsh,
      alt,
      ast,
      ck,
      glucose,
      insulin,
      hba1c,
      vitamin_d,
      lpa,
      lpb,
      hgb,
      vit_a,
      vit_e,
      vit_k      
from 
     cdw_ods..redcap_lipids_v2
where 
    removal_flag is null
    and lipid_labs_complete is not null  
),

last_redcap_lab as (

select
      mrn,
      max(redcap_repeat_instance) as last_redcap_instance
from 
     cdw_ods..redcap_lipids_v2     
where 
    removal_flag is null
    and lipid_labs_complete is not null     
group by
     mrn       
)       

select distinct
      lipid_clinic_appts.mrn as "mrn",
      'lipid_labs' as "redcap_repeat_instrument",
      row_number() over (partition by lipid_clinic_appts.mrn order by lipid_results.specimen_taken_date asc) + coalesce(last_redcap_instance,0) as "redcap_repeat_instance",      
      lipid_results.specimen_taken_date as "lab_date",
      coalesce(cast(lipid_results.tc as varchar(10)),'') as "tc",
      coalesce(cast(lipid_results.hdl as varchar(10)),'') as "hdl",
      coalesce(cast(lipid_results.ldl as varchar(10)),'') as "ldl",
      coalesce(cast(lipid_results.tg as varchar(10)),'') as "tg",
      coalesce(cast(other_results.t4 as varchar(10)),'') as "t4",
      coalesce(cast(other_results.tsh as varchar(10)),'') as "tsh",
      coalesce(cast(other_results.alt as varchar(10)),'') as "alt",
      coalesce(cast(other_results.ast as varchar(10)),'') as "ast",
      coalesce(cast(other_results.ck as varchar(10)),'') as "ck",
      coalesce(cast(other_results.glucose as varchar(10)),'') as "glucose",
      coalesce(cast(other_results.insulin as varchar(10)),'') as "insulin",
      coalesce(cast(other_results.hba1c as varchar(10)),'') as "hba1c",
      coalesce(cast(other_results.vitamin_d as varchar(10)),'') as "vitamin_d",
      coalesce(cast(other_results.lpa as varchar(10)),'') as "lpa",
      coalesce(cast(other_results.lpb as varchar(10)),'') as "lpb",
      coalesce(cast(other_results.hgb as varchar(10)),'') as "hgb",
      coalesce(cast(other_results.vit_a as varchar(10)),'') as "vit_a",
      coalesce(cast(other_results.vit_e as varchar(10)),'') as "vit_e",
      coalesce(cast(other_results.vit_k as varchar(10)),'') as "vit_k",
      lipid_clinic_appts.encounter_date,
     '2' as "lipid_labs_complete"

from
    lipid_clinic_appts
    inner join lipid_results
      on lipid_clinic_appts.mrn = lipid_results.mrn 
    left join other_results
      on lipid_clinic_appts.mrn = other_results.mrn
        and date(lipid_results.specimen_taken_date) = date(other_results.specimen_taken_date)
    left join redcap_labs 
      on redcap_labs.mrn = lipid_clinic_appts.mrn
         and date(redcap_labs.lab_date) = date(lipid_results.specimen_taken_date)
    left join last_redcap_lab
      on lipid_clinic_appts.mrn = last_redcap_lab.mrn     
where
     redcap_labs.lab_date is null         
