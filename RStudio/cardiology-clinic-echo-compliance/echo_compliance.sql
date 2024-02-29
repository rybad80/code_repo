with 
holter_ttm as (  
select      
      procedure_order_clinical.mrn,
      placed_date,
      provider.full_nm as ordering_provider,
      procedure_order_clinical.procedure_name
  from 
     chop_analytics..procedure_order_clinical
     inner join procedure_order_appointment
       on procedure_order_clinical.proc_ord_key = procedure_order_appointment.proc_ord_key
     left join procedure_order
       on procedure_order_clinical.proc_ord_key = procedure_order.proc_ord_key
     left join provider
       on provider.prov_key = procedure_order.auth_prov_key   
where 
     procedure_order_clinical.procedure_name in ('HOLTER MONITOR', 'OP CARD TRANSTELEPHONIC MONITOR')
    
)

,
clinic_echo_holter_ttm_raw as (
select
      cardiac_clinic_encounters.mrn,
      cardiac_clinic_encounters.csn,
      cardiac_clinic_encounters.encounter_date as clinic_date,
      cardiac_clinic_encounters.encounter_date+30 as clinic_date_plus30,
      cardiac_clinic_encounters.provider_name as clinic_provider,
      cardiac_clinic_encounters.department_name as clinic_location,      
      case when cardiac_clinic_encounters.visit_key is not null then 1 else 0 end as cardiac_clinic_encounter_ind,
      cardiac_echo.study_date as echo_date,
      provider.full_nm as echo_ordering_provider,
      procedure_order_clinical.procedure_name as echo_procedure,      
      holter_ttm.placed_date as holter_ttm_date,
      holter_ttm.ordering_provider as holter_ttm_ordering_provider, 
      holter_ttm.procedure_name as holter_ttm_procedure,    
      case when lower(cardiac_clinic_encounters.provider_name) = lower(provider.full_nm) then 1 else 0 end as echo_provider_match,
      case when cardiac_echo.cardiac_study_id is not null then 1 else 0 end as echo_30day_flag,
      case when lower(cardiac_clinic_encounters.provider_name) = lower(holter_ttm.ordering_provider) then 1 else 0 end as holter_ttm_provider_match,
      case when holter_ttm.mrn is not null then 1 else 0 end as holter_ttm_30day_flag      
from  
     chop_analytics..cardiac_clinic_encounters
     left join chop_analytics..cardiac_echo
       on cardiac_clinic_encounters.mrn = cardiac_echo.mrn
       and cardiac_echo.study_date between cardiac_clinic_encounters.encounter_date and cardiac_clinic_encounters.encounter_date+30       
     left join chop_analytics..procedure_order_clinical
       on procedure_order_clinical.procedure_order_id = cardiac_echo.procedure_order_id
     left join procedure_order
       on procedure_order_clinical.proc_ord_key = procedure_order.proc_ord_key
     left join provider
       on provider.prov_key = procedure_order.auth_prov_key   
     left join holter_ttm
       on cardiac_clinic_encounters.mrn = holter_ttm.mrn
       and holter_ttm.placed_date between cardiac_clinic_encounters.encounter_date and cardiac_clinic_encounters.encounter_date+30          
      
where
    appointment_date >= '2023-01-01' --and '2023-02-28'
    and appointment_status in ('COMPLETED', 'ARRIVED')
    and visit_type in ('NPV', 'NPV CARD', 'NEW PATIENT VISIT', 'MYCHOP CARDIOLOGY NEW')
    and cardiac_clinic_encounters.department_name NOT IN  ('MAIN FETAL HEART', 'MAIN CARDIOLOGY CLINIC', 'BGR CARDIOLOGY CLINIC')

)

select
      mrn,
      csn,
      clinic_date,
      clinic_provider,
      clinic_location,
      cardiac_clinic_encounter_ind,
      min(echo_date) as echo_date,
      max(echo_ordering_provider) as echo_ordering_provider,      
      max(echo_provider_match) as provider_match_ind,
      max(echo_30day_flag) as echo_30_day_ind,
      max(case when echo_provider_match = 1 and echo_30day_flag = 1 then 1 else 0 end) as provider_order_echo_30_day_ind,
      min(holter_ttm_date) as holter_ttm_date,
      max(holter_ttm_ordering_provider) as holter_ttm_ordering_provider,      
      max(holter_ttm_provider_match) as holter_ttm_provider_match_ind,
      max(holter_ttm_30day_flag) as holter_ttm_30_day_ind,
      max(case when holter_ttm_provider_match = 1 and holter_ttm_30day_flag = 1 then 1 else 0 end) as provider_order_holter_ttm_30_day_ind      
from
      clinic_echo_holter_ttm_raw
group by 
      mrn,
      csn,
      clinic_date,
      clinic_provider,
      clinic_location,
      cardiac_clinic_encounter_ind