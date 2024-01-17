with implant_log_details as (
select
     impl_key,
     max(case when implant_action.src_id = 1
          then log_key end) as implant_log_key,
     max(case when implant_action.src_id = 2
          then log_key end) as explant_log_key,
     upper(laterality.dict_nm) as laterality,
     upper(impl_area.dict_nm) as implant_area

from
     {{source('cdw', 'or_log_implants')}} as or_log_implants
     left join {{source('cdw', 'cdw_dictionary')}} as implant_action
               on or_log_implants.dict_impl_actn_key = implant_action.dict_key
     left join {{source('cdw', 'cdw_dictionary')}} as impl_area
               on or_log_implants.dict_impl_area_key = impl_area.dict_key
     left join {{source('cdw', 'cdw_dictionary')}} as laterality
               on or_log_implants.dict_or_lateral_key = laterality.dict_key
group by
      impl_key,
      laterality.dict_nm,
      impl_area.dict_nm
)

select
       patient_all.patient_name,
       date(patient_all.dob) as dob,
       patient_all.mrn,
       impl_id as implant_id,
       impl_nm as implant_name,
       implant_log_key,
       explant_log_key,
       laterality,
       implant_area,
       manufacturer.dict_nm as manufacturer_name,
       supplier.dict_nm as supplier_name,
       model_num as model_number,
       implant_date.full_dt as implant_date,
       explant_date.full_dt as explant_date,
       impl_size as implant_size,
       impl_desc as implant_description,
       impl_type.dict_nm as implant_type,
       impl_sn as serial_number,
       lot_num as lot_number,
       upper(or_imp.static_udi) as device_identifier,
       upper(impl_status.dict_nm) as implant_status,
       case when deceased_ind = 1 then 'No'
            when deceased_ind = 0 then 'Yes' end as patient_alive,
       death_dt as death_date
from
        {{source('cdw', 'or_implant')}} as or_implant
        left join implant_log_details
                  on implant_log_details.impl_key = or_implant.impl_key
        left join {{source('cdw', 'cdw_dictionary')}} as impl_type
                  on or_implant.dict_or_impl_type_key = impl_type.dict_key
        left join {{source('cdw', 'cdw_dictionary')}} as impl_status
                  on or_implant.dict_or_impl_stat_key = impl_status.dict_key
        left join {{source('cdw', 'cdw_dictionary')}} as manufacturer
                  on or_implant.dict_or_manuf_key = manufacturer.dict_key
        left join {{source('cdw', 'cdw_dictionary')}} as supplier
                  on or_implant.dict_or_supplier_key = supplier.dict_key
        left join {{source('cdw', 'or_log_implant_implanted')}} as or_log_implant_implanted
                  on or_implant.impl_key = or_log_implant_implanted.impl_key
        left join {{source('cdw', 'or_log_implant_explanted')}} as or_log_implant_explanted
                  on or_implant.impl_key = or_log_implant_explanted.expl_key
        left join {{source('cdw', 'or_log_implant_wasted')}} as or_log_implant_wasted
                  on or_implant.impl_key = or_log_implant_wasted.impl_key
        left join {{ref('patient_all')}} as patient_all
                  on or_implant.pat_key = patient_all.pat_key
        left join {{source('cdw', 'patient')}} as patient
                  on patient.pat_key = patient_all.pat_key
        left join {{ source('clarity_ods', 'or_imp')}} as or_imp
                  on or_imp.implant_id = or_implant.impl_id
        left join {{source('cdw', 'or_implant_description')}} as or_implant_description
                  on or_implant_description.impl_key = or_implant.impl_key
        left join {{source('cdw', 'master_date')}} as implant_date
                  on implant_date.dt_key = or_log_implant_implanted.impl_dt_key
        left join {{source('cdw', 'master_date')}} as explant_date
                  on explant_date.dt_key = or_log_implant_explanted.explant_dt_key
