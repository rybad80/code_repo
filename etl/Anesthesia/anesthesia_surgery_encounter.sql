with main_anes_provider_setup as (
select
     anesstaff.summary_block_id,
     anesstaff.an_prov_id,
     case when lower(anestype.name) like '%cardiac%anes%attending%' then 1 else 0 end as cardiac_anes_attend_ind,
     case when lower(anestype.name) like '%cardiac%crna%' then 1 else 0 end as cardiac_crna_ind,
     row_number() over (partition by anesstaff.summary_block_id order by line) as prov_key_order
from
     {{ source('clarity_ods', 'an_staff') }} as anesstaff
     inner join {{ source('clarity_ods', 'zc_or_anstaff_type') }} as anestype
          on anestype.anest_staff_req_c = anesstaff.an_prov_type_c
),

main_anes_provider as (
select
     summary_block_id,
     max(cardiac_anes_attend_ind) as cardiac_anes_attend_ind,
     max(cardiac_crna_ind) as cardiac_crna_ind,
     max(case when prov_key_order = 1 then an_prov_id else '0' end) as main_anes_prov_id
from
     main_anes_provider_setup
group by
     summary_block_id
)

select
     stg_surgery_anesthesia.anes_key,
     stg_surgery_anesthesia.anes_visit_key,
     stg_surgery_anesthesia.anesthesia_id,
     stg_surgery.or_key,
     stg_surgery.case_id,
     stg_surgery.log_id,
     stg_surgery.surgery_date,
     stg_surgery.patient_name,
     stg_surgery.mrn,
     stg_surgery.dob,
     surgery_encounter_timestamps.in_room_date,
     coalesce(upper(primanes1.full_nm), upper(primanes2.full_name)) as primary_anesthesiologist,
     upper(room.full_nm) as room,
     initcap(dict_case_service.dict_nm) as service,
     stg_surgery.source_system,
     main_anes_provider.cardiac_anes_attend_ind,
     main_anes_provider.cardiac_crna_ind,
     stg_surgery_anesthesia.case_key,
     stg_surgery_anesthesia.log_key,
     stg_surgery.hsp_acct_key,
     stg_surgery.surgery_csn,
     stg_surgery.csn as admission_csn,
     stg_surgery_anesthesia.anesthesia_csn,
     stg_surgery.pat_key,
     stg_surgery.visit_key,
     stg_surgery.vsi_key,
     stg_surgery.posted_ind
from
     {{ ref('stg_surgery') }} as stg_surgery
     inner join {{ ref('stg_surgery_anesthesia') }} as stg_surgery_anesthesia
          on stg_surgery.or_key = stg_surgery_anesthesia.or_key
     left join {{ ref('surgery_encounter_timestamps') }} as surgery_encounter_timestamps
          on stg_surgery.or_key = surgery_encounter_timestamps.or_key
     left join main_anes_provider
          on stg_surgery_anesthesia.anesthesia_id = main_anes_provider.summary_block_id
     --join in cdw.provider until we fully swap out prov_key
     left join {{source('cdw', 'provider')}} as primanes1
          on stg_surgery_anesthesia.prov_key = primanes1.prov_key
     left join {{ ref('dim_provider') }} as primanes2
          on main_anes_provider.main_anes_prov_id = primanes2.prov_id
     left join {{source('cdw', 'provider')}} as room
          on stg_surgery.room_prov_key = room.prov_key
     left join {{ source('cdw', 'cdw_dictionary') }} as dict_case_service
          on stg_surgery.dict_or_svc_key = dict_case_service.dict_key
