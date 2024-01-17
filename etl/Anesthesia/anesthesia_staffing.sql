select
     anesthesia_encounter_link.anes_id as anesthesia_id,
     stg_surgery_log.log_id,
     provider.prov_name as anesthesia_staff_name,
     anestype.name as anesthesia_staff_type,
     an_begin_local_dttm as anesthesia_staff_begin_date,
     an_end_local_dttm as anesthesia_staff_end_date
   from
        {{ref('stg_surgery')}} as stg_surgery_log
        inner join {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
                  on stg_surgery_log.log_key = anesthesia_encounter_link.or_log_key
        inner join {{source('clarity_ods', 'an_staff')}} as anesstaff
                  on anesthesia_encounter_link.anes_id = anesstaff.summary_block_id
        inner join {{source('clarity_ods', 'zc_or_anstaff_type')}} as anestype
                  on anestype.anest_staff_req_c = anesstaff.an_prov_type_c
        left join {{source('clarity_ods', 'clarity_ser')}} as provider
                  on provider.prov_id = anesstaff.an_prov_id
