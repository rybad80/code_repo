-- sql qualifier for m_CDW_update_VISIT
with last_stay_class as (
    select
      visit_key_lookup.visit_key,
      case
      when
          stg_pat_enc.enc_type_c in (
            101, --'office visit'
            50,  --'appointment'
            155  --'confidential visit'
          )
          and stg_pat_enc.appt_status_c in (
            2,  --'completed',
            6   --'arrived'
          )
          and hospital_account_visit.pri_visit_ind is null
      then
          'non-hospital op encounter'
      when
          stg_pat_enc.enc_type_c in (
            3,    -- 'hospital encounter',
            101,  -- 'office visit',
            1058, -- 'procedure only',
            50    --'appointment'
          )
          and hospital_account_visit.pri_visit_ind = 1
          and coalesce(appt_status_c, -2) in (
            2,  --'completed',
            -2, --'not applicable',
            6   --'arrived'
          )
          and visit_addl_info.adt_pat_class_c in (
            1,  --'inpatient',
            7,  --'admit after surgery',
            10, --'admit after surgery-ip',
            9   --'ip deceased organ donor'
          )
          and stg_pat_enc.hosp_admsn_time is not null
      then
          'hospital ip encounter'
      when
          stg_pat_enc.enc_type_c in (
            3,    -- 'hospital encounter',
            101,  -- 'office visit',
            1058, -- 'procedure only',
            50    --'appointment'
          )
          and hospital_account_visit.pri_visit_ind = 1
          and coalesce(stg_pat_enc.appt_status_c, -2) in (
            2,  --'completed',
            -2, --'not applicable',
            6   --'arrived'
          )
          and visit_addl_info.adt_pat_class_c in (
            2,  --'outpatient',
            4   -- 'day surgery'
            )
          and stg_pat_enc.hosp_admsn_time is not null
      then
          'hospital op encounter'
      when
          stg_pat_enc.enc_type_c in (
            3,    -- 'hospital encounter',
            101,  -- 'office visit',
            1058, -- 'procedure only',
            50    --'appointment'
          )
          and hospital_account_visit.pri_visit_ind = 0
          and coalesce(stg_pat_enc.appt_status_c, -2) in (
            2,  --'completed',
            -2, --'not applicable',
            6   --'arrived'
          )
          and visit_addl_info.adt_pat_class_c = 6 --'recurring outpatient'
          and stg_pat_enc.hosp_admsn_time is not null
      then
          'hospital op encounter'
      when
          stg_pat_enc.enc_type_c in (
            3,    -- 'hospital encounter',
            101,  -- 'office visit',
            1058, -- 'procedure only',
            50    --'appointment'
          )
          and hospital_account_visit.pri_visit_ind = 1
          and coalesce(stg_pat_enc.appt_status_c, -2) in (
            2,  --'completed',
            -2, --'not applicable',
            6   --'arrived'
          )
          and visit_addl_info.adt_pat_class_c = 3 --'emergency'
          and stg_pat_enc.hosp_admsn_time is not null
      then
          'hospital ed encounter'
      when
          stg_pat_enc.enc_type_c in (
            3,    -- 'hospital encounter',
            101,  -- 'office visit',
            1058, -- 'procedure only',
            50    --'appointment'
          )
          and hospital_account_visit.pri_visit_ind = 1
          and coalesce(stg_pat_enc.appt_status_c, -2) in (
            2,  --'completed',
            -2, --'not applicable',
            6   --'arrived'
          )
          and visit_addl_info.adt_pat_class_c in (
            5,  --'observation',
            8   --'admit after surgery-obs'
          )
          and stg_pat_enc.hosp_admsn_time is not null
      then
          'hospital obs encounter'
      end as last_encounter_stay_class,
      case
        when last_encounter_stay_class is not null
        then 1
        else 0
      end as last_encounter_stay_indicator
    from
      {{ref('stg_pat_enc')}} as stg_pat_enc
      left join {{ref('stg_visit_key_lookup')}} as visit_key_lookup
        on visit_key_lookup.encounter_id = stg_pat_enc.pat_enc_csn_id
        and visit_key_lookup.source_name = 'clarity'
      left join {{ref('stg_hsp_acct_pat_csn')}} as hospital_account_visit
        on stg_pat_enc.pat_enc_csn_id = hospital_account_visit.pat_enc_csn_id
        and stg_pat_enc.hsp_account_id = hospital_account_visit.hsp_account_id
      left join {{ref('stg_pat_enc_hsp')}} as visit_addl_info
        on stg_pat_enc.pat_enc_csn_id = visit_addl_info.pat_enc_csn_id
)

select
    last_stay_class.visit_key,
    coalesce(last_stay_class.last_encounter_stay_indicator, -2) as visit_last_stay_class_ind,
    coalesce(cdw_dictionary.dict_key, -2) as dict_visit_last_stay_cls_key,
    current_timestamp as upd_dt
from
    last_stay_class
    left join {{source('cdw','cdw_dictionary')}} as cdw_dictionary
        on lower(cdw_dictionary.dict_nm) = lower(last_stay_class.last_encounter_stay_class)
        and dict_cat_key = 41
