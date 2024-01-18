{%- set lookup_key = [
    ('pat_class'         , 'stage.adt_pat_class_c'      , '10052' ),
    ('acuity'            , 'stage.acuity_level_c'       , '10053' ),
    ('dspn'              , 'stage.ed_disposition_c'     , '10054' ),
    ('arrvl_mode'        , 'stage.means_of_arrv_c'      , '10028' ),
    ('dpart_mode'        , 'stage.means_of_depart_c'    , '10055' ),
    ('hsp_admsn_type'    , 'stage.hosp_admsn_type_c'    , '10058' ),
    ('dischrg_dspn'      , 'stage.disch_disp_c'         , '100137'),
    ('accom'             , 'stage.accommodation_c'      , '10057' ),
    ('admit_src'         , 'stage.admit_source_c'       , '100178'),
    ('pat_stat'          , 'stage.adt_patient_stat_c'   , 'stage.adt_patient_stat_dict_cat_key'),
    ('xfer_src'          , 'stage.transfer_from_c'      , 'stage.transfer_from_c_dict_cat_key'),
    ('disch_dest'        , 'stage.disch_dest_c'         ,'100179'),
    ('hosp_serv'         , 'stage.hosp_serv_c'          ,'10051')
] %}

with x_aud as (
  select 
    pat_enc_csn_id, 
    line, 
    item, 
    max(change_time) max_change_time 
  from 
    {{source('cdw_stage','s_cl_audit_itm_pat_enc')}}
  where 
    line = 1 
    and item = 410 
    and new_internal_value in ('13', '14', '15', '16', '17') 
  group by 
    pat_enc_csn_id, 
    line, 
    item
), 
-- get the single row by pat_enc_csn_id and updated_conf_stat_c
x_enc as (
  select 
    distinct pat_enc_csn_id, 
    updated_conf_stat_c 
  from 
    {{source('cdw_stage','s_cl_pat_enc_stat_hx')}} 
  where 
    updated_conf_stat_c = 3
),

beds as (
    select
        bed_id,
        bed_key,
        row_number() over (partition by bed_id order by bed_key desc) as row_num
    from
        {{source('cdw','master_bed')}}
),

rooms as (
    select
        room_id,
        room_key,
        room_nm,
        row_number() over (partition by room_nm order by room_key desc) as row_num
    from
        {{source('cdw','master_room')}}
),

stage as (
    select 
        hsp.pat_id, 
        hsp.pat_enc_csn_id, 
        hsp.adt_pat_class_c, 
        hsp.adt_patient_stat_c,
        '100332' as adt_patient_stat_dict_cat_key,
        hsp.admit_source_c, 
        hsp.adt_arrival_time, 
        hsp.hosp_admsn_time, 
        hsp.hosp_disch_time, 
        hsp.discharge_prov_id, 
        hsp.admission_prov_id, 
        hsp.hosp_admsn_type_c, 
        coalesce(hsp.department_id, 0) as department_id,
        coalesce(hsp.room_id, '0') as room_id,
        coalesce(hsp.bed_id, '0') as bed_id,
        hsp.hosp_serv_c, 
        hsp.means_of_depart_c, 
        hsp.disch_disp_c, 
        hsp.disch_dest_c, 
        hsp.means_of_arrv_c, 
        hsp.acuity_level_c, 
        hsp.accommodation_c, 
        coalesce(hsp.dis_event_id, 0) as dis_event_id,
        coalesce(hsp.inpatient_data_id, '0') as inpatient_data_id,
        hsp.pvt_hsp_enc_c,
        case
            when hsp.pvt_hsp_enc_c = 1 then 1
            when hsp.pvt_hsp_enc_c = 2 then 0
            else -2
        end as priv_ind,
        hsp.ed_episode_id,
        case
            when ed_episode_id is not null 
            then 1 
            else 0 
        end as ed_ind,
        hsp.ed_disposition_c, 
        coalesce(hsp.ed_area_of_care_id, 0) as ed_area_of_care_id,
        hsp.inp_adm_date, 
        hsp.ed_departure_time, 
        hsp.instant_of_entry_tm, 
        aud.new_external_value, 
        hsp.cuml_room_nm, 
        hsp.edecu_arrival, 
        hsp.edecu_reason_to_admit, 
        coalesce(hsp.edecu_room, 'DEFAULT') as edecu_room, 
        hsp.derived_hosp_srvc, 
        hsp.transfer_from_c,
        '100326' as transfer_from_c_dict_cat_key,
        case
            when x_enc.updated_conf_stat_c is not null
            then 1
            else 0
        end as ed_cancelled_visit_ind
    from 
        {{ref('stg_pat_enc_hsp')}} as hsp 
        left join x_aud 
            on x_aud.pat_enc_csn_id = hsp.pat_enc_csn_id 
        left join {{source('cdw_stage','s_cl_audit_itm_pat_enc')}} as aud
            on aud.pat_enc_csn_id = x_aud.pat_enc_csn_id 
            and aud.change_time = x_aud.max_change_time 
            and aud.line = x_aud.line 
            and aud.item = x_aud.item 
        left join x_enc 
            on x_enc.pat_enc_csn_id = hsp.pat_enc_csn_id
),

keys as (
    select
        stage.pat_enc_csn_id,
        visit_event.visit_event_key as disch_visit_event_key,
        stg_visit_key_lookup.visit_key,
        patient.pat_key,
        department.dept_key as last_dept_key,
        master_room.room_key as last_room_key,
        edecu_room.room_key as edecu_room_key,
        beds.bed_key last_bed_key,
        visit_stay_info.vsi_key,
        ed_area.ed_area_key,
        rn_acuity.dict_key as dict_rn_acuity_key,
        {%- for target_name, source_id, dict_key in lookup_key %}
           {{target_name}}.dict_key as dict_{{target_name}}_key,       
        {% endfor %}
        admit_provider.prov_key as admit_prov_key,
        discharge_provider.prov_key as dischrg_prov_key
    from
        stage
        left join {{ref('stg_visit_key_lookup')}} as stg_visit_key_lookup
            on stg_visit_key_lookup.encounter_id = stage.pat_enc_csn_id
            and stg_visit_key_lookup.source_name = 'clarity'
        left join {{source('cdw','patient')}} as patient
            on patient.pat_id = stage.pat_id
        left join {{source('cdw','visit_event')}} as visit_event
            on visit_event.adt_event_id = stage.dis_event_id
        left join {{source('cdw','department')}} as department
            on department.dept_id = stage.department_id
        left join {{source('cdw','master_room')}} as master_room
            on master_room.room_id = stage.room_id
        left join beds
            on beds.bed_id = stage.bed_id
            and beds.row_num = 1
        left join rooms as edecu_room
            on edecu_room.room_nm = stage.edecu_room
            and edecu_room.row_num = 1
        left join {{source('cdw','visit_stay_info')}} as visit_stay_info
            on visit_stay_info.vsi_id = stage.inpatient_data_id
        left join {{source('cdw','ed_area')}} as ed_area 
            on ed_area.ed_area_id = stage.ed_area_of_care_id
        left join {{source('cdw','provider')}} as admit_provider 
            on admit_provider.prov_id = stage.admission_prov_id
        left join {{source('cdw','provider')}} as discharge_provider 
            on discharge_provider.prov_id = stage.discharge_prov_id
        left join {{source('cdw','cdw_dictionary')}} as rn_acuity
            on rn_acuity.dict_nm = stage.new_external_value
            and rn_acuity.dict_cat_key = 10053
        {%- for target_name, source_id, dict_key in lookup_key %}
            {{join_cdw_dictionary( target_name, source_id, dict_key ) }}            
        {% endfor %}
)

select
    coalesce(keys.visit_key, 0) as visit_key,
    coalesce(keys.pat_key, 0) as pat_key,
    coalesce(keys.admit_prov_key, 0) as admit_prov_key,
    coalesce(keys.dischrg_prov_key, 0) as dischrg_prov_key,
    coalesce(keys.last_dept_key, 0) as last_dept_key,
    coalesce(keys.last_bed_key, 0) as last_bed_key,
    coalesce(keys.last_room_key, 0) as last_room_key,
    coalesce(keys.edecu_room_key, 0) as edecu_room_key,
    coalesce(keys.ed_area_key, 0) as ed_area_key,
    coalesce(keys.vsi_key, 0) as vsi_key,
    coalesce(keys.disch_visit_event_key, 0) as disch_visit_event_key,
    coalesce(keys.dict_pat_class_key, -2) as dict_pat_class_key,
    coalesce(keys.dict_acuity_key, -2) as dict_acuity_key,
    coalesce(keys.dict_rn_acuity_key, -2) as dict_rn_acuity_key,
    coalesce(keys.dict_dspn_key, -2) as dict_dspn_key,
    coalesce(keys.dict_arrvl_mode_key, -2) as dict_arrvl_mode_key,
    coalesce(keys.dict_dpart_mode_key, -2) as dict_dpart_mode_key,
    coalesce(keys.dict_hsp_admsn_type_key, -2) as dict_hsp_admsn_type_key,
    coalesce(keys.dict_dischrg_dspn_key, -2) as dict_dischrg_dspn_key,
    coalesce(keys.dict_accom_key, -2) as dict_accom_key,
    coalesce(keys.dict_admit_src_key, -2) as dict_admit_src_key,
    coalesce(keys.dict_disch_dest_key, -2) as dict_disch_dest_key,
    coalesce(keys.dict_hosp_serv_key, -2) as dict_hosp_serv_key,
    coalesce(keys.dict_xfer_src_key, -2) as dict_xfer_src_key,
    coalesce(keys.dict_pat_stat_key, -2) as dict_pat_stat_key,
    stage.pat_enc_csn_id as enc_id,
    stage.cuml_room_nm,
    stage.edecu_reason_to_admit as edecu_rsn,
    stage.derived_hosp_srvc as der_hsp_svc,
    stage.edecu_arrival as edecu_arrvl_dt,
    stage.instant_of_entry_tm as src_create_dt,
    stage.adt_arrival_time as adt_arrvl_dt,
    stage.ed_departure_time as ed_dpart_dt,
    stage.hosp_admsn_time as hosp_admit_dt,
    stage.hosp_disch_time as hosp_disch_dt,
    stage.inp_adm_date as ip_admit_dt,
    stage.ed_ind,
    stage.priv_ind,
    stage.ed_cancelled_visit_ind,
    current_timestamp as create_dt,
    'Enterprise Marts' as create_by,
    current_timestamp as upd_dt,
    'Enterprise Marts' as upd_by
    
from
    stage
    left join keys
        on keys.pat_enc_csn_id = stage.pat_enc_csn_id
