with rc_flat as (
    select
         record_id,
         COALESCE(submission_timestamp, date '9999-12-31') as submit_dt,
         MAX(case when field_nm = 'user_name' then value end) as user_name,
         MAX(case when field_nm = 'other_unit' then value end) as other_unit_discovered,
         MAX(case when field_nm = 'unit' and value_int > 1 then value_int end) as unit_id,
         MAX(case when field_nm = 'unit_develop' and value_int > 1 then value_int end) as unit_developed_id,
         MAX(case when field_nm = 'other_unit_develop' then value end) as other_unit_developed,
         MAX(case when field_nm = 'medical_record' then value end) as mrn,
         MAX(case when field_nm = 'patient_date_of_birth' then value end) as patient_date_of_birth,
         MAX(case when field_nm = 'date_of_infiltration' then value end) as date_of_infiltration,
         MAX(case when field_nm = 'time_of_infiltration' then value end) as time_of_infiltration,
         MAX(case when field_nm = 'location_infil_discovered' then value end) as location_infil_discovered,
         MAX(case when field_nm = 'vas_rn_initial' then value end) as vas_rn_initial,
         MAX(case when field_nm = 'pivie_x' then value end) as pivie_x,
         MAX(case when field_nm = 'pivie_y' then value end) as pivie_y,
         MAX(case when field_nm = 'extravasation_volume_x_y_x' then value end) as extravasation_volume_x_y_x,
         MAX(case when field_nm = 'other_pivie_injury_please' then value end) as other_pivie_injury_please,
         MAX(case when field_nm = 'automated_pivie_scoring' then value end) as automated_pivie_scoring,
         MAX(case when field_nm = 'date_of_piv_insertion' then value end) as date_of_piv_insertion,
         MAX(case when field_nm = 'number_of_iv_attempts' then value end) as number_of_iv_attempts,
         MAX(case when field_nm = 'inserted_by' then value end) as inserted_by,
         MAX(case when field_nm = 'inserted_by_other' then value end) as inserted_by_other,
         MAX(case when field_nm = 'us_utilied_during_placemnt' then value end) as ultrasound_utilized_during_placement,
         MAX(case when field_nm = 'catheter_guage' then value end) as catheter_guage,
         MAX(case when field_nm = 'peripheral_iv_site' then value end) as peripheral_iv_site,
         MAX(case when field_nm = 'cvl_present' then value end) as cvl_present,
         MAX(case when field_nm = 'patient_s_access_problem' then value end) as patient_access_problem,
         MAX(case when field_nm = 'other_infusate_please_list' then value end) as other_infusate_please_list,
         MAX(case when field_nm = 'other_bolus_please_specify' then value end) as other_bolus_please_specify,
         MAX(case when field_nm = 'chemo_irritants_other' then value end) as chemo_irritants_other,
         MAX(case when field_nm = 'chemo_vesicants_other' then value end) as chemo_vesicants_other,
         MAX(case when field_nm = 'irritants_other' then value end) as irritants_other,
         MAX(case when field_nm = 'maintenance_fluids_other' then value end) as maintenance_fluids_other,
         MAX(case when field_nm = 'parenteral_fluids' then value end) as parenteral_fluids,
         MAX(case when field_nm = 'vesicants_other_please_lis' then value end) as vesicants_other_please_lis,
         MAX(case when field_nm = 'post_pivie' then value end) as post_pivie,
         MAX(case when field_nm = 'treatment_initiated' then value end) as treatment_initiated,
         MAX(case when field_nm = 'treatmnt_other' then value end) as treatmnt_other,
         MAX(case when field_nm = 'vas_infiltration_grading_form_complete' then value end) as vas_infiltration_grading_form_complete,
         MAX(case when field_nm = 'updated_pivie_score' then value end) as updated_pivie_score,
         MAX(case when field_nm = 'piv_midline' then value end) as midline_or_piv,
         MAX(case when field_nm = 'date_of_midline_insertion' then value end) as midline_insertion_dt,
         MAX(case when field_nm = 'conf_dt' then value end) as conf_dt
    from {{ ref('stg_harm_pivie_redcap') }}
    group by 1, 2
),
-- region Redcap Data combined with legacy data
rc_legacy_combined as (
    select
        rc_flat.record_id,
        MAX(CAST(rc_flat.date_of_infiltration as date)) as date_of_infiltration,
        MAX(CAST(rc_flat.date_of_infiltration as timestamp) + CAST(COALESCE(rc_flat.time_of_infiltration, '00:00:00') as time)) as datetime_of_infiltration,
        MAX(COALESCE(m_disc.historical_dept_key, d_disc.dept_key)) as unit_discovered_dept_key,
        MAX(COALESCE(m_disc.historical_dept_id, d_disc.dept_id)) as unit_discovered_dept_id,
        MAX(COALESCE(m_dev.historical_dept_key, d_dev.dept_key, m_disc.historical_dept_key, d_disc.dept_key)) as unit_developed_dept_key,
        MAX(COALESCE(m_dev.historical_dept_id, d_dev.dept_id, m_disc.historical_dept_id, d_disc.dept_id)) as unit_developed_dept_id,
        0 as legacy_data
    from
        rc_flat
        left join {{ source('cdw', 'department') }} as d_disc on CAST(rc_flat.unit_id as int) = d_disc.dept_id
        left join {{ ref('master_harm_prevention_dept_mapping') }} as m_disc
            on m_disc.harm_type = 'PIVIE'
            and m_disc.current_dept_id = d_disc.dept_id
            and rc_flat.date_of_infiltration between m_disc.start_dt and m_disc.end_dt
            and m_disc.denominator_only_ind = 0
        left join {{ source('cdw', 'department') }} as d_dev on CAST(rc_flat.unit_developed_id as int) = d_dev.dept_id
        left join {{ ref('master_harm_prevention_dept_mapping') }} as m_dev
            on m_dev.harm_type = 'PIVIE'
            and m_dev.current_dept_id = d_dev.dept_id
            and rc_flat.date_of_infiltration between m_dev.start_dt and m_dev.end_dt
            and m_dev.denominator_only_ind = 0
    where
        COALESCE(rc_flat.unit_id, '1') != '1104'
        and COALESCE(rc_flat.automated_pivie_scoring, rc_flat.updated_pivie_score) is not null
        and (rc_flat.unit_developed_id != '1104' or rc_flat.unit_developed_id is null)
        and (rc_flat.other_unit_developed is null or UPPER(rc_flat.other_unit_developed) not like '%TRANSPORT%')
    group by 1
),
-- end region 
--REGION checkbox: PIVIE_DESC
pivie_desc as (
    select
        record_id,
        MAX(DECODE(row_num, 1, value, ''))
             || MAX(DECODE(row_num, 2, ', ' || value, '')) || MAX(DECODE(row_num, 3, ', ' || value, ''))
             || MAX(DECODE(row_num, 4, ', ' || value, '')) || MAX(DECODE(row_num, 5, ', ' || value, ''))
             || MAX(DECODE(row_num, 6, ', ' || value, '')) || MAX(DECODE(row_num, 7, ', ' || value, ''))
             || MAX(DECODE(row_num, 8, ', ' || value, '')) || MAX(DECODE(row_num, 9, ', ' || value, ''))
             || MAX(DECODE(row_num, 10, ', ' || value, '')) || MAX(DECODE(row_num, 11, ', ' || value, ''))
             || MAX(DECODE(row_num, 12, ', ' || value, '')) || MAX(DECODE(row_num, 13, ', ' || value, ''))
             || MAX(DECODE(row_num, 14, ', ' || value, '')) || MAX(DECODE(row_num, 15, ', ' || value, ''))
             || MAX(DECODE(row_num, 16, ', ' || value, ''))
         as pivie_desc
    from {{ ref('stg_harm_pivie_redcap') }}
    where field_nm = 'pivie_desc'
    group by 1
),
--ENDREGION
--REGION checkbox: INFUSATE_RUNNING
infusate_running as (
    select
        record_id,
        MAX(DECODE(element_id, 1, element_desc)) as infusate_running1,
        MAX(DECODE(element_id, 2, element_desc)) as infusate_running2,
        MAX(DECODE(element_id, 3, element_desc)) as infusate_running3,
        MAX(DECODE(element_id, 4, element_desc)) as infusate_running4,
        MAX(DECODE(element_id, 5, element_desc)) as infusate_running5,
        MAX(DECODE(element_id, 6, element_desc)) as infusate_running6,
        MAX(DECODE(element_id, 7, element_desc)) as infusate_running7,
        MAX(DECODE(element_id, 8, element_desc)) as infusate_running8,
        MAX(DECODE(row_num, 1, value, ''))
        || MAX(DECODE(row_num, 2, ', ' || value, '')) || MAX(DECODE(row_num, 3, ', ' || value, ''))
        || MAX(DECODE(row_num, 6, ', ' || value, '')) || MAX(DECODE(row_num, 7, ', ' || value, ''))
        || MAX(DECODE(row_num, 8, ', ' || value, '')) as infusate_running
    from {{ ref('stg_harm_pivie_redcap') }}
    where field_nm = 'infusate_running'
    group by 1
),
--ENDREGION
--REGION checkbox: SPS
sps as (
    select
        record_id,
        MAX(case when value = 'Any number of clear blisters (open or closed are present' then 1 else 0 end) as clear_blister_ind,
        MAX(case when value = 'Deep partial thickness tissue injury' then 1 else 0 end) as deep_thickness_burns_ind,
        MAX(case when value = 'Diminished pulse in distal extremity' then 1 else 0 end) as diminished_pulse_extremity_ind,
        MAX(case when value = 'No distal pulse by palpation or doppler on initial assessment' then 1 else 0 end) as no_palpable_pulse_ind,
        MAX(case when value = 'Fasciotomy' then 1 else 0 end) as fasciotomy_ind,
        MAX(case when value = 'Full thickness skin loss' then 1 else 0 end) as full_thickness_skin_loss_ind,
        MAX(case when value = 'Red skin that blanches (turns white when pressure is applied (such as when pressing a finger on the skin' then 1 else 0 end) as red_skin_blanches_ind,
        MAX(case when value = 'Skin graft or tissue transfer' then 1 else 0 end) as skin_graft_ind,
        MAX(case when value = 'Superficial partial thickness burn/tissue damage' then 1 else 0 end) as superficial_thickness_burn_ind,
        MAX(case when value = 'Swelling = 30-60% as calculated' then 1 else 0 end) as swelling_between_30_60_ind,
        MAX(case when value = 'Swelling >60% as calculated' then 1 else 0 end) as swelling_greater_60_ind,
        MAX(case when value = 'Cap refill >8 seconds (excluding chronic low bloodflow conditions' then 1 else 0 end) as cap_refill_greater_8_seconds_ind,
        MAX(case when value = 'Swelling < 30% as calculated' then 1 else 0 end) as swelling_less_30_ind,
        MAX(case when value = 'Any red skin that does not blanch (turn white when pressure is applied (such as when pressing a finger on the skin' then 1 else 0 end) as red_skin_not_blanch_ind,
        MAX(case when value = 'Any white skin that does not refill when pressure is applied (such as when pressing a finger on the skin' then 1 else 0 end) as white_skin_not_refill_ind,
        MAX(case when value = 'Swelling < 60%' then 1 else 0 end) as swelling_less_60_ind,
        MAX(case when value = 'Erythema that blanches when pressure is applied' then 1 else 0 end) as erythema_blanch_ind,
        MAX(case when value = 'Significant skin discoloration that does not change (blanch/refill) when pressure is applied' then 1 else 0 end) as any_skin_not_refill_ind
    from {{ ref('stg_harm_pivie_redcap') }}
    where field_nm = 'pivie_desc'
    group by record_id --noqa: L054
),
-- end region 
--REGION checkbox: BOLUS_FLUID, MAINTENANCE_FLUIDS
fluids as (
    select
        record_id,
        MAX(case when field_nm = 'bolus_fluid' and row_num = 1 then value else '' end)
            || MAX(case when field_nm = 'bolus_fluid' and row_num = 2 then ', ' || value else '' end)
            || MAX(case when field_nm = 'bolus_fluid' and row_num = 3 then ', ' || value else '' end)
            || MAX(case when field_nm = 'bolus_fluid' and row_num = 4 then ', ' || value else '' end)
        as bolus_fluid,
        MAX(case when field_nm = 'maintenance_fluids' and row_num = 1 then value else '' end)
            || MAX(case when field_nm = 'maintenance_fluids' and row_num = 2 then ', ' || value else '' end)
            || MAX(case when field_nm = 'maintenance_fluids' and row_num = 3 then ', ' || value else '' end)
            || MAX(case when field_nm = 'maintenance_fluids' and row_num = 4 then ', ' || value else '' end)
            || MAX(case when field_nm = 'maintenance_fluids' and row_num = 5 then ', ' || value else '' end)
            || MAX(case when field_nm = 'maintenance_fluids' and row_num = 6 then ', ' || value else '' end)
            || MAX(case when field_nm = 'maintenance_fluids' and row_num = 7 then ', ' || value else '' end)
            || MAX(case when field_nm = 'maintenance_fluids' and row_num = 8 then ', ' || value else '' end)
            || MAX(case when field_nm = 'maintenance_fluids' and row_num = 9 then ', ' || value else '' end)
        as maintenance_fluids
    from {{ ref('stg_harm_pivie_redcap') }}
    where field_nm in ('bolus_fluid', 'maintenance_fluids')
    group by 1
),
-- end region 
--REGION checkbox: PLEASE_CHECK_IF_ANY_OF_THE, TREATMNT_CONSLUTED
other_checkbox as (
    select
        record_id,
        MAX(case when field_nm = 'please_check_if_any_of_the' and row_num = 1 then element_id else '' end)
            || MAX(case when field_nm = 'please_check_if_any_of_the' and row_num = 2 then ', ' || element_id else '' end)
            || MAX(case when field_nm = 'please_check_if_any_of_the' and row_num = 3 then ', ' || element_id else '' end)
         as completed_after_infiltration, -- used to be called: PLEASE_CHECK_IF_ANY_OF_THE,                  
         MAX(case when field_nm = 'treatmnt_consluted' and row_num = 1 then value else '' end)
            || MAX(case when field_nm = 'treatmnt_consluted' and row_num = 2 then ', ' || value else '' end)
            || MAX(case when field_nm = 'treatmnt_consluted' and row_num = 3 then ', ' || value else '' end)
         as treatmnt_consluted
    from {{ ref('stg_harm_pivie_redcap') }}
    where field_nm in ('please_check_if_any_of_the', 'treatmnt_consluted')
    group by 1
),
-- end region 
--REGION checkbox: CHEMOTHERAPUTIC_IRRITANTS
chemotheraputic_irritants as (
select
    record_id,
     MAX(DECODE(row_num, 1, value, ''))
         || MAX(DECODE(row_num, 2, ', ' || value, '')) || MAX(DECODE(row_num, 3, ', ' || value, ''))
         || MAX(DECODE(row_num, 4, ', ' || value, '')) || MAX(DECODE(row_num, 5, ', ' || value, ''))
         || MAX(DECODE(row_num, 6, ', ' || value, '')) || MAX(DECODE(row_num, 7, ', ' || value, ''))
         || MAX(DECODE(row_num, 8, ', ' || value, '')) || MAX(DECODE(row_num, 9, ', ' || value, ''))
         || MAX(DECODE(row_num, 10, ', ' || value, '')) || MAX(DECODE(row_num, 11, ', ' || value, ''))
         || MAX(DECODE(row_num, 12, ', ' || value, '')) || MAX(DECODE(row_num, 14, ', ' || value, ''))
         || MAX(DECODE(row_num, 14, ', ' || value, '')) || MAX(DECODE(row_num, 15, ', ' || value, ''))
         || MAX(DECODE(row_num, 16, ', ' || value, '')) || MAX(DECODE(row_num, 17, ', ' || value, ''))
     as chemotheraputic_irritants
    from {{ ref('stg_harm_pivie_redcap') }}
    where field_nm = 'chemotheraputic_irritants'
    group by 1
),
-- end region
--REGION checkbox: CHEMOTHERAPUTIC_VESICANTS
chemotheraputic_vesicants as (
    select
        record_id,
         MAX(DECODE(row_num, 1, value, ''))
             || MAX(DECODE(row_num, 2, ', ' || value, '')) || MAX(DECODE(row_num, 3, ', ' || value, ''))
             || MAX(DECODE(row_num, 4, ', ' || value, '')) || MAX(DECODE(row_num, 5, ', ' || value, ''))
             || MAX(DECODE(row_num, 6, ', ' || value, '')) || MAX(DECODE(row_num, 7, ', ' || value, ''))
             || MAX(DECODE(row_num, 8, ', ' || value, '')) || MAX(DECODE(row_num, 9, ', ' || value, ''))
             || MAX(DECODE(row_num, 10, ', ' || value, '')) || MAX(DECODE(row_num, 11, ', ' || value, ''))
         as chemotheraputic_vesicants
      from {{ ref('stg_harm_pivie_redcap') }}
      where field_nm = 'chemotheraputic_vesicants'
      group by 1
),
-- end region 
--REGION checkbox: IRRITANTS
irritants as (
    select
        record_id,
             MAX(DECODE(row_num, 1, value, ''))
                 || MAX(DECODE(row_num, 2, ', ' || value, '')) || MAX(DECODE(row_num, 3, ', ' || value, ''))
                 || MAX(DECODE(row_num, 4, ', ' || value, '')) || MAX(DECODE(row_num, 5, ', ' || value, ''))
                 || MAX(DECODE(row_num, 6, ', ' || value, '')) || MAX(DECODE(row_num, 7, ', ' || value, ''))
                 || MAX(DECODE(row_num, 8, ', ' || value, '')) || MAX(DECODE(row_num, 9, ', ' || value, ''))
                 || MAX(DECODE(row_num, 10, ', ' || value, '')) || MAX(DECODE(row_num, 11, ', ' || value, ''))
                 || MAX(DECODE(row_num, 12, ', ' || value, '')) || MAX(DECODE(row_num, 13, ', ' || value, ''))
                 || MAX(DECODE(row_num, 14, ', ' || value, '')) || MAX(DECODE(row_num, 15, ', ' || value, ''))
                 || MAX(DECODE(row_num, 16, ', ' || value, '')) || MAX(DECODE(row_num, 17, ', ' || value, ''))
                 || MAX(DECODE(row_num, 18, ', ' || value, '')) || MAX(DECODE(row_num, 19, ', ' || value, ''))
                 || MAX(DECODE(row_num, 20, ', ' || value, '')) || MAX(DECODE(row_num, 21, ', ' || value, ''))
                 || MAX(DECODE(row_num, 22, ', ' || value, '')) || MAX(DECODE(row_num, 23, ', ' || value, ''))
                 || MAX(DECODE(row_num, 24, ', ' || value, '')) || MAX(DECODE(row_num, 25, ', ' || value, ''))
                 || MAX(DECODE(row_num, 26, ', ' || value, '')) || MAX(DECODE(row_num, 27, ', ' || value, ''))
                 || MAX(DECODE(row_num, 28, ', ' || value, '')) || MAX(DECODE(row_num, 29, ', ' || value, ''))
                 || MAX(DECODE(row_num, 30, ', ' || value, '')) || MAX(DECODE(row_num, 31, ', ' || value, ''))
                 || MAX(DECODE(row_num, 32, ', ' || value, '')) || MAX(DECODE(row_num, 33, ', ' || value, ''))
                 || MAX(DECODE(row_num, 34, ', ' || value, '')) || MAX(DECODE(row_num, 35, ', ' || value, ''))
                 || MAX(DECODE(row_num, 36, ', ' || value, '')) || MAX(DECODE(row_num, 37, ', ' || value, ''))
                 || MAX(DECODE(row_num, 38, ', ' || value, '')) || MAX(DECODE(row_num, 39, ', ' || value, ''))
                 || MAX(DECODE(row_num, 40, ', ' || value, '')) || MAX(DECODE(row_num, 41, ', ' || value, ''))
                 || MAX(DECODE(row_num, 42, ', ' || value, '')) || MAX(DECODE(row_num, 43, ', ' || value, ''))
                 || MAX(DECODE(row_num, 44, ', ' || value, '')) || MAX(DECODE(row_num, 45, ', ' || value, ''))
                 || MAX(DECODE(row_num, 46, ', ' || value, '')) || MAX(DECODE(row_num, 47, ', ' || value, ''))
                 || MAX(DECODE(row_num, 48, ', ' || value, '')) || MAX(DECODE(row_num, 49, ', ' || value, ''))
             as irritants
    from {{ ref('stg_harm_pivie_redcap') }}
    where field_nm = 'irritants'
    group by 1 --noqa: L054
),
-- end region 
--REGION checkbox: VESICANTS_AND_VASOACTIVE
vesicants_and_vasoactive as (
    select
        record_id,
         MAX(DECODE(row_num, 1, value, ''))
             || MAX(DECODE(row_num, 2, ', ' || value, '')) || MAX(DECODE(row_num, 3, ', ' || value, ''))
             || MAX(DECODE(row_num, 4, ', ' || value, '')) || MAX(DECODE(row_num, 5, ', ' || value, ''))
             || MAX(DECODE(row_num, 6, ', ' || value, '')) || MAX(DECODE(row_num, 7, ', ' || value, ''))
             || MAX(DECODE(row_num, 8, ', ' || value, '')) || MAX(DECODE(row_num, 9, ', ' || value, ''))
             || MAX(DECODE(row_num, 10, ', ' || value, '')) || MAX(DECODE(row_num, 11, ', ' || value, ''))
             || MAX(DECODE(row_num, 12, ', ' || value, '')) || MAX(DECODE(row_num, 13, ', ' || value, ''))
             || MAX(DECODE(row_num, 14, ', ' || value, '')) || MAX(DECODE(row_num, 15, ', ' || value, ''))
             || MAX(DECODE(row_num, 16, ', ' || value, '')) || MAX(DECODE(row_num, 17, ', ' || value, ''))
             || MAX(DECODE(row_num, 18, ', ' || value, '')) || MAX(DECODE(row_num, 19, ', ' || value, ''))
         as vesicants_and_vasoactive
    from {{ ref('stg_harm_pivie_redcap') }}
    where field_nm = 'vesicants_and_vasoactive'
    group by 1
),
-- end region 
surg as (
    select
        TO_DATE(orlog.surg_dt_key, 'YYYYMMDD') as surgical_dt,
        TO_NUMBER(pat.pat_mrn_id, '99999999') as mrn
    from
        {{ source('cdw', 'or_log') }} as orlog
        left join {{ source('cdw', 'patient') }} as pat on orlog.pat_key = pat.pat_key
    where
        orlog.surg_dt_key > '20150201'
        and orlog.pat_key >= 0
),
surg_flag as (
    select
        rc_flat.mrn,
        MAX(case when rc_flat.date_of_infiltration - surg.surgical_dt <= 1 and rc_flat.date_of_infiltration - surg.surgical_dt >= 0 then 1 else 0 end) as recent_surg_flag
    from
        rc_flat
        left join surg on surg.mrn = rc_flat.mrn
    group by 1
),
service as (
    select
        rcf.record_id,
        v.*,
        ROW_NUMBER() over (partition by rcf.record_id
                            order by case when v.dept_key in (COALESCE(rcl.unit_developed_dept_key, 0), COALESCE(rcl.unit_discovered_dept_key, 0)) then 1 else 0 end desc,
                                      COALESCE(v.bed_key, 0) desc,
                                      v.adt_svc_key desc,
v.visit_key asc
          ) as rownum
    from
        rc_flat as rcf
        inner join rc_legacy_combined as rcl on rcl.record_id = rcf.record_id
        inner join {{ ref('stg_visit_event_service') }} as v on v.pat_mrn_id = LPAD(rcf.mrn, 8, 0)
    where
        rcl.datetime_of_infiltration between v.enter_dt and v.exit_dt
),
was_ip as (
    select
        c.pat_key,
        c.census_dt
    from
        {{ source('cdw', 'fact_census_occ') }} as c
        inner join {{ ref('stg_harm_dept_ip_op') }} as ip on ip.dept_key = c.dept_key
    where
        ip.ip_unit_ind = 1
        and c.hr_0 is not null
    group by --noqa: L054
       c.pat_key, c.census_dt
)
select
     CAST(COALESCE(CAST(rcf.record_id as bigint), -1) as bigint) as record_id,
    case when rcf.mrn is null then CAST(0 as bigint) else CAST(COALESCE(p.pat_key, -1) as bigint) end as pat_key,
    case when rcf.unit_developed_id is null and rcf.unit_id is null then CAST(0 as bigint) else CAST(COALESCE(rlc.unit_developed_dept_key, -1) as bigint) end as developed_dept_key,
    case when rcf.unit_id is null then CAST(0 as bigint) else CAST(COALESCE(rlc.unit_discovered_dept_key, -1) as bigint) end as discovered_dept_key,
    COALESCE(s.visit_key, -1) as visit_key,
    COALESCE(s.room_key, -1) as room_key,
    COALESCE(s.bed_key, -1) as bed_key,
    COALESCE(s.adt_svc_key, -2) as dict_svc_key,
    CAST(rlc.unit_developed_dept_id as bigint) as developed_dept_id,
    CAST(rlc.unit_discovered_dept_id as bigint) as discovered_dept_id,
    CAST(COALESCE(p.pat_mrn_id, rcf.mrn) as varchar(50)) as pat_mrn_id,
    CAST(p.last_nm as varchar(100)) as pat_last_nm,
    CAST(p.first_nm as varchar(100)) as pat_first_nm,
    CAST(p.full_nm as varchar(200)) as pat_full_nm,
    p.dob as pat_dob,
    p.sex as pat_sex,
    CAST(s.adt_svc_nm as varchar(50)) as svc_nm,
    s.room_nm,
    s.room_num,
    s.bed_nm,
    CAST(rcf.user_name as varchar(150)) as user_nm,
    CAST(rcf.location_infil_discovered as varchar(250)) as location_infil_discovered,
    CAST(rcf.other_unit_developed as varchar(250)) as unit_developed_other,
    CAST(rcf.other_unit_discovered as varchar(250)) as unit_discovered_other,
    CAST(rcf.vas_rn_initial as varchar(50)) as vas_rn_initial,
    CAST((COALESCE(rcf.pivie_x, '0')) as integer) as measurement_x_cm_swelling_or_edema,
    CAST((COALESCE(rcf.pivie_y, '0')) as integer) as measurement_y_cm_length_of_arm_or_leg,
    CAST(rcf.extravasation_volume_x_y_x as numeric(15, 6)) as calculated_extravasation_volume,
    CAST(rcf.other_pivie_injury_please as varchar(250)) as pivie_other,
    CAST(rcf.automated_pivie_scoring as integer) as automated_pivie_scoring,
    CAST(COALESCE(rcf.updated_pivie_score, rcf.automated_pivie_scoring) as integer) as updated_pivie_score,
    case when UPPER(rcf.number_of_iv_attempts) != 'NOT DOCUMENTED' then CAST(rcf.number_of_iv_attempts as varchar(25)) end as number_of_iv_attempts,
    CAST(rcf.inserted_by as varchar(150)) as inserted_by,
    CAST(rcf.inserted_by_other as varchar(150)) as inserted_by_other,
    CAST(rcf.catheter_guage as integer) as catheter_guage,
    CAST(rcf.peripheral_iv_site as varchar(250)) as peripheral_iv_site,
    CAST(rcf.other_bolus_please_specify as varchar(250)) as bolus_other,
    CAST(rcf.chemo_irritants_other as varchar(250)) as chemo_irritants_other,
    CAST(rcf.chemo_vesicants_other as varchar(250)) as chemo_vesicants_other,
    CAST(rcf.irritants_other as varchar(250)) as irritants_other,
    CAST(rcf.maintenance_fluids_other as varchar(250)) as maintenance_fluids_other,
    CAST(rcf.parenteral_fluids as varchar(250)) as parenteral_fluids,
    CAST(rcf.vesicants_other_please_lis as varchar(200)) as vesicant_other,
    CAST(rcf.treatmnt_other as varchar(200)) as treatmnt_other,
    CAST(rcf.vas_infiltration_grading_form_complete as varchar(25)) as vas_infiltration_grading_form_complete,
    CAST(infusate_running as varchar(500)) as infusate_running,
    CAST(infusate_running1 as varchar(500)) as infusate_running1,
    CAST(infusate_running2 as varchar(500)) as infusate_running2,
    CAST(infusate_running3 as varchar(500)) as infusate_running3,
    CAST(infusate_running4 as varchar(500)) as infusate_running4,
    CAST(infusate_running5 as varchar(500)) as infusate_running5,
    CAST(infusate_running6 as varchar(500)) as infusate_running6,
    CAST(infusate_running7 as varchar(500)) as infusate_running7,
    CAST(infusate_running8 as varchar(500)) as infusate_running8,
    CAST(rcf.other_infusate_please_list as varchar(250)) as infusate_other,
    CAST(bolus_fluid as varchar(200)) as bolus_fluid,
    CAST(maintenance_fluids as varchar(200)) as maintenance_fluids,
    CAST(other_checkbox.completed_after_infiltration as varchar(25)) as completed_after_infiltration, -- previously named "PLEASE_CHECK_IF_ANY_OF_THE"
    CAST(treatmnt_consluted as varchar(150)) as treatmnt_consulted,
    CAST(chemotheraputic_irritants.chemotheraputic_irritants as varchar(250)) as chemotheraputic_irritants,
    CAST(chemotheraputic_vesicants.chemotheraputic_vesicants as varchar(250)) as chemotheraputic_vesicants,
    CAST(irritants.irritants as varchar(250)) as irritants,
    CAST(vesicants_and_vasoactive.vesicants_and_vasoactive as varchar(250)) as vesicants_and_vasoactive,
    CAST(pivie_desc as varchar(1500)) as pivie_desc,
    CAST(rcf.conf_dt as timestamp) as conf_dt,
    CAST(rlc.date_of_infiltration as timestamp) + CAST(rcf.time_of_infiltration as time) as infiltration_dt,
    CAST(rcf.submit_dt as timestamp) as submit_dt,
    CAST(rcf.date_of_piv_insertion as timestamp) as piv_insertion_dt,
    CAST(rcf.midline_insertion_dt as timestamp) as midline_insertion_dt,
    case
        when UPPER(rcf.midline_or_piv) = 'MIDLINE' then CAST(1 as byteint)
        when UPPER(rcf.midline_or_piv) like 'PIV%' then CAST(0 as byteint)
        else CAST(-2 as byteint)
    end as midline_ind,
    CAST(COALESCE(sps.clear_blister_ind, -2) as byteint) as clear_blister_ind,
    CAST(COALESCE(sps.deep_thickness_burns_ind, -2) as byteint) as deep_thickness_burns_ind,
    CAST(COALESCE(sps.diminished_pulse_extremity_ind, -2) as byteint) as diminished_pulse_extremity_ind,
    CAST(COALESCE(sps.no_palpable_pulse_ind, -2) as byteint) as no_palpable_pulse_ind,
    CAST(COALESCE(sps.fasciotomy_ind, -2) as byteint) as fasciotomy_ind,
    CAST(COALESCE(sps.full_thickness_skin_loss_ind, -2) as byteint) as full_thickness_skin_loss_ind,
    CAST(COALESCE(sps.red_skin_blanches_ind, -2) as byteint) as red_skin_blanches_ind,
    CAST(COALESCE(sps.skin_graft_ind, -2) as byteint) as skin_graft_ind,
    CAST(COALESCE(sps.superficial_thickness_burn_ind, -2) as byteint) as superficial_thickness_burn_ind,
    CAST(COALESCE(sps.swelling_between_30_60_ind, -2) as byteint) as swelling_between_30_60_ind,
    CAST(COALESCE(sps.swelling_greater_60_ind, -2) as byteint) as swelling_greater_60_ind,
    CAST(COALESCE(sps.cap_refill_greater_8_seconds_ind, -2) as byteint) as cap_refill_greater_8_seconds_ind,
    CAST(COALESCE(sps.swelling_less_30_ind, -2) as byteint) as swelling_less_30_ind,
    CAST(COALESCE(sps.red_skin_not_blanch_ind, -2) as byteint) as red_skin_not_blanch_ind,
    CAST(COALESCE(sps.white_skin_not_refill_ind, -2) as byteint) as white_skin_not_refill_ind,
    case
        when rcf.ultrasound_utilized_during_placement = '1' then CAST(1 as byteint) /* Was Ultrasound utilized during placement of PIV? */
        when rcf.ultrasound_utilized_during_placement = '0' then CAST(0 as byteint) else CAST(-2 as byteint)
    end as ultrasound_utilized_during_placement_ind,
    case
        when rcf.cvl_present = '1' then CAST(1 as byteint) /* Was a Central Line Present? */
        when rcf.cvl_present = '0' then CAST(0 as byteint) else CAST(-2 as byteint)
    end as cvl_present_ind,
    case
        when rcf.patient_access_problem = '1' then CAST(1 as byteint) /* Patient's access problem */
        when rcf.patient_access_problem = '0' then CAST(0 as byteint) else CAST(-2 as byteint)
    end as patient_access_problem_ind,
    case
        when rcf.post_pivie = '1' then CAST(1 as byteint) /* Was the real-time bedside event review done for this infiltration? */
        when rcf.post_pivie = '0' then CAST(0 as byteint) else CAST(-2 as byteint)
    end as post_pivie_ind,
    case
        when rcf.treatment_initiated = '1' then CAST(1 as byteint) /* Was medical treatment initiated? */
        when rcf.treatment_initiated = '0' then CAST(0 as byteint) else CAST(-2 as byteint)
    end as treatment_initiated_ind,
    CAST(COALESCE(surg_flag.recent_surg_flag, -2) as byteint) as recent_surg_flag_ind,
    CAST(COALESCE(s.international_ind, -2) as byteint) as international_ind,
    case when
        (ip.ip_unit_ind = 1 or (ip.ip_unit_ind = 0 and was_ip.pat_key is not null))
        and (
                (COALESCE(rcf.updated_pivie_score, rcf.automated_pivie_scoring) = 3 and infiltration_dt < '2018-07-01')
                or (COALESCE(rcf.updated_pivie_score, rcf.automated_pivie_scoring) = 3 and infiltration_dt >= '2018-07-01')
        )
        then CAST(1 as byteint)
        else CAST(0 as byteint)
    end as reportable_ind,
    CURRENT_TIMESTAMP as create_dt,
    'DBT' as create_by,
    CURRENT_TIMESTAMP as upd_dt
from
    rc_legacy_combined as rlc
    left join {{ source('cdw', 'department') }} as unit_developed on rlc.unit_developed_dept_key = unit_developed.dept_key
    left join {{ source('cdw', 'department') }} as unit_discovered on rlc.unit_discovered_dept_key = unit_discovered.dept_key
    left join {{ ref('stg_harm_dept_ip_op') }} as ip on ip.dept_key = rlc.unit_developed_dept_key
    left join rc_flat as rcf on rlc.record_id = rcf.record_id
    left join surg_flag on rcf.mrn = surg_flag.mrn
    left join pivie_desc on rcf.record_id = pivie_desc.record_id
    left join infusate_running on rcf.record_id = infusate_running.record_id
    left join fluids on rcf.record_id = fluids.record_id
    left join other_checkbox on rcf.record_id = other_checkbox.record_id
    left join chemotheraputic_irritants on rcf.record_id = chemotheraputic_irritants.record_id
    left join chemotheraputic_vesicants on rcf.record_id = chemotheraputic_vesicants.record_id
    left join irritants on rcf.record_id = irritants.record_id
    left join vesicants_and_vasoactive on rcf.record_id = vesicants_and_vasoactive.record_id
    left join sps on rcf.record_id = sps.record_id
    left join service as s on s.record_id = rlc.record_id and s.rownum = 1
    left join {{ source('cdw', 'patient') }} as p on p.pat_mrn_id = LPAD(rcf.mrn, 8, 0)
    left join was_ip
       on was_ip.pat_key = p.pat_key
       and was_ip.census_dt = DATE(rlc.date_of_infiltration)
