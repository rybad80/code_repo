with rc as (
     select
           rcp.app_title,
           rcq.mstr_redcap_quest_key,
           rcq.field_order,
           rcq.field_nm,
           rcq.element_label,
           rcd.record,
           rcea.element_id,
           SUBSTR(COALESCE(rcea.element_desc, rcd.value), 1, 250) as value,
           rsr.return_cd,
           rsr.timestamps,
           ROW_NUMBER() over (partition by rcd.record, rcd.mstr_redcap_quest_key order by rcea.element_id) as row_num
     from
        {{source('cdw', 'redcap_detail')}} as rcd
        left join {{source('cdw', 'master_redcap_project')}} as rcp on rcp.mstr_project_key = rcd.mstr_project_key
        left join {{source('cdw', 'master_redcap_question')}} as rcq on rcq.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key
        left join {{source('cdw', 'master_redcap_element_answr')}} as rcea on rcea.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key and rcd.value = rcea.element_id
        left join ( --noqa: L042
            select
rsr.redcap_record,
                rsr.mstr_redcap_event_key,
                UPPER(MAX(rsr.survey_response_return_cd)) as return_cd,
                MAX(rsr.survey_response_first_submit_dt) as timestamps
            from {{source('cdw', 'master_redcap_survey_response')}} as rsr
            group by rsr.redcap_record, rsr.mstr_redcap_event_key
        ) as rsr
            on rsr.mstr_redcap_event_key = rcd.mstr_redcap_event_key and rsr.redcap_record = rcd.record
     where
        rcd.cur_rec_ind = 1
        and rcp.project_id = 168
     order by
        rcd.record,
        rcq.field_order
),
rc_flat as (
    select
           rc_flat_a.*,
           LPAD(TRIM(TRANSLATE(rc_flat_a.safety_event_id, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()-,', '')), 15, '0') as file_id
    from (
        select
            rc.record as record_id,
              MAX(rc.return_cd) as return_cd,
              MAX(rc.timestamps) as submit_dt,
              MAX(case when rc.field_nm = 'safetyevent_id' then UPPER(rc.value) end) as safety_event_id,
              MAX(case when rc.field_nm = 'admit_date' then rc.value end) as admit_date,
              TO_TIMESTAMP(SUBSTR(MAX(case when rc.field_nm = 'fall_datetime' and LENGTH(rc.value) >= 10 then rc.value end), 1, 16), 'yyyy-mm-dd HH:MI') as fall_dt,
              MAX(case when rc.field_nm = 'location' then rc.value end) as location_unit_nm,
              MAX(case when rc.field_nm = 'location' then rc.element_id end) as location_unit_id,
              MAX(case when rc.field_nm = 'pt_age' then rc.value end) as pat_age_at_fall,
              MAX(case when rc.field_nm = 'fall_type' then rc.value end) as fall_type,
              MAX(case when rc.field_nm = 'physiologic' then rc.value end) as physiologic,
              MAX(case when rc.field_nm = 'event_description' then rc.value end) as event_description,
              MAX(case when rc.field_nm = 'injury_level' then rc.value end) as injury_level,
              MAX(case when rc.field_nm = 'pt_ot' then rc.value end) as pt_ot,
              MAX(case when rc.field_nm = 'risk_assess' then rc.value end) as risk_assess_prior_to_fall,
              MAX(case when rc.field_nm = 'timeto_fall' then rc.value end) as time_between_risk_assess_and_fall,
              MAX(case when rc.field_nm = 'risk_score' then rc.value end) as risk_assess_score_prior_to_fall,
              MAX(case when rc.field_nm = 'fall_risk' then rc.value end) as patient_at_risk_to_fall,
              MAX(case when rc.field_nm = 'prev_protocol' then rc.value end) as fall_prevention_protocol_followed,
              MAX(case when rc.field_nm = 're_assess' then rc.value end) as re_assessment_done_after_event,
              MAX(case when rc.field_nm = 'fall_assist' then rc.value end) as fall_assist,
              MAX(case when rc.field_nm = 'assist_role' then rc.value end) as assisted_employee_role,
              MAX(case when rc.field_nm = 'prior_fall' then rc.value end) as prior_fall,
              MAX(case when rc.field_nm = 'falls_with_injury_complete' then rc.value end) as falls_survey_complete,
              MAX(case when rc.field_nm = 'verify' then rc.value end) as fall_verified,
              DATE(MAX(case when rc.field_nm = 'conf_dt' then rc.value end)) as conf_dt
           from rc
           group by rc.record
           order by rc.record
        ) as rc_flat_a
),
kaps_safetynet_mrn as (
    select
       LPAD(TRIM(TRANSLATE(kaps_incident_1.file_id, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()-,', '')), 15, '0') as file_id,
       COALESCE(LPAD(kaps_event_person_affected.person_mrn, 8, 0), 'UNKNOWN') as mrn
     from
        {{source('cdw', 'kaps_incident_1')}} as kaps_incident_1
        inner join {{source('cdw', 'kaps_event_person_affected')}} as kaps_event_person_affected on kaps_incident_1.incid_id = kaps_event_person_affected.incid_id
     group by --noqa: L054
        1, 2
     union distinct
     select
           LPAD(TRIM(TRANSLATE(UPPER(safetynet_claim_1.claim), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()-,', '')), 15, '0') as file_id,
           COALESCE(LPAD(safetynet_claim_2.medical_record_number, 8, 0), 'UNKNOWN') as mrn
     from
        {{source('cdw', 'safetynet_claim_1')}} as safetynet_claim_1
        inner join {{source('cdw', 'safetynet_claim_2')}} as safetynet_claim_2 on safetynet_claim_1.claim_id = safetynet_claim_2.claim_id
     where
        safetynet_claim_2.medical_record_number is not null
     group by --noqa: L054
        1, 2
),
service as (
    select
        rcf.record_id,
        v.*,
        ROW_NUMBER() over (partition by rcf.record_id
                             order by case when CAST(d.dept_id as varchar(25)) = COALESCE(rcf.location_unit_id, '0') then 1 else 0 end desc,
                                      COALESCE(v.bed_key, 0) desc,
                                      v.adt_svc_key desc,
                                      v.enter_dt desc
          ) as rownum
    from
        rc_flat as rcf
        inner join kaps_safetynet_mrn as k on rcf.file_id = k.file_id
        inner join {{ref('stg_visit_event_service')}} as v on v.pat_mrn_id = LTRIM(RTRIM(LPAD(k.mrn, 8, 0)))
        left join {{source('cdw', 'department')}} as d on d.dept_key = v.dept_key
    where
        rcf.fall_dt between v.enter_dt and v.exit_dt
),
was_ip as (
    select
        c.pat_key,
        c.census_dt
    from
        {{source('cdw', 'fact_census_occ')}} as c
        inner join {{ref('stg_harm_dept_ip_op')}} as ip on ip.dept_key = c.dept_key
    where
        ip.ip_unit_ind = 1
        and c.hr_0 is not null
    group by
       c.pat_key, c.census_dt
)
    select
          case when kaps_safetynet_mrn.mrn is null then 0 else COALESCE(p.pat_key, -1) end as pat_key,
          case when rcf.location_unit_id is null then CAST(0 as bigint) else CAST(COALESCE(m.historical_dept_key, d.dept_key, -1) as bigint) end as dept_key,
          fall_dt,
          COALESCE(s.visit_key, -1) as visit_key,
          COALESCE(s.room_key, -1) as room_key,
          COALESCE(s.bed_key, -1) as bed_key,
          COALESCE(s.adt_svc_key, -2) as dict_svc_key,
          CAST(COALESCE(m.historical_dept_id, d.dept_id, -1) as bigint) as dept_id,
          s.room_id,
          s.bed_id,
          rcf.file_id,
          CAST(rcf.record_id as bigint) as record_id,
          CAST(rcf.location_unit_id as varchar(50)) as location_unit_id,
          CAST(rcf.safety_event_id as varchar(50)) as safety_event_id,
          CAST(COALESCE(p.pat_mrn_id, kaps_safetynet_mrn.mrn) as varchar(25)) as pat_mrn_id,
          CAST(p.last_nm as varchar(100)) as pat_last_nm,
          CAST(p.first_nm as varchar(100)) as pat_first_nm,
          p.full_nm as pat_full_nm,
          p.dob as pat_dob,
          p.sex as pat_sex,
          CAST(rcf.pat_age_at_fall as varchar(50)) as pat_age_at_fall,
          CAST(rcf.patient_at_risk_to_fall as varchar(50)) as patient_at_risk_to_fall,
          CAST(s.adt_svc_nm as varchar(50)) as svc_nm,
          s.room_nm,
          s.room_num,
          s.bed_nm,
          CAST(rcf.location_unit_nm as varchar(50)) as location_unit_nm,
          CAST(rcf.injury_level as varchar(50)) as injury_level,
          CAST(rcf.risk_assess_score_prior_to_fall as varchar(50)) as risk_assess_score_prior_to_fall,
          CAST(rcf.time_between_risk_assess_and_fall as varchar(50)) as time_between_risk_assess_and_fall,
          CAST(rcf.assisted_employee_role as varchar(50)) as assisted_employee_role,
          rcf.event_description,
          CAST(rcf.fall_type as varchar(50)) as fall_type,
          CAST(rcf.falls_survey_complete as varchar(50)) as falls_survey_complete,
          TO_DATE(SUBSTR(rcf.admit_date, 1, 10), 'yyyy-mm-dd') as redcap_admit_dt,
          CAST(rcf.conf_dt as timestamp) as conf_dt,
          rcf.submit_dt,
          case
                when UPPER(rcf.fall_assist) = 'YES' then CAST(1 as byteint)
                when UPPER(rcf.fall_assist) = 'NO' then CAST(0 as byteint)
                else CAST(-2 as byteint)
            end as fall_assisted_by_employee_ind,
          case
                when UPPER(rcf.fall_prevention_protocol_followed) = 'YES' then CAST(1 as byteint)
                when UPPER(rcf.fall_prevention_protocol_followed) = 'NO' then CAST(0 as byteint)
                else CAST(-2 as byteint)
            end as fall_prevention_protocol_followed_ind,
          case
                when rcf.fall_verified = '1' then CAST(1 as byteint)
                when rcf.fall_verified = '0' then CAST(0 as byteint)
                else CAST(-2 as byteint)
            end as fall_verified_ind,
          case
                when UPPER(rcf.physiologic) = 'YES' then CAST(1 as byteint)
                when UPPER(rcf.physiologic) = 'NO' then CAST(0 as byteint)
                else CAST(-2 as byteint)
            end as physiologic_ind,
          case
                when rcf.prior_fall = '1' then CAST(1 as byteint)
                when rcf.prior_fall = '0' then CAST(0 as byteint)
                else CAST(-2 as byteint)
            end as prior_fall_1month_ind,
          case
                when rcf.pt_ot = '1' then CAST(1 as byteint)
                when rcf.pt_ot = '0' then CAST(0 as byteint)
                else CAST(-2 as byteint)
            end as pt_ot_ind,
          case
                when UPPER(rcf.re_assessment_done_after_event) = 'YES' then CAST(1 as byteint)
                when UPPER(rcf.re_assessment_done_after_event) = 'NO' then CAST(0 as byteint)
                else CAST(-2 as byteint)
            end as re_assessment_done_after_event_ind,
          case
                when UPPER(rcf.risk_assess_prior_to_fall) = 'YES' then CAST(1 as byteint)
                when UPPER(rcf.risk_assess_prior_to_fall) = 'NO' then CAST(0 as byteint)
                else CAST(-2 as byteint)
            end as risk_assess_prior_to_fall_ind,
          CAST(COALESCE(s.international_ind, -2) as byteint) as international_ind,
          case
                when
                    UPPER(rcf.injury_level) not in ('NONE', 'MINOR')
                    and rcf.location_unit_id not in ('10292012', '10201512', '900100100', '101001045', '89356016', '10421099')
                    and (ip.ip_unit_ind = 1 or (ip.ip_unit_ind = 0 and was_ip.pat_key is not null))
                    then CAST(1 as byteint)
                else CAST(0 as byteint)
            end as reportable_ind,
            CURRENT_TIMESTAMP as create_dt,
            'DBT' as create_by,
            CURRENT_TIMESTAMP as upd_dt
    from
        rc_flat as rcf
        left join kaps_safetynet_mrn on kaps_safetynet_mrn.file_id = rcf.file_id
        left join {{source('cdw', 'patient')}} as p on kaps_safetynet_mrn.mrn = LPAD(p.pat_mrn_id, 8, 0)
        left join service as s on s.record_id = rcf.record_id and s.rownum = 1
        left join {{source('cdw', 'department')}} as d on d.dept_id = rcf.location_unit_id
        left join {{ref('stg_harm_dept_ip_op')}} as ip on ip.dept_key = d.dept_key
        left join {{ref('master_harm_prevention_dept_mapping')}} as m
            on m.harm_type = 'Falls with Injury'
            and m.current_dept_key = d.dept_key
            and fall_dt between m.start_dt and m.end_dt
            and m.denominator_only_ind = 0
        left join was_ip
           on was_ip.pat_key = p.pat_key
           and was_ip.census_dt = DATE(fall_dt)
    where
         UPPER(fall_verified) in ('1', 'YES')
