select
    pat_key,
    discovered_dept_key,
    pat_lda_key,
    cast(discovered_dt as timestamp) as discovered_dt,
    cast(anatomical_location as varchar(500)) as anatomical_location,
    developed_dept_key,
    discovered_other_dept_key,
    visit_key,
    room_key,
    bed_key,
    dict_svc_key,
    cast(discovered_dept_id as bigint) as discovered_dept_id,
    cast(developed_dept_id as bigint) as developed_dept_id,
    cast(discovered_other_dept_id as bigint) as discovered_other_dept_id,
    cast(lda_id as bigint) as lda_id,
    cast(enc_id as numeric(14, 3)) as enc_id,
    room_id,
    bed_id,
    cast(record_id as bigint) as record_id,
    cast(redcap_enc_id as numeric(14, 3)) as redcap_enc_id,
    cast(pat_mrn_id as varchar(25)) as pat_mrn_id,
    cast(pat_last_nm as varchar(100)) as pat_last_nm,
    cast(pat_first_nm as varchar(100)) as pat_first_nm,
    cast(pat_full_nm as varchar(200)) as pat_full_nm,
    pat_dob,
    pat_sex,
    cast(svc_nm as varchar(50)) as svc_nm,
    room_nm,
    room_num,
    bed_nm,
    cast(new_stage as varchar(25)) as new_stage,
    age_at_discovery,
    cast(age_group as varchar(50)) as age_group,
    cast(confirmed_by as varchar(100)) as confirmed_by,
    cast(device_other as varchar(500)) as device_other,
    cast(device_type_1 as varchar(100)) as device_type_1,
    cast(device_type_2 as varchar(100)) as device_type_2,
    cast(hapi_incident_comments as varchar(500)) as hapi_incident_comments,
    cast(injury_stage as varchar(100)) as injury_stage,
    cast(source_of_documentation as varchar(500)) as source_of_documentation,
    cast(survey_complete as varchar(50)) as survey_complete,
    cast(survey_entry_username as varchar(100)) as survey_entry_username,
    cast(padding_used as varchar(50)) as padding_used,
    cast(wound_description as varchar(500)) as wound_description,
    cast(conf_dt as timestamp) as conf_dt,
    cast(submit_dt as timestamp) as submit_dt,
    cast(recent_surg_dt as timestamp) as recent_surg_dt,
    progression_dt,
    cast(date_time_assessed_within_24hrs_ind as byteint) as date_time_assessed_within_24hrs_ind,
    cast(device_related_ind as byteint) as device_related_ind,
    cast(present_on_admission_ind as byteint) as present_on_admission_ind,
    cast(recent_surg_ind as byteint) as recent_surg_ind,
    cast(international_ind as byteint) as international_ind,
    cast(reportable_ind as byteint) as reportable_ind,
    cast(source_epic_documentation_ind as byteint) as source_epic_documentation_ind,
    cast(source_floc_ind as byteint) as source_floc_ind,
    cast(source_safetynet_ind as byteint) as source_safetynet_ind,
    cast(source_wound_consult_ind as byteint) as source_wound_consult_ind,
    cast(string_tightness_ind as byteint) as string_tightness_ind,
    cast(string_check_24hrs_ind as byteint) as string_check_24hrs_ind,
    current_timestamp as create_dt,
    'DBT' as create_by,
    current_timestamp as upd_dt
from (
    with rc as (
        select distinct
            rcp.app_title,
            rcq.mstr_redcap_quest_key,
            rcq.field_order,
            rcq.field_nm,
            rcq.element_label,
            rcd.record,
            rcd.value as rcd_value,
            substr(coalesce(rcea.element_desc, rcd.value), 1, 250) as value,
            rsr.submit_dt,
            row_number() over (partition by rcd.record, rcd.mstr_redcap_quest_key order by rcea.element_id) as row_num
        from
            {{source('cdw', 'redcap_detail')}} as rcd
             left join {{ source('cdw', 'master_redcap_project') }} as rcp on rcp.mstr_project_key = rcd.mstr_project_key
             left join {{ source('cdw', 'master_redcap_question') }} as rcq on rcq.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key
             left join {{ source('cdw', 'master_redcap_element_answr') }} as rcea on rcea.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key and rcd.value = rcea.element_id
             left join ( --noqa: L042
                select
                    rsr.redcap_record,
                    rsr.mstr_redcap_event_key,
                    min(rsr.survey_response_first_submit_dt) as submit_dt
                from {{ source('cdw', 'master_redcap_survey_response') }} as rsr
                group by rsr.redcap_record, rsr.mstr_redcap_event_key
             ) as rsr on rsr.mstr_redcap_event_key = rcd.mstr_redcap_event_key and rsr.redcap_record = rcd.record
        where
            rcp.project_id = 76
            and rcd.cur_rec_ind = 1
        order by
            rcd.record,
            rcq.field_order,
            row_num
    ),
    rc_flat as (
        select
             rc.record as record_id,
               --, MAX(RC.TIMESTAMPS) AS , cast(SUBMIT_DT as timestamp) as SUBMIT_DT
             max(case when rc.field_nm = 'lda_id' and length(rc.value) > 1 then rc.value end) as lda_id,
             max(case when rc.field_nm = 'name' then rc.value end) as survey_entry_username,
             max(case when rc.field_nm = 'patientname' then rc.value end) as redcap_patient_name,
             max(case when rc.field_nm = 'csn' then cast(regexp_replace(rc.value, '\D', '') as numeric(14, 3)) end) as redcap_csn,
             max(case when rc.field_nm = 'date_discovered' and length(rc.value) > 1 then cast(rc.value as date) end) as date_discovered,
             max(case when rc.field_nm = 'anatomical_location' then rc.value end) as anatomical_location,
             max(case when rc.field_nm = 'unit' and length(rc.rcd_value) > 1 then rc.rcd_value end) as unit,
             max(case when rc.field_nm = 'unit_other' and length(rc.value) > 1 then rc.rcd_value end) as unit_other,
             max(case when rc.field_nm = 'wound_description' then rc.value end) as wound_description,
             max(case when rc.field_nm = 'device_related' then rc.value end) as device_related,
             max(case when rc.field_nm = 'device_type1' then rc.value end) as device_type_1,
             max(case when rc.field_nm = 'device_type2' then rc.value end) as device_type_2,
             max(case when rc.field_nm = 'device_other' then rc.value end) as device_other,
             max(case when rc.field_nm = 'poa' then rc.value end) as poa,
             max(case when rc.field_nm = 'pu_placementdate' then rc.value end) as pu_placementdate,
             max(case when rc.field_nm = 'reported' then rc.value end) as reported,
             max(case when rc.field_nm = 'conf_status2' then rc.rcd_value end) as conf_status2,
             max(case when rc.field_nm = 'comments' then rc.value end) as hapi_incident_comments,
             max(case when rc.field_nm = 'hospital_acquired_pressure_injury_incident_report_complete' then rc.value end) as survey_complete,
             max(case when rc.field_nm = 'string_check' then rc.value end) as string_check,
             max(case when rc.field_nm = 'string_tightness' then rc.value end) as string_tightness,
             max(case when rc.field_nm = 'stage_progression' and rc.value = '1. Stage 3' then 'Stage 3' when rc.field_nm = 'stage_progression' and rc.value = '2. Stage 4' then 'Stage 4' when rc.field_nm = 'stage_progression' and rc.value = '3. Unstageable' then 'Unstageable' end) as new_stage,
             max(case when rc.field_nm = 'date_progression' then cast(rc.value as timestamp) end) as progression_dt,
             max(case when rc.field_nm = 'padding_used' then rc.value end) as padding_used,
             max(case when rc.field_nm = 'conf_dt' then rc.value end) as conf_dt,
             max(lpad(case when rc.field_nm = 'mrn' then rc.value end, 8, 0)) as mrn,
             max(rc.submit_dt) as submit_dt
        from rc
        group by rc.record
    ),
    rc_mrn as (
        select
            rcf.*,
            coalesce(rcf.mrn, p.pat_mrn_id) as redcap_mrn
        from
            rc_flat as rcf
            left join {{ source('cdw', 'visit') }} as v on v.enc_id = rcf.redcap_csn
            left join {{ source('cdw', 'patient') }} as p on p.pat_key = v.pat_key
    ),
    stage as (
        select
            record as record_id,
            max(case
                  when rcd_value = 1 then 'Stage 1'
                  when rcd_value = 2 then 'Stage 2'
                  when rcd_value = 3 then 'Stage 3'
                  when rcd_value = 4 then 'Stage 4'
                  when rcd_value = 5 then 'Unstageable'
                  when rcd_value = 6 then 'Deep Tissue Injury (DTI)'
                  when rcd_value = 7 then 'Mucosal'
                  when rcd_value = 8 then 'Indeterminate' end
            ) as injury_stage,
            max(case
                    when rcd_value = 4 then 1
                    when rcd_value = 5 then 2
                    when rcd_value = 3 then 3
                    when rcd_value = 8 then 4
                    when rcd_value = 7 then 5
                    when rcd_value = 2 then 6
                    when rcd_value = 6 then 7
                    when rcd_value = 1 then 8
                    else 9
                end
            ) as injury_stage_raw
          from rc
          where field_nm = 'stage'
          group by 1 --noqa: L054
    ),
    conf_status as (
        select
             record as record_id,
             max(case
                   when rcd_value = '1' then 'Documented by RN but not confirmed'
                   when rcd_value = '2' then 'Confirmed by Skin Champion / CNS'
                   when rcd_value = '3' then 'Confirmed by CWOCN' end) as confirmed_by
          from rc
          where field_nm = 'conf_status'
          group by 1 --noqa: L054
    ),
    source as (
        select
             record as record_id,
             max(case when rcd_value = '1' then 'Epic documentation' else '' end) as source1,
             max(case when rcd_value = '2' then 'SafetyNet report' else '' end) as source2,
             max(case when rcd_value = '3' then 'Wound Consult' else '' end) as source3,
             max(case when rcd_value = '4' then 'Documented by FLOC' else '' end) as source4,
             max(case when row_num = 1 and rcd_value in ('1', '2', '3', '4') then value else '' end)
                 || max(case when row_num = 2 and rcd_value in ('1', '2', '3', '4') then ', ' || value else '' end)
                 || max(case when row_num = 3 and rcd_value in ('1', '2', '3', '4') then ', ' || value else '' end)
                 || max(case when row_num = 4 and rcd_value in ('1', '2', '3', '4') then ', ' || value else '' end
            ) as source_of_documentation
        from rc
        where field_nm = 'source'
        group by 1 --noqa: L054
    ),
    units as (
        select
             rc.record_id,
             max( coalesce(m_unit.historical_dept_key, dept_unit.dept_key) ) as unit_discovered_dept_key,
             max( coalesce(m_unit.historical_dept_id, dept_unit.dept_id) ) as unit_discovered_dept_id,
             max( coalesce(m_unit_other.historical_dept_key, dept_unit_other.dept_key) ) as unit_other_discovered_dept_key,
             max( coalesce(m_unit_other.historical_dept_id, dept_unit_other.dept_id) ) as unit_other_discovered_dept_id,
             max( coalesce(m_unit_other.historical_dept_key, dept_unit_other.dept_key, m_unit.historical_dept_key, dept_unit.dept_key) ) as unit_developed_key,
             max( coalesce(m_unit_other.historical_dept_id, dept_unit_other.dept_id, m_unit.historical_dept_id, dept_unit.dept_id) ) as unit_developed_dept_id
        from
          rc_flat as rc
          left join {{ source('cdw', 'department') }} as dept_unit on cast(rc.unit as int) = dept_unit.dept_id
          left join {{ref('master_harm_prevention_dept_mapping')}} as m_unit
            on m_unit.harm_type = 'HAPI'
            and m_unit.current_dept_id = dept_unit.dept_id
            and rc.date_discovered between m_unit.start_dt and m_unit.end_dt
            and m_unit.denominator_only_ind = 0
          left join {{ source('cdw', 'department') }} as dept_unit_other on cast(rc.unit_other as int) = dept_unit_other.dept_id
          left join {{ref('master_harm_prevention_dept_mapping')}} as m_unit_other
            on m_unit_other.harm_type = 'HAPI'
            and m_unit_other.current_dept_id = dept_unit_other.dept_id
            and rc.date_discovered between m_unit_other.start_dt and m_unit_other.end_dt
            and m_unit_other.denominator_only_ind = 0
        group by 1 --noqa: L054
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
        group by
           c.pat_key, c.census_dt
    )
    select
          case when rc.redcap_mrn is null then 0 else coalesce(p.pat_key, -1) end as pat_key,
        case when rc.unit is null then 0 else coalesce(units.unit_discovered_dept_key, -1) end as discovered_dept_key,
        case when rc.lda_id is null then 0 else coalesce(patient_lda.pat_lda_key, -1) end as pat_lda_key,
        coalesce(rc.date_discovered, '9999-12-31') as discovered_dt,
        coalesce(rc.anatomical_location, 'N/A') as anatomical_location,
        coalesce(units.unit_developed_key, units.unit_discovered_dept_key, -1) as developed_dept_key,
        coalesce(units.unit_other_discovered_dept_key, 0) as discovered_other_dept_key,
        coalesce(s.visit_key, visit.visit_key, -1) as visit_key,
        coalesce(s.room_key, -1) as room_key,
        coalesce(s.bed_key, -1) as bed_key,
        coalesce(s.adt_svc_key, -2) as dict_svc_key,
        units.unit_discovered_dept_id as discovered_dept_id,
        coalesce(units.unit_developed_dept_id, units.unit_discovered_dept_id) as developed_dept_id,
        units.unit_other_discovered_dept_id as discovered_other_dept_id,
        patient_lda.lda_id,
        coalesce(s.enc_id, visit.enc_id) as enc_id,
        s.room_id,
        s.bed_id,
        rc.record_id,
        rc.redcap_csn as redcap_enc_id,
        coalesce(rc.redcap_mrn, p.pat_mrn_id) as pat_mrn_id,
        p.last_nm as pat_last_nm,
        p.first_nm as pat_first_nm,
        p.full_nm as pat_full_nm,
        p.dob as pat_dob,
        p.sex as pat_sex,
        s.adt_svc_nm as svc_nm,
        s.room_nm,
        s.room_num,
        s.bed_nm,
        rc.new_stage,
        rc.date_discovered - date(p.dob) as age_at_discovery,
        case
               when rc.date_discovered - p.dob <= '28 days' then 'Neonate'
               when rc.date_discovered - p.dob <= '365 days' then 'Infancy'
               when rc.date_discovered - p.dob <= '1826 days' then 'Early Childhood'
               when rc.date_discovered - p.dob <= '4748 days' then 'Late Childhood'
               when rc.date_discovered - p.dob <= '6574 days' then 'Adolescence'
               when rc.date_discovered - p.dob > '6574 days' then 'Adult'
          end as age_group,
        conf_status.confirmed_by,
        rc.device_other,
        rc.device_type_1,
        rc.device_type_2,
        rc.hapi_incident_comments,
        stage.injury_stage,
        source.source_of_documentation,
        rc.survey_complete,
        rc.survey_entry_username,
        rc.padding_used,
        rc.wound_description,
        rc.conf_dt,
        rc.submit_dt,
        max(surgery_date.full_dt) over (partition by rc.record_id) as recent_surg_dt,
        rc.progression_dt,
        case when rc.pu_placementdate = '1' then 1 when rc.pu_placementdate = '0' then 0 else -2 end as date_time_assessed_within_24hrs_ind,
        case when rc.device_related = '1' then 1 when rc.device_related = '0' then 0 else -2 end as device_related_ind,
        case when rc.poa = '1' then 1 when rc.poa = '0' then 0 else -2 end as present_on_admission_ind,
        max(case when rc.date_discovered - surgery_date.full_dt between 0 and 4 then 1 else 0 end) over (partition by rc.record_id) as recent_surg_ind,
        coalesce(s.international_ind, -2) as international_ind,
         case
            when
                (ip.ip_unit_ind = 1 or (ip.ip_unit_ind = 0 and was_ip.pat_key is not null))
                and (stage.injury_stage in ('Stage 3', 'Stage 4', 'Unstageable') or rc.new_stage in ('Stage 3', 'Stage 4', 'Unstageable'))
            then 1
            else 0
          end as reportable_ind,
        case when source.source1 is not null then 1 else 0 end as source_epic_documentation_ind,
        case when source.source4 is not null then 1 else 0 end as source_floc_ind,
        case when source.source2 is not null then 1 else 0 end as source_safetynet_ind,
        case when source.source3 is not null then 1 else 0 end as source_wound_consult_ind,
        case when rc.string_tightness = '1' then 1 when rc.string_tightness = '0' then 0 else -2 end as string_tightness_ind,
        case when rc.string_check = '1' then 1 when rc.string_check = '0' then 0 else -2 end as string_check_24hrs_ind,
        row_number() over (partition by
                                coalesce(s.pat_key, 0),
                                coalesce(units.unit_discovered_dept_key, 0),
                                case when rc.lda_id is null then 0 else coalesce(patient_lda.pat_lda_key, -1) end,
                                coalesce(rc.date_discovered, '9999-12-31'),
                                coalesce(rc.anatomical_location, 'N/A')
                             order by
                                case when upper(rc.survey_complete) like 'COMPLETE%' then 1 else 0 end desc,
                                stage.injury_stage_raw asc,
                                rc.record_id desc,
                                s.adt_svc_key desc,
                                s.bed_key desc
         ) as rownum
    from
          rc_mrn as rc
          left join {{ source('cdw', 'patient') }} as p on p.pat_mrn_id = rc.redcap_mrn
          left join {{ ref('stg_visit_event_service') }} as s on s.pat_key = p.pat_key and rc.date_discovered between date(s.enter_dt) and date(s.exit_dt)
          left join {{ source('cdw', 'or_log') }} as or_log on or_log.pat_key = coalesce(s.pat_key, p.pat_key)
          left join stage on rc.record_id = stage.record_id
          left join conf_status on rc.record_id = conf_status.record_id
          left join source on rc.record_id = source.record_id
          left join units on rc.record_id = units.record_id
          left join {{ ref('stg_harm_dept_ip_op') }} as ip on ip.dept_key = units.unit_developed_key
          left join {{ source('cdw', 'patient_lda') }} as patient_lda on rc.lda_id = cast(cast(patient_lda.lda_id as float) as varchar(25))
          left join {{ source('cdw', 'master_date') }} as surgery_date on or_log.surg_dt_key = surgery_date.dt_key
          left join {{ source('cdw', 'visit') }} as visit on visit.enc_id = rc.redcap_csn
          left join was_ip
             on was_ip.pat_key = p.pat_key
             and was_ip.census_dt = date(rc.date_discovered)
    where
          upper(conf_status.confirmed_by) like '%CONFIRM%'
          and (rc.poa is null or rc.poa = '0')
          and (rc.conf_status2 is null or rc.conf_status2 = 1)
) as hapi --noqa: L025
where rownum = 1
