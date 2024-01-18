with rc as (
     select
          rcp.app_title,
          rcq.mstr_redcap_quest_key,
          rcq.field_order,
          rcq.field_nm,
          rcq.element_label,
          rcd.record,
          rcd.value as rcd_value,
          rcea.element_id,
          SUBSTR(COALESCE(rcea.element_desc, rcd.value), 1, 250) as value,
          rsr.return_cd,
        rsr.timestamps
     from {{ source('cdw', 'redcap_detail') }} as rcd
          left join {{ source('cdw', 'master_redcap_project') }} as rcp on rcp.mstr_project_key = rcd.mstr_project_key
          left join {{ source('cdw', 'master_redcap_question') }} as rcq on rcq.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key
          left join {{ source('cdw', 'master_redcap_element_answr') }} as rcea on rcea.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key and rcd.value = rcea.element_id
          left join ( --noqa: L042
                  select
rsr.redcap_record,
                        rsr.mstr_redcap_event_key,
                        UPPER(MAX(rsr.survey_response_return_cd)) as return_cd,
                        MAX(rsr.survey_response_first_submit_dt) as timestamps
                  from {{ source('cdw', 'master_redcap_survey_response') }} as rsr
                  group by rsr.redcap_record, rsr.mstr_redcap_event_key
                 ) as rsr on rsr.mstr_redcap_event_key = rcd.mstr_redcap_event_key and rsr.redcap_record = rcd.record
     where
          rcd.cur_rec_ind = 1
          and rcp.project_id = 64
     order by rcd.record, rcq.field_order
),
results as (
     select
         rc.record as record_id,
        MAX(rc.timestamps) as submit_dt,
        MAX(case when rc.field_nm = 'unit_develop' then rc.rcd_value end) as unit,
        MAX(case when rc.field_nm = 'unit_develop' then rc.value end) as unit_where_clot_developed,
        MAX(case when rc.field_nm = 'mrn' then LPAD(rc.value, 8, 0) end) as mrn,
        MAX(case when rc.field_nm = 'event_dt' and LENGTH(rc.value) > 1 then rc.value end) as event_dt,
        MAX(case when rc.field_nm = 'nonline_vte' then CAST(rc.value as int) end) as nonline_vte_above12,
        MAX(case when rc.field_nm = 'nonline_under12' then CAST(rc.value as int) end) as nonline_vte_under12,
        MAX(case when rc.field_nm = 'line_vte' then CAST(rc.value as int) end) as line_vte,
        MAX(case when rc.field_nm = 'sps_linetype' then rc.value end) as line_type,
        MAX(case when rc.field_nm = 'count' then CAST(rc.value as numeric(12, 0)) end) as num_total_vte,
        MAX(case when rc.field_nm = 'cath_assoc' then rc.value end) as catheter_assoc,
        MAX(case when rc.field_nm = 'lumen' then rc.value end) as lumen_type,
        MAX(case when rc.field_nm = 'sps_cathsize' then rc.value end) as cvc_size,
        MAX(case when rc.field_nm = 'location' then rc.value end) as vein_location,
        MAX(case when rc.field_nm = 'clot_num' then rc.value end) as num_clots_during_admission,
        MAX(case when rc.field_nm = 'abstraction_complete' then rc.value end) as redcap_complete,
        MAX(case when rc.field_nm = 'conf_dt' then rc.value end) as conf_dt
     from rc
     group by rc.record
),
service as (
    select
        r.record_id,
        v.*,
        ROW_NUMBER() over (partition by r.record_id
                             order by case when d.dept_id = COALESCE(r.unit, '0') then 1 else 0 end desc,
                                      COALESCE(v.bed_key, 0) desc,
                                      v.adt_svc_key desc
          ) as rownum
    from
        results as r
        inner join {{ ref('stg_visit_event_service') }} as v on v.pat_mrn_id = LTRIM(RTRIM(LPAD(r.mrn, 8, 0)))
        left join {{ source('cdw', 'department') }} as d on d.dept_key = v.dept_key
    where
        r.event_dt between DATE(v.enter_dt) and DATE(v.exit_dt)
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
    CAST(COALESCE(r.record_id, '-1') as bigint) as record_id,
    COALESCE(s.visit_key, -1) as visit_key,
    case when r.mrn is null then 0 else COALESCE(p.pat_key, -1) end as pat_key,
    COALESCE(m.historical_dept_key, d.dept_key, -1) as developed_dept_key,
    COALESCE(s.room_key, -1) as room_key,
    COALESCE(s.bed_key, -1) as bed_key,
    COALESCE(s.adt_svc_key, -2) as dict_svc_key,
    CAST(COALESCE(m.historical_dept_id, d.dept_id, -1) as bigint) as developed_dept_id,
    CAST(r.unit as varchar(50)) as redcap_unit_id,
    CAST(COALESCE(r.mrn, p.pat_mrn_id) as varchar(25)) as pat_mrn_id,
    CAST(p.last_nm as varchar(100)) as pat_last_nm,
    CAST(p.first_nm as varchar(100)) as pat_first_nm,
    CAST(p.full_nm as varchar(200)) as pat_full_nm,
    p.dob as pat_dob,
    p.sex as pat_sex,
    CAST(s.adt_svc_nm as varchar(50)) as svc_nm,
    CAST(s.room_nm as varchar(200)) as room_nm,
    CAST(s.room_num as varchar(100)) as room_num,
    CAST(s.bed_nm as varchar(300)) as bed_nm,
    CAST(COALESCE(r.num_clots_during_admission, '0') as smallint) as num_clots_during_admission,
    CAST(r.num_total_vte as smallint) as num_total_vte,
    CAST(r.vein_location as varchar(30)) as vein_location,
    CAST(r.cvc_size as varchar(30)) as cvc_size,
    CAST(r.lumen_type as varchar(20)) as lumen_type,
    CAST(r.line_type as varchar(50)) as line_type,
    case when r.nonline_vte_above12 = 1 then CAST('Non-Line Associated; >=12 yr' as varchar(30))
         when r.nonline_vte_under12 = 1 then CAST('Non-Line Associated; <12 yr' as varchar(30))
         when r.line_vte = 1 then CAST('Line Associated' as varchar(30))
    end as vte_type,
    CAST(r.conf_dt as timestamp) as conf_dt,
    CAST(r.submit_dt as timestamp) as submit_dt,
    CAST(r.event_dt as timestamp) as event_dt,
    case
        when UPPER(r.redcap_complete) like 'COMPLETE%' then CAST(1 as byteint)
        when UPPER(r.redcap_complete) not like 'COMPLETE%' then CAST(0 as byteint)
        else CAST(-2 as byteint)
    end as redcap_complete_ind,
    case
        when UPPER(r.catheter_assoc) = 'YES' then CAST(1 as byteint)
        when UPPER(r.catheter_assoc) = 'NO' then CAST(0 as byteint)
        else CAST(-2 as byteint)
    end as catheter_assoc_ind,
    CAST(COALESCE(r.line_vte, -2) as byteint) as line_vte_ind,
    CAST(COALESCE(r.nonline_vte_above12, -2) as byteint) as nonline_vte_12_and_above_ind,
    CAST(COALESCE(r.nonline_vte_under12, -2) as byteint) as nonline_vte_under_12_ind,
    CAST(COALESCE(s.international_ind, -2) as byteint) as international_ind,
    case
        when
            COALESCE(r.nonline_vte_above12, -2) = 1
            and d.dept_key is not null
            and (ip.ip_unit_ind = 1 or (ip.ip_unit_ind = 0 and was_ip.pat_key is not null))
            then CAST(1 as byteint)
        else CAST(0 as byteint)
    end as reportable_ind,
    CURRENT_TIMESTAMP as create_dt,
    'DBT' as create_by,
    CURRENT_TIMESTAMP as upd_dt
from
     results as r
     left join service as s on s.record_id = r.record_id and s.rownum = 1
     left join {{ source('cdw', 'patient') }} as p on p.pat_mrn_id = LPAD(r.mrn, 8, 0)
     left join {{ source('cdw', 'department') }} as d on d.dept_id = r.unit
     left join {{ ref('master_harm_prevention_dept_mapping') }} as m
        on m.harm_type = 'VTE'
        and m.current_dept_key = d.dept_key
        and r.event_dt between m.start_dt and m.end_dt
        and m.denominator_only_ind = 0
     left join was_ip
        on was_ip.pat_key = p.pat_key
        and was_ip.census_dt = DATE(r.event_dt)
     left join {{ ref('stg_harm_dept_ip_op') }} as ip on ip.dept_key = COALESCE(m.historical_dept_key, d.dept_key, -1)
where r.num_total_vte > 0
