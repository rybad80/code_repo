{{ config(database=env_var('CDW_STG_DB', 'CDW_STG_UAT')) }}
with initial_and_final_center as (
    select distinct
      visit_ed_event.visit_key,
      first_value(department.dept_key) over(partition by visit_ed_event.visit_key
                                            order by coalesce(visit_ed_event.event_dt, visit_ed_event.event_rec_dt) asc
                                           ) as initial_ed_dept_key,
      first_value(clarity_dep.center_c) over(partition by visit_ed_event.visit_key
                                             order by coalesce(visit_ed_event.event_dt, visit_ed_event.event_rec_dt) asc
                                            ) as initial_ed_department_center_id,
      first_value(zc_center.abbr) over(partition by visit_ed_event.visit_key
                                       order by coalesce(visit_ed_event.event_dt, visit_ed_event.event_rec_dt) asc
                                      ) as initial_ed_department_center_abbr,
      first_value(department.dept_key) over(partition by visit_ed_event.visit_key
                                            order by coalesce(visit_ed_event.event_dt, visit_ed_event.event_rec_dt) desc
                                           ) as final_ed_dept_key,
      first_value(clarity_dep.center_c) over(partition by visit_ed_event.visit_key
                                             order by coalesce(visit_ed_event.event_dt, visit_ed_event.event_rec_dt) desc
                                            ) as final_ed_department_center_id,
      first_value(zc_center.abbr) over(partition by visit_ed_event.visit_key
                                       order by coalesce(visit_ed_event.event_dt, visit_ed_event.event_rec_dt) desc
                                      ) as final_ed_department_center_abbr
    from
      {{source('cdw', 'visit_ed_event')}} as visit_ed_event
      inner join {{source('cdw', 'master_event_type')}} as master_event_type on visit_ed_event.event_type_key = master_event_type.event_type_key
      left join {{source('clarity_ods', 'ed_iev_event_info')}} as ed_iev_event_info
        on visit_ed_event.pat_event_id = ed_iev_event_info.event_id
           and visit_ed_event.seq_num = ed_iev_event_info.line
      left join {{source('clarity_ods', 'clarity_dep')}} as clarity_dep on ed_iev_event_info.event_dept_id = clarity_dep.department_id
      left join {{source('cdw', 'department')}} as department on clarity_dep.department_id = department.dept_id
      left join {{source('clarity_ods', 'zc_center')}} as zc_center on clarity_dep.center_c = zc_center.center_c
      where
        master_event_type.event_id in (50, -- patient arrived in ed
                                      55, -- patient roomed in ed
                                      95, -- ed tracking end
                                      205, -- triage started
                                      210, -- triage completed
                                      300121, -- assign resident/np
                                      120, -- assign nurse
                                      111, -- assign attending
                                      215, -- registration started
                                      220, -- registration completed
                                      300711, -- ed conference review
                                      30020501, -- md eval
                                      30020502, -- attending eval
                                      85, -- avs printed
                                      300100, -- ed admit-md report given
                                      300101, -- ed admit-ip nurse paged
                                      300105, -- bed assigned
                                      231, -- bed requested
                                      300103, -- ip md paged
                                      300835, -- ready to plan
                                      300102, -- ed admit-handoff report review
                                      300122, -- assign ed fellow
                                      300940, -- edecu rn paged
                                      300941) -- verbal report given
        and visit_ed_event.visit_key <> -1
        and ed_iev_event_info.event_dept_id in (10292012, -- MAIN EMERGENCY DEPT
                                                101003001 -- KOPH EMERGENCY DEP
                                               )
)

select
  visit_ed_event.visit_key,
  initial_and_final_center.initial_ed_dept_key,
  initial_and_final_center.initial_ed_department_center_id,
  initial_and_final_center.initial_ed_department_center_abbr,
  initial_and_final_center.final_ed_dept_key,
  initial_and_final_center.final_ed_department_center_id,
  initial_and_final_center.final_ed_department_center_abbr,
  min(case
        when master_event_type.event_id = 50
             and visit_ed_event.event_dt::date <= '20121231'
          then visit_ed_event.event_dt
        when clarity_dep.dep_ed_type_c in (
                                           1, -- Emergency Department
                                           3  -- Observation
                                          )
             and master_event_type.event_id = 50
          then visit_ed_event.event_dt
        when ed_iev_event_info.event_dept_id = 101001060  -- BED MANAGEMENT CENT*
             and master_event_type.event_id = 50
          then visit_ed_event.event_dt
        when ed_iev_event_info.event_dept_id is null
             and master_event_type.event_id = 50
          then visit_ed_event.event_dt
        else null::timestamp
      end) as arrive_ed_dt,
  max(case
        when master_event_type.event_id = 95 then visit_ed_event.event_dt
        else null::timestamp
      end) as depart_ed_dt,
  min(case
        when master_event_type.event_id = 205 then visit_ed_event.event_dt
        else null::timestamp
      end) as triage_start_dt,
  max(case
        when master_event_type.event_id = 210 then visit_ed_event.event_dt
        else null::timestamp
      end) as triage_end_dt,
  min(case
        when master_event_type.event_id = 120 then visit_ed_event.event_dt
        else null::timestamp
      end) as assign_rn_dt,
  min(case
        when master_event_type.event_id = 300121 then visit_ed_event.event_dt
        else null::timestamp
      end) as assignresident_np_dt,
  min(case
        when master_event_type.event_id = 111 then visit_ed_event.event_dt
        else null::timestamp
      end) as assign_1st_attending_dt,
  min(case
        when master_event_type.event_id = 215 then visit_ed_event.event_dt
        else null::timestamp
      end) as registration_start_dt,
  min(case
        when master_event_type.event_id = 55 then visit_ed_event.event_dt
        else null::timestamp
      end) as roomed_ed_dt,
  max(case
        when master_event_type.event_id = 220 then visit_ed_event.event_dt
        else null::timestamp
      end) as registration_end_dt,
  max(case
        when master_event_type.event_id = 300711 then visit_ed_event.event_dt
        else null::timestamp
      end) as ed_conference_review_dt,
  min(case
        when master_event_type.event_id = 30020501 then visit_ed_event.event_dt
        else null::timestamp
      end) as md_evaluation_dt,
  min(case
        when master_event_type.event_id = 30020502 then visit_ed_event.event_dt
        else null::timestamp
      end) as attending_evaluation_dt,
  min(case
        when master_event_type.event_id = 85 then visit_ed_event.event_dt
        else null::timestamp
      end) as after_visit_summaryprinted_dt,
  min(case
        when master_event_type.event_id = 300100 then visit_ed_event.event_dt
        else null::timestamp
      end) as md_report_dt,
  min(case
        when master_event_type.event_id = 300101 then visit_ed_event.event_dt
        else null::timestamp
      end) as paged_ip_rn_dt,
  min(case
        when master_event_type.event_id = 300103 then visit_ed_event.event_dt
        else null::timestamp
      end) as paged_ip_md_dt,
  min(case
        when master_event_type.event_id = 300105 then visit_ed_event.event_dt
        else null::timestamp
      end) as ip_bed_assigned_dt,
  min(case
        when master_event_type.event_id = 231 then visit_ed_event.event_dt
        else null::timestamp
      end) as admission_form_bed_request_dt,
  min(case
        when master_event_type.event_id = 300835 then visit_ed_event.event_dt
        else null::timestamp
      end) as ready_to_plan_dt,
  min(case
        when master_event_type.event_id in (111, 300121, 300103) then visit_ed_event.event_dt
        else null::timestamp
      end) as earliest_md_eval_dt,
  min(case
        when master_event_type.event_id in (300102, 300103, 300122, 300940, 300941) then visit_ed_event.event_dt
        else null::timestamp
      end) as earliest_rn_report_dt
from
  {{source('cdw', 'visit_ed_event')}} as visit_ed_event
  inner join {{source('cdw', 'master_event_type')}} as master_event_type on visit_ed_event.event_type_key = master_event_type.event_type_key
  left join initial_and_final_center on visit_ed_event.visit_key = initial_and_final_center.visit_key
  left join {{source('clarity_ods', 'ed_iev_event_info')}} as ed_iev_event_info
    on visit_ed_event.pat_event_id = ed_iev_event_info.event_id
        and visit_ed_event.seq_num = ed_iev_event_info.line
  left join {{source('clarity_ods', 'clarity_dep')}} as clarity_dep on ed_iev_event_info.event_dept_id = clarity_dep.department_id
where
  master_event_type.event_id in (50, -- Patient arrived in ED
                                 55, -- Patient roomed in ED
                                 95, -- ED Tracking End
                                 205, -- Triage Started
                                 210, -- Triage Completed
                                 300121, -- Assign Resident/NP
                                 120, -- Assign Nurse
                                 111, -- Assign Attending
                                 215, -- Registration Started
                                 220, -- Registration Completed
                                 300711, -- ED Conference Review
                                 30020501, -- MD Eval
                                 30020502, -- Attending Eval
                                 85, -- AVS Printed
                                 300100, -- ED Admit-MD report given
                                 300101, -- ED Admit-IP nurse paged
                                 300105, -- Bed Assigned
                                 231, -- Bed Requested
                                 300103, -- IP MD PAGED
                                 300835, -- Ready to Plan
                                 300102, -- ED Admit-Handoff Report review
                                 300122, -- Assign ED Fellow
                                 300940, -- EDECU RN Paged
                                 300941) -- Verbal Report Given
  and visit_ed_event.visit_key <> -1
group by
  visit_ed_event.visit_key,
  initial_and_final_center.initial_ed_dept_key,
  initial_and_final_center.initial_ed_department_center_id,
  initial_and_final_center.initial_ed_department_center_abbr,
  initial_and_final_center.final_ed_dept_key,
  initial_and_final_center.final_ed_department_center_id,
  initial_and_final_center.final_ed_department_center_abbr
order by
  visit_ed_event.visit_key
