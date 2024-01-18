with enc_sort_rn_treat_team_exists as (
    select vt.visit_key
 from
      {{source('cdw', 'visit_treatment')}} as vt
      inner join {{source('cdw', 'cdw_dictionary')}} as dict1 on vt.dict_treat_rel_key = dict1.dict_key
    where
      lower(dict1.dict_nm) = 'sort rn'
    group by
      vt.visit_key
),

test_patients as (
    select
      patient.pat_key,
      patient.pat_id,
      max(case
              when patient_type.pat_id is not null
                then 1
              else 0
          end) as patient_type_ind,
      max(case
              when patient_3.pat_id is not null
                then 1
              else 0
          end) as test_patient_flag_ind
    from
      {{source('cdw', 'patient')}} as patient
      left join {{source('clarity_ods', 'patient_type')}} as patient_type
        on patient.pat_id = patient_type.pat_id
           and patient_type.patient_type_c in (
                                               '16'
                                              )
      left join {{source('clarity_ods', 'patient_3')}} as patient_3
        on patient.pat_id = patient_3.pat_id
           and lower(patient_3.is_test_pat_yn) = 'y'
    where
      coalesce(patient_type.pat_id, patient_3.pat_id) is not null
    group by
      patient.pat_key,
      patient.pat_id
)

select
  vis.visit_key,
  vis.pat_key,
  vis.hsp_acct_key,
  vis.dict_acuity_key,
  vis.dict_dspn_key,
  coalesce(vis.admit_dept_key, 0) as admit_dept_key,
  vis.enc_id,
  vw_ed.initial_ed_dept_key,
  vw_ed.initial_ed_department_center_id,
  vw_ed.initial_ed_department_center_abbr,
  vw_ed.final_ed_dept_key,
  vw_ed.final_ed_department_center_id,
  vw_ed.final_ed_department_center_abbr,
  vis.edecu_arrvl_dt,
  vw_ed.arrive_ed_dt,
  edqi_adt.admit_ed_dt,
  coalesce(edqi_adt.admit_edecu_dt, edqi_adt.disch_ed_dt) as disch_ed_dt,
  edqi_adt.admit_edecu_dt,
  case
      when coalesce(edqi_adt.disch_edecu_dt, vis.edecu_arrvl_dt) is not null
        then coalesce(edqi_adt.disch_edecu_dt, edqi_adt.disch_ed_dt)
  end as disch_edecu_dt,
  vw_ed.depart_ed_dt,
  vw_ed.triage_start_dt,
  vw_ed.triage_end_dt,
  vw_ed.assign_rn_dt,
  vw_ed.assignresident_np_dt as assign_resident_np_dt,
  vw_ed.assign_1st_attending_dt,
  vw_ed.registration_start_dt,
  vw_ed.roomed_ed_dt,
  vw_ed.registration_end_dt,
  vw_ed.ed_conference_review_dt,
  vw_ed.md_evaluation_dt,
  vw_ed.attending_evaluation_dt,
  vw_ed.after_visit_summaryprinted_dt as after_visit_summary_printed_dt,
  vw_ed.md_report_dt,
  vw_ed.paged_ip_rn_dt,
  vw_ed.paged_ip_md_dt,
  vw_ed.ip_bed_assigned_dt,
  vw_ed.admission_form_bed_request_dt,
  vw_ed.earliest_md_eval_dt,
  vw_ed.earliest_rn_report_dt,
  vw_ed.ready_to_plan_dt,
  vis.age as age_at_visit,
  vis.age_days as age_days_at_visit,
  extract(epoch from coalesce(vis.edecu_arrvl_dt, vw_ed.depart_ed_dt) - vw_ed.earliest_md_eval_dt)::numeric(20, 2) / 60 as md_eval_to_pt_left_ed_min,
  extract(epoch from coalesce(vis.edecu_arrvl_dt, vw_ed.depart_ed_dt) - vw_ed.arrive_ed_dt)::numeric(20, 2) / 60 as ed_los,
  extract(epoch from vw_ed.depart_ed_dt - coalesce(vis.edecu_arrvl_dt, vw_ed.depart_ed_dt))::numeric(20, 2) / 60 as edecu_los,
  extract(epoch from vw_ed.triage_start_dt - vw_ed.arrive_ed_dt)::numeric(20, 2) / 60 as arrival_to_triage_min,
  case
    when rn_sort.visit_key is not null then 0::numeric(20, 2)
    else extract(epoch from vw_ed.triage_start_dt - vw_ed.arrive_ed_dt)::numeric(20, 2) / 60
  end as arrival_to_triage_min_adjusted,
  extract(epoch from vw_ed.earliest_md_eval_dt - vw_ed.arrive_ed_dt)::numeric(20, 2) / 60 as arrival_to_md_eval_min,
  /* extract(epoch from vw_ed.admission_form_bed_request_dt- vw_ed.earliest_md_eval_dt)::numeric(20,2) / 60 as md_eval_to_bed_request_min,*/
  case
    when vw_ed.arrive_ed_dt >= '2015-10-20' then extract(epoch from vw_ed.ready_to_plan_dt - vw_ed.earliest_md_eval_dt)::numeric(20, 2) / 60
    else extract(epoch from vw_ed.admission_form_bed_request_dt - vw_ed.earliest_md_eval_dt)::numeric(20, 2) / 60
   end as md_eval_to_bed_request_min,
  /* extract(epoch from vw_ed.md_report_dt - vw_ed.admission_form_bed_request_dt)::numeric(20,2) / 60 as bed_request_to_md_report_min,*/
  case
    when vw_ed.arrive_ed_dt >= '2015-10-20' then extract(epoch from vw_ed.md_report_dt - vw_ed.ready_to_plan_dt)::numeric(20, 2) / 60
    else extract(epoch from vw_ed.md_report_dt - vw_ed.admission_form_bed_request_dt)::numeric(20, 2) / 60
  end as bed_request_to_md_report_min,
  extract(epoch from coalesce(vis.edecu_arrvl_dt, vw_ed.depart_ed_dt) - vw_ed.md_report_dt)::numeric(20, 2) / 60 as md_report_to_pt_left_min,
  /* extract(epoch from coalesce(vis.edecu_arrvl_dt,vw_ed.depart_ed_dt) - vw_ed.admission_form_bed_request_dt)::numeric(20,2) / 60 as bed_request_to_pt_left_min ,*/
  case
    when vw_ed.arrive_ed_dt >= '2015-10-20' then extract(epoch from coalesce(vis.edecu_arrvl_dt, vw_ed.depart_ed_dt) - vw_ed.ready_to_plan_dt)::numeric(20, 2) / 60
    else extract(epoch from coalesce(vis.edecu_arrvl_dt, vw_ed.depart_ed_dt) - vw_ed.admission_form_bed_request_dt)::numeric(20, 2) / 60
  end as bed_request_to_pt_left_min,
  extract(epoch from vw_ed.ready_to_plan_dt - vw_ed.earliest_md_eval_dt)::numeric(20, 2) / 60 as md_eval_to_ready_to_plan_min,
  extract(epoch from vw_ed.md_report_dt - vw_ed.ready_to_plan_dt)::numeric(20, 2) / 60 as ready_to_plan_to_md_report_min,
  extract(epoch from coalesce(vis.edecu_arrvl_dt, vw_ed.depart_ed_dt) - vw_ed.ready_to_plan_dt)::numeric(20, 2) / 60 as ready_to_plan_to_pt_left_min,
  extract(epoch from coalesce(vis.edecu_arrvl_dt, vw_ed.depart_ed_dt) - vw_ed.paged_ip_rn_dt)::numeric(20, 2) / 60 as paged_ip_rn_to_pt_left_min,
  extract(epoch from vw_ed.md_report_dt - vw_ed.paged_ip_md_dt)::numeric(20, 2) / 60 as paged_ip_md_to_md_report_min,
  vis.ed_disposition,
  vis.ed_general_disposition,
  vis.ed_resuscitation_rm_use_ind,
  vis.ed_patients_seen_ind,
  vis.ed_patients_presenting_ind,
  case
    when vis.edecu_arrvl_dt is not null then 1
    when edqi_adt.admit_edecu_dt is not null then 1
    when edqi_adt.disch_edecu_dt is not null then 1
    else 0
  end as edecu_ind,
  case
    when lower(vis.ed_general_disposition) = 'lwbs' then 0
    when lag(lower(vis.ed_general_disposition)) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id) != 'discharge' then 0
    when date_part('epoch', (vw_ed.arrive_ed_dt - lag(vw_ed.depart_ed_dt) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ))) / 3600 <= '72' then 1
    else 0
  end as hr_72_revisit_ind,
  case
    when lower(vis.ed_general_disposition) = 'lwbs' or lower(vis.ed_general_disposition) != 'discharge' then 0
    when lead(lower(vis.ed_general_disposition)) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id) = 'lwbs'
      then
        case
          when lead(lower(vis.ed_general_disposition), 2) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt ) = 'lwbs' then 0
          else date_part('epoch', (vw_ed.depart_ed_dt - lead(vw_ed.arrive_ed_dt, 2) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ))) / -3600
        end
    else date_part('epoch', (vw_ed.depart_ed_dt - lead(vw_ed.arrive_ed_dt) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ))) / -3600
  end as hrs_revist_first_visit,
  case
    when lower(vis.ed_general_disposition) = 'lwbs' or lower(vis.ed_general_disposition) != 'discharge' then 0
    when lead(lower(vis.ed_general_disposition)) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ) = 'lwbs'
      then
        case
          when lead(lower(vis.ed_general_disposition), 2) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ) = 'lwbs' then 0
          when date_part('epoch', (vw_ed.depart_ed_dt - lead(vw_ed.arrive_ed_dt, 2) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id))) / -3600 <= '72' then 1
          else 0
        end
    when date_part('epoch', (vw_ed.depart_ed_dt - lead(vw_ed.arrive_ed_dt) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ))) / -3600 <= '72' then 1
    else 0
  end as hr_72_revisit_first_visit_ind,
  case
    when lower(vis.ed_general_disposition) = 'lwbs' or lower(vis.ed_general_disposition) != 'discharge' then 0
    when lead(lower(vis.ed_general_disposition)) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ) = 'lwbs'
      then
        case
          when lead(lower(vis.ed_general_disposition), 2) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ) = 'lwbs' then 0
          when date_part('epoch', (vw_ed.depart_ed_dt - lead(vw_ed.arrive_ed_dt, 2) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ))) / -3600 <= '72'
            then
              case
                when lead(lower(vis.ed_general_disposition), 2) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id) in ('admit', 'edecu', 'transfer not from triage') then 1
                else 0
              end
          else 0
        end
    when date_part('epoch', (vw_ed.depart_ed_dt - lead(vw_ed.arrive_ed_dt) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt, vis.enc_id ))) / -3600 <= '72'
      then
        case
          when lead(lower(vis.ed_general_disposition)) over (partition by vis.pat_key order by vw_ed.arrive_ed_dt ) in ('admit', 'edecu', 'transfer not from triage') then 1
          else 0
        end
    else 0
  end as hr_72_revisit_first_visit_admit_ind,
  1 as patient_counter,
  vis.pediatric_age_days_group::varchar(100) as pediatric_age_days_group,
  now() as create_dt,
  cast('ETL' as varchar(20)) as create_by,
  now() as upd_dt,
  cast('ETL' as varchar(20)) as upd_by
from
  {{ref('s_cdw_edqi_visit_ed_event')}} as vw_ed
  inner join {{ref('s_cdw_edqi_visit')}} as vis on vw_ed.visit_key = vis.visit_key
  left join enc_sort_rn_treat_team_exists as rn_sort on vis.visit_key = rn_sort.visit_key
  left join {{ref('s_cdw_edqi_adt')}} as edqi_adt on vw_ed.visit_key = edqi_adt.visit_key
  left join test_patients on vis.pat_key = test_patients.pat_key
where
  vw_ed.arrive_ed_dt is not null
  and test_patients.pat_key is null
