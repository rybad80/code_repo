{{ config(database=env_var('CDW_STG_DB', 'CDW_STG_UAT')) }}
select
  s_cdw_edqi_visit_ed_event.visit_key,
  min(case
          when dept.dept_id in (10292012,  -- MAIN EMERGENCY DEPT
                                101003001 -- KOPH EMERGENCY DEP
                               )
               and d_adt_event.src_id in (1, 3)
               and visit_event.eff_event_dt is not null
            then visit_event.eff_event_dt
      end) as admit_ed_dt,
  max(case
          when dept.dept_id in (10292012, -- MAIN EMERGENCY DEPT
                                101003001 -- KOPH EMERGENCY DEP
                               )
               and d_adt_event.src_id in (2, 4)
               and visit_event.eff_event_dt is not null
            then visit_event.eff_event_dt
      end) as disch_ed_dt,
  min(case
          when dept.dept_id = 10201512  -- 10201512	ED EXTENDED CARE UN*
               and d_adt_event.src_id in (1, 3)
               and visit_event.eff_event_dt is not null
            then visit_event.eff_event_dt
          when dept.dept_id = 10292012 -- MAIN EMERGENCY DEPT
               and visit_event.eff_event_dt is not null
               and dict_pat_class.src_id = 5  -- Observation
               and lower(master_bed.bed_nm) like 'ec%'
            then visit_event.eff_event_dt
      end) as admit_edecu_dt,
  max(case
          when dept.dept_id = 10201512  -- 10201512	ED EXTENDED CARE UN*
               and d_adt_event.src_id in (2, 4)
               and visit_event.eff_event_dt is not null
            then visit_event.eff_event_dt
      end) as disch_edecu_dt,
  max(case
          when lower(master_bed.bed_nm) like 'ec%' then 1
          else 0
      end) as edecu_bed_ind
from
  {{ref('s_cdw_edqi_visit_ed_event')}} as s_cdw_edqi_visit_ed_event
  join {{source('cdw', 'visit_event')}} as visit_event on s_cdw_edqi_visit_ed_event.visit_key = visit_event.visit_key
  join {{source('cdw', 'department')}} as dept on visit_event.dept_key = dept.dept_key
  join {{source('cdw', 'cdw_dictionary')}} as d_adt_event on visit_event.dict_adt_event_key = d_adt_event.dict_key
  join {{source('cdw', 'cdw_dictionary')}} as d_event_subtype on visit_event.dict_event_subtype_key = d_event_subtype.dict_key
  left join {{source('cdw', 'master_bed')}} as master_bed on visit_event.bed_key = master_bed.bed_key
  left join {{source('cdw', 'cdw_dictionary')}} as dict_pat_class on visit_event.dict_pat_class_key = dict_pat_class.dict_key
where
  dept.dept_id in (10292012, -- MAIN EMERGENCY DEPT
                   101003001, -- KOPH EMERGENCY DEP
                   10201512  -- ED EXTENDED CARE UN*
                  )
  and d_event_subtype.src_id <> 2   -- Canceled
group by
  s_cdw_edqi_visit_ed_event.visit_key
