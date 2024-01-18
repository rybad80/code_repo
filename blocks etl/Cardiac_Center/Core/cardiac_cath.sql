with caseids as (
     select
          lower(cath_study.case_id) as unique_case_id, count(*) as cnumber
     from
           {{source('cdw', 'cath_study')}} as cath_study
     group by
          lower(cath_study.case_id)
     having
          count(*) = 1
),
procedure_type_tpvr as (
     select distinct
          cath_study_procedure_performed.cath_study_id
     from
           {{source('cdw', 'cath_study_procedure_performed')}} as cath_study_procedure_performed
     where
          lower(cath_study_procedure_performed.procedure_performed_name) = 'stent placement: transcatheter valve'
),
procedure_type as (
     select
          cath_study.cath_study_id,
          case
               when procedure_type_tpvr.cath_study_id is not null then 'TPVR'
               when lower(procedure_type) = '_' then 'Diagnostic'
               when lower(procedure_type) = 'aicd' then 'EP'
               when lower(procedure_type) = 'angiography' then 'Diagnostic'
               when lower(procedure_type) = 'annual cath' then 'Diagnostic'
               when lower(procedure_type) = 'arrhythmia monitor implant' then 'EP'
               when lower(procedure_type) = 'biopsy / rrt hrt cath' then 'Diagnostic'
               when lower(procedure_type) = 'biopsy right heart' then 'Diagnostic'
               when lower(procedure_type) like '%dft%s%' then 'EP'
               when lower(procedure_type) = 'diagnostic (r&l hrt caths)' then 'Diagnostic'
               when lower(procedure_type) = 'diagnostic r & l heart' then 'Diagnostic'
               when lower(procedure_type) = 'ep (without rfa)' then 'EP'
               when lower(procedure_type) = 'ep: rfa' then 'EP'
               when lower(procedure_type) = 'eps' then 'EP'
               when lower(procedure_type) = 'fluoro' then 'Diagnostic'
               when lower(procedure_type) = 'interventional' then 'Intervention (Non-TPVR)'
               when lower(procedure_type) = 'lead extraction' then 'EP'
               when lower(procedure_type) = 'left heart' then 'Diagnostic'
               when lower(procedure_type) = 'left heart cath' then 'Diagnostic'
               when lower(procedure_type) = 'lymph access' then 'Lymphatic'
               when lower(procedure_type) = 'lymph intervention' then 'Lymphatic'
               when lower(procedure_type) = 'other' then 'Diagnostic'
               when lower(procedure_type) = 'pacemaker procedure' then 'EP'
               when lower(procedure_type) = 'pacemaker procedures' then 'EP'
               when lower(procedure_type) = 'pericardialcentesis/pleural' then 'Diagnostic'
               when lower(procedure_type) = 'pericardiocentesis / pleuralcentesis' then 'Diagnostic'
               when lower(procedure_type) = 'peritoneal' then 'Diagnostic'
               when lower(procedure_type) = 'ph drug study' then 'Diagnostic'
               when lower(procedure_type) = 'pulmonary hypertension study' then 'Diagnostic'
               when lower(procedure_type) = 'remove arrhythmia monitor implnt' then 'EP'
               when lower(procedure_type) = 'rfa' then 'EP'
               when lower(procedure_type) = 'right heart' then 'Diagnostic'
               when lower(procedure_type) = 'right heart cath' then 'Diagnostic'
               when lower(procedure_type) = 'vascular access' then 'Diagnostic'
               when lower(procedure_type) = 'venogram' then 'Diagnostic'
               when lower(procedure_type) = 'venogram / angio' then 'Diagnostic'
               else 'Undefined'
          end as cath_proc_type
     from
          {{source('cdw', 'cath_study')}} as cath_study
          left join procedure_type_tpvr on cath_study.cath_study_id = procedure_type_tpvr.cath_study_id
),
adverse_events as (
     select
          cath_study.cath_study_id,
          max(case
               when cath_study_adverse_events.cath_study_id is null
                    or lower(cath_study_adverse_events.primary_adverse_event) like 'no complication%'
                    or lower(cath_study_adverse_events.primary_adverse_event) = 'no adverse events'
                    or lower(cath_study_adverse_events.primary_adverse_event) = 'none' then 0 else 1
          end) as adverse_event_ind
     from
           {{source('cdw', 'cath_study')}} as cath_study
          left join {{source('cdw', 'cath_study_adverse_events')}} as cath_study_adverse_events
               on cath_study.cath_study_id = cath_study_adverse_events.cath_study_id
     group by
          cath_study.cath_study_id
),
-- the distinct is here as the source system has duplicated data
impact as (
     select distinct
          lower(registry_impact_cath_data.auxiliary_5) as case_id,
          registry_impact_cath_data.r_hsp_vst_key as hsp_vst_key,
          registry_impact_cath_data.hsp_stat,
          registry_impact_cath_data.pre_proc_stat,
          registry_impact_cath_data.proc_aortic_valvuloplasty_ind,
          registry_impact_cath_data.proc_asd_closure_ind,
          registry_impact_cath_data.proc_coarctation_ind,
          registry_impact_cath_data.proc_diagnostic_cath_ind,
          registry_impact_cath_data.proc_ablation_ind,
          registry_impact_cath_data.proc_cath_ind,
          registry_impact_cath_data.proc_other_ind,
          registry_impact_cath_data.proc_pda_closure_ind,
          registry_impact_cath_data.proc_proximal_pa_stenting_ind,
          registry_impact_cath_data.proc_pulmonary_valvuloplasty_ind,
          registry_impact_cath_data.proc_transcath_pulm_valve_ind,
          registry_hospital_visit.visit_key
     from
          {{source('cdw', 'registry_impact_cath_data')}} as registry_impact_cath_data
          inner join  {{source('cdw', 'registry_hospital_visit')}}  as registry_hospital_visit
               on registry_impact_cath_data.r_hsp_vst_key = registry_hospital_visit.r_hsp_vst_key
     where
          registry_impact_cath_data.auxiliary_5 is not null
          and registry_impact_cath_data.cur_rec_ind = 1
),

sensis_proc_inds as (
     select
          cath_study.cath_study_id as cardiac_study_id,
          case
               when sensis_pedpp.ip5000 = 1 then 1
               when sensis_pedpp.ip5000 = 2 then 0
               else -2
          end as proc_diagnostic_cath_ind,
          case
               when sensis_pedpp.ip5001 = 1 then 1
               when sensis_pedpp.ip5001 = 2 then 0
               else -2
          end as proc_asd_closure_ind,
          case
               when sensis_pedpp.ip5002 = 1 then 1
               when sensis_pedpp.ip5002 = 2 then 0
               else -2
          end as proc_coarctation_ind,
          case
               when sensis_pedpp.ip5003 = 1 then 1
               when sensis_pedpp.ip5003 = 2 then 0
               else -2
          end as proc_aortic_valvuloplasty_ind,
          case
               when sensis_pedpp.ip5004 = 1 then 1
               when sensis_pedpp.ip5004 = 2 then 0
               else -2
          end as proc_pulmonary_valvuloplasty_ind,
          case
               when sensis_pedpp.ip5005 = 1 then 1
               when sensis_pedpp.ip5005 = 2 then 0
               else -2
          end as proc_pda_closure_ind,
          case
               when sensis_pedpp.ip5006 = 1 then 1
               when sensis_pedpp.ip5006 = 2 then 0
               else -2
          end as proc_proximal_pa_stenting_ind,
          case
               when sensis_pedpp.ip5007 = 1 then 1
               when sensis_pedpp.ip5007 = 2 then 0
               else -2
          end as proc_cath_ind,
          case
               when sensis_pedpp.ip5008 = 1 then 1
               when sensis_pedpp.ip5008 = 2 then 0
               else -2
          end as proc_ablation_ind,
          case
               when sensis_pedpp.ip5009 = 1 then 1
               when sensis_pedpp.ip5009 = 2 then 0
               else -2
          end as proc_transcath_pulm_valve_ind,
          case
               when sensis_pedpp.ip5010 = 1 then 1
               when sensis_pedpp.ip5010 = 2 then 0
               else -2
          end as proc_other_ind
     from
          {{source('cdw', 'cath_study')}} as cath_study
     inner join {{source('ccis_ods', 'sensis_pedpp')}}  as sensis_pedpp
          on cath_study.source_system_id = sensis_pedpp.refno
     where
          lower(cath_study.source_system) = 'sensis'
),
sensis_status_inds as (
     select
          cath_study.cath_study_id as cardiac_study_id,
          case
               when sensis_aptver2.ptstat = 2 then 'Outpatient'
               when sensis_aptver2.ptstat = 3 then 'Admit to inpatient floor'
               when sensis_aptver2.ptstat = 4 then 'Admit to inpatient ICU'
               when sensis_aptver2.ptstat = 5 then '23 Hour obs outpatient'
               when sensis_aptver2.ptstat = 6 then 'Return to inpatient floor'
               when sensis_aptver2.ptstat = 7 then 'Return to inpatient ICU'
          else null
          end as hsp_stat,
          case
               when sensis_aptver2.dprstat = 1 then 'Elective'
               when sensis_aptver2.dprstat = 2 then 'Urgent'
               when sensis_aptver2.dprstat = 3 then 'Emergent'
               when sensis_aptver2.dprstat = 4 then 'Salvage'
               else null
          end as pre_proc_stat
     from
               {{source('cdw', 'cath_study')}} as cath_study
          inner join {{source('ccis_ods', 'sensis_aptver2')}} as sensis_aptver2
               on cath_study.source_system_id = sensis_aptver2.refno
     where
          lower(cath_study.source_system) = 'sensis'
),
cath as (
     select
          cath_study.cath_study_id as cardiac_study_id,
          cath_study.patient_key as pat_key,
          stg_patient.mrn,
          stg_patient.patient_name,
          stg_patient.sex,
          stg_patient.dob,
          surgcsn.csn as surgery_csn,
          admcsn.csn as admission_csn,
          to_date(cath_study.study_date_key, 'YYYYMMDD') as study_date,
          cath_study.case_id,
          case when cath_study.procedure_type in ('DFT''s', 'DFTs') --noqa: PRS,L048
             then 'DFTs'
             when cath_study.procedure_type in ('Diagnostic (R&L Hrt Caths)', 'Diagnostic R & L Heart')
             then 'Diagnostic R&L Heart'
             when cath_study.procedure_type in ('Biopsy / Rrt Hrt Cath', 'Biopsy Right Heart')
             then 'Biopsy/Right Heart Cath'
             when cath_study.procedure_type in ('Left Heart', 'Left Heart Cath')
             then 'Left Heart Cath'
             when cath_study.procedure_type in ('Right Heart', 'Right Heart Cath')
             then 'Right Heart Cath'
             when cath_study.procedure_type in ('Pacemaker Procedure', 'Pacemaker Procedures')
             then 'Pacemaker Procedure'
             when cath_study.procedure_type in ('Pericardialcentesis/Pleural',
                                                'Pericardiocentesis / Pleuralcentesis')
             then 'Pericardiocentesis/Pleuralcentesis'
             when cath_study.procedure_type in ('PH Drug Study', 'Pulmonary Hypertension Study')
             then 'Pulmonary Hypertension Drug Study'
          else coalesce(cath_study.procedure_type, 'Not Selected') end as procedure_type,
          case when lower(procedure_location) = 'rm-1' then 'Room 1'
          when lower(procedure_location) = 'rm-2' then 'Room 2'
          when lower(procedure_location) = 'z cicu case' then 'CICU'
          when procedure_location is null then 'Undefined'
          else procedure_location end as procedure_location,
          cath_proc_type,
          case
               when lower(procedure_type) = '_' then 'CATH'
               when lower(procedure_type) = 'aicd' then 'EP'
               when lower(procedure_type) = 'angiography' then 'CATH'
               when lower(procedure_type) = 'annual cath' then 'CATH'
               when lower(procedure_type) = 'arrhythmia monitor implant' then 'EP'
               when lower(procedure_type) = 'biopsy / rrt hrt cath' then 'CATH'
               when lower(procedure_type) = 'biopsy right heart' then 'CATH'
               when lower(procedure_type) like '%dft%s%' then 'EP'
               when lower(procedure_type) = 'diagnostic (r&l hrt caths)' then 'CATH'
               when lower(procedure_type) = 'diagnostic r & l heart' then 'CATH'
               when lower(procedure_type) = 'ep (without rfa)' then 'EP'
               when lower(procedure_type) = 'ep: rfa' then 'EP'
               when lower(procedure_type) = 'eps' then 'EP'
               when lower(procedure_type) = 'fluoro' then 'CATH'
               when lower(procedure_type) = 'interventional' then 'CATH'
               when lower(procedure_type) = 'lead extraction' then 'EP'
               when lower(procedure_type) = 'left heart' then 'CATH'
               when lower(procedure_type) = 'left heart cath' then 'CATH'
               when lower(procedure_type) = 'lymph access' then 'CATH'
               when lower(procedure_type) = 'lymph intervention' then 'CATH'
               when lower(procedure_type) = 'other' then 'CATH'
               when lower(procedure_type) = 'pacemaker procedure' then 'EP'
               when lower(procedure_type) = 'pacemaker procedures' then 'EP'
               when lower(procedure_type) = 'pericardialcentesis/pleural' then 'CATH'
               when lower(procedure_type) = 'pericardiocentesis / pleuralcentesis' then 'CATH'
               when lower(procedure_type) = 'peritoneal' then 'CATH'
               when lower(procedure_type) = 'ph drug study' then 'CATH'
               when lower(procedure_type) = 'pulmonary hypertension study' then 'CATH'
               when lower(procedure_type) = 'remove arrhythmia monitor implnt' then 'EP'
               when lower(procedure_type) = 'rfa' then 'EP'
               when lower(procedure_type) = 'right heart' then 'CATH'
               when lower(procedure_type) = 'right heart cath' then 'CATH'
               when lower(procedure_type) = 'vascular access' then 'CATH'
               when lower(procedure_type) = 'venogram' then 'CATH'
               when lower(procedure_type) = 'venogram / angio' then 'CATH'
               else 'Undefined'
          end as cath_or_ep,
          coalesce(sensis_status_inds.hsp_stat, impact.hsp_stat, 'Undefined') as hsp_stat,
          coalesce(sensis_status_inds.pre_proc_stat, impact.pre_proc_stat, 'Undefined') as pre_proc_stat,
          adverse_event_ind,
          coalesce(
               sensis_proc_inds.proc_aortic_valvuloplasty_ind, impact.proc_aortic_valvuloplasty_ind, -2
          ) as proc_aortic_valvuloplasty_ind,
          coalesce(
               sensis_proc_inds.proc_asd_closure_ind, impact.proc_asd_closure_ind, -2
          ) as proc_asd_closure_ind,
          coalesce(
               sensis_proc_inds.proc_coarctation_ind, impact.proc_coarctation_ind, -2
          ) as proc_coarctation_ind,
          coalesce(
               sensis_proc_inds.proc_diagnostic_cath_ind, impact.proc_diagnostic_cath_ind, -2
          ) as proc_diagnostic_cath_ind,
          coalesce(
               sensis_proc_inds.proc_ablation_ind, impact.proc_ablation_ind, -2
          ) as proc_ablation_ind,
          coalesce(
               sensis_proc_inds.proc_cath_ind, impact.proc_cath_ind, -2
          ) as proc_cath_ind,
          coalesce(
               sensis_proc_inds.proc_other_ind, impact.proc_other_ind, -2
          ) as proc_other_ind,
          coalesce(
               sensis_proc_inds.proc_pda_closure_ind, impact.proc_pda_closure_ind, -2
          ) as proc_pda_closure_ind,
          coalesce(
               sensis_proc_inds.proc_proximal_pa_stenting_ind, impact.proc_proximal_pa_stenting_ind, -2
          ) as proc_proximal_pa_stenting_ind,
          coalesce(
               sensis_proc_inds.proc_pulmonary_valvuloplasty_ind, impact.proc_pulmonary_valvuloplasty_ind, -2
          ) as proc_pulmonary_valvuloplasty_ind,
          coalesce(
               sensis_proc_inds.proc_transcath_pulm_valve_ind, impact.proc_transcath_pulm_valve_ind, -2
          ) as proc_tpvr_ind,
          case when impact.case_id is null then 0 else 1 end as impact_ind,
          coalesce(surgcsn.visit_key, 0) as surgery_visit_key,
          coalesce(admcsn.visit_key, 0) as admission_visit_key,
          coalesce(impact.hsp_vst_key, 0) as hsp_vst_key
     from
          {{ref('cardiac_study')}} as cardiac_study
          inner join {{source('cdw', 'cath_study')}} as cath_study
               on cardiac_study.cardiac_study_id = cath_study.cath_study_id
          inner join procedure_type
               on cath_study.cath_study_id = procedure_type.cath_study_id
          inner join adverse_events
               on cath_study.cath_study_id = adverse_events.cath_study_id
          inner join {{ref('stg_patient')}} as stg_patient
               on stg_patient.pat_key = cath_study.patient_key
          inner join caseids
               on lower(cath_study.case_id) = caseids.unique_case_id
          left join impact
               on impact.case_id = lower(cath_study.case_id)
          left join sensis_status_inds
               on cardiac_study.cardiac_study_id = sensis_status_inds.cardiac_study_id
          left join sensis_proc_inds
               on cardiac_study.cardiac_study_id = sensis_proc_inds.cardiac_study_id
          left join {{source('ccis_ods', 'sensis_study')}} as sensis_study
               on cath_study.source_system_id = sensis_study.refno
          left join {{ref( 'procedure_order_clinical')}} as procedure_order_clinical
               on procedure_order_clinical.procedure_order_id = sensis_study.ordnum
          left join {{source('cdw', 'procedure_order_appointment')}} as procedure_order_appointment
               on procedure_order_appointment.proc_ord_key = procedure_order_clinical.proc_ord_key
          left join {{ref( 'stg_encounter' )}} as surgcsn
               on surgcsn.visit_key = procedure_order_appointment.visit_key
          left join {{ref( 'stg_encounter' )}} as admcsn
               on admcsn.csn = sensis_study.admissid
)

select *
from cath
