select
      surgery.log_id,
      coalesce(baseline_creat.resultvaluenumeric, 0) as creatlst,
      hctbase,
      hctlastprecpb,
      hctfirst,
      lwsthct,
      hctlast,
      hctpostpro,
      hctfirsticu,
      coalesce(lactatefirstor, 0) as lactatefirstor,
      coalesce(lactatelastprecpb, 0) as lactatelastprecpb,
      coalesce(lactatefirstoncpb, 0) as lactatefirstoncpb,
      coalesce(lactatelastoncpb, 0) as lactatelastoncpb,
      coalesce(lactatepostpro, 0) as lactatepostpro,
      coalesce(creatfirst.creatvalue, 0) as creatfirsticu,
      coalesce(creatmax.resultvaluenumeric, 0) as creatmax48,
      coalesce(medvoloncpb, 0) as medvoloncpb,
      coalesce(lactatemax24, 0)as lactatemax24,
      coalesce(actbase, 0) as actbase,
      coalesce(actpostheparin, 0) as actpostheparin,
      coalesce(actmincpb, 0) as actmincpb,
      coalesce(actmaxcpb, 0) as actmaxcpb,
      coalesce(actpostprot, 0) as actpostprot,
      coalesce(cplegiavol, 0) as cplegiavol,
      coalesce(round((.8) * cplegiavol, 0), 0) as cplegiacrysvol
 from
      {{ref('cardiac_perfusion_surgery')}} as perfusion
      inner join {{ref('surgery_encounter')}} as surgery
                on perfusion.log_key = surgery.log_key
      inner join {{ref('cardiac_perfusion_bypass')}} as bypass
                on perfusion.anes_visit_key = bypass.visit_key
      inner join {{ref('surgery_encounter_timestamps')}} as timestamps
                on surgery.log_key = timestamps.or_key
       left join {{ref('stg_hematocrit')}} as hematocrit on hematocrit.log_key = perfusion.log_key
       left join {{ref('stg_lactate')}} as lactate on lactate.log_key = perfusion.log_key
       left join {{ref('stg_creatmax')}} as creatmax on creatmax.log_key = surgery.log_key
       left join {{ref('stg_creatinine')}} as creatfirst
          on creatfirst.log_key = surgery.log_key and creat_postop_order = 1
       left join {{ref('stg_baseline_creat')}} as baseline_creat
          on baseline_creat.log_key = surgery.log_key and baseline_creat_order = 1
       left join {{ref('stg_lactate_max24')}} as lactate_max24
          on lactate_max24.log_key = surgery.log_key
       left join {{ref('stg_act')}} as act on act.log_key = perfusion.log_key
       left join {{ref('stg_cpb_meds')}} as cpb_meds on cpb_meds.log_key = perfusion.log_key
       left join {{ref('stg_cardioplegia')}} as cardioplegia on cardioplegia.log_key = perfusion.log_key
where
     date(surgery.surgery_date) >= '2021-10-01'
