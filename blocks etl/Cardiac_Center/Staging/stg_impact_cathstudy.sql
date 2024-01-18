with sheath as (
select
       refno,
       min(shtime) as sh_acc_tm
   from
       {{source('ccis_ods', 'sensis_asr')}}
group by
       refno
)

select study.refno,
        study.admissid,
        cast(v1.enc_id as int) as surg_enc_id,
        study.accessno,
        ct.caseid as cath_case_id,
        study.ordnum,
        studate,
        coalesce(sh_acc_tm, ct.timout) as sh_acc_tm,
        poct.prend,
        patient_match.pat_key,
        pat_mrn_id,
        case when pd.height < 500 then pd.height else null end as height,
        pd.weight
 from
     {{source('ccis_ods', 'sensis_study')}}  as study
     inner join {{source('cdw', 'patient_match')}} as patient_match
        on study.refno = patient_match.src_sys_id
           and src_sys_nm = 'SENSIS'
     inner join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = patient_match.pat_key
     inner join {{source('ccis_ods', 'sensis_ct')}} as ct
        on study.refno = ct.refno
     inner join {{source('cdw', 'procedure_order')}} as po
        on po.proc_ord_id = study.ordnum
     inner join {{source('cdw', 'or_case_order')}} as oco
        on oco.ord_key = po.proc_ord_key
     inner join {{source('cdw', 'or_log')}} as or_log
        on or_log.case_key = oco.or_case_key
     inner join {{source('cdw', 'visit')}} as v1
        on v1.visit_key = or_log.visit_key
     left join {{source('ccis_ods', 'sensis_pd')}} as pd
        on study.refno = pd.refno
     left join sheath
        on study.refno = sheath.refno
     left join {{source('ccis_ods', 'sensis_poct')}} as poct
        on study.refno = poct.refno
where
     poct.prtype != 10 --fluoro
     and date(studate) >= '2019-07-01'
