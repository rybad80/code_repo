with preproc_hemo_all as (
select
         cath_study.pat_key,
         studate,
         result_date,
         cath_study.refno,
         case when result_component_id in (5, 34, 123050003, 123130024, 123030093)
             then result_value_numeric end as hemoglobin,
         extract(epoch from sh_acc_tm - result_date) as anesrecdatediff,
         row_number() over (partition by cath_study.refno
                      order by extract(epoch from sh_acc_tm - result_date)) as rec_order
--select *
 from     {{ref('stg_impact_cathstudy')}} as cath_study
          inner join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
             on procedure_order_result_clinical.pat_key = cath_study.pat_key
             and cast(result_date as date) <=  date(studate)
 where
      result_component_id in (5, 34, 123050003, 123130024, 123030093)
      and result_value_numeric != 9999999
      and extract(epoch from sh_acc_tm - result_date) between 0 and 2592000

),

preproc_hemo as (
select
      refno,
      hemoglobin
from
     preproc_hemo_all
where
    rec_order = 1
),

preproc_labs_all as (
select
       cath_study.refno,
       ph,
       pco2,
       cast(hgb as numeric (5, 2)) as hgb,
       pat_mrn_id,
       row_number() over (partition by cath_study.refno
                          order by extract(epoch from sh_acc_tm - autotime) desc, seqno) as rec_order
  from
       {{ref('stg_impact_cathstudy')}} as cath_study
       inner join {{source('ccis_ods', 'sensis_bg')}} as bg on cath_study.refno = bg.refno
  where hgb is not null
),

preproc_labs as (
select
      refno,
      ph,
      pco2,
      hgb,
      pat_mrn_id
from
    preproc_labs_all
where
    rec_order = 1
),

preproc_creat_all as (
 select
         cath_study.pat_key,
         studate,
         sh_acc_tm,
         prend,
         result_date,
         cath_study.refno,
         result_value_numeric as creatinine,
         extract(epoch from sh_acc_tm - result_date) as anesrecdatediff,
         row_number() over (partition by refno
                            order by extract(epoch from sh_acc_tm - result_date), proc_ord_key) as rec_order

 from     {{ref('stg_impact_cathstudy')}} as cath_study
          inner join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
             on procedure_order_result_clinical.pat_key = cath_study.pat_key
             and cast(result_date as date) <=  date(studate)

 where
       result_component_id in (84, 123030288, 123030297, 123030299)
       and result_value_numeric != 9999999
       and (extract(epoch from sh_acc_tm - result_date) between 0 and 2592000
        or result_date between sh_acc_tm and prend)
),

preproc_creat as (
select
      refno,
      creatinine
from
    preproc_creat_all
where
    rec_order = 1
),

preproc_o2sat_all as (
select
         cath_study.pat_key,
         studate,
         sh_acc_tm,
         cath_study.refno,
         recorded_date as rec_dt,
         spo2 as o2sat,
         extract(epoch from sh_acc_tm - recorded_date) as diff,
         row_number() over (partition by refno
                            order by extract(epoch from sh_acc_tm - recorded_date)) as rec_order
from
    {{ref('stg_impact_cathstudy')}} as cath_study
    inner join {{ref('flowsheet_vitals')}} as flowsheet_vitals
       on cath_study.pat_key = flowsheet_vitals.pat_key
       and date(studate) >= date(recorded_date)
where
     spo2 is not null
     and extract(epoch from sh_acc_tm - recorded_date) between 0 and 2592000
),

preproc_o2sat as (
select
      refno,
      o2sat
from
    preproc_o2sat_all
where
    rec_order = 1
),

preproc_meds as (
select cath.refno,
       min(case when prmeds.ip4045 in (1, 2, 3, 4, 5, 6, 7, 8, 9) then '1' else '2' end) as preprocmed,
       min(case when prmeds.ip4045 = 1  then '1' else '2' end) as preprocantiarr,
       min(case when prmeds.ip4045 = 2  then '1' else '2' end) as preprocanticoag,
       min(case when prmeds.ip4045 = 3  then '1' else '2' end) as preprocantihyp,
       min(case when prmeds.ip4045 = 4  then '1' else '2' end) as preprocantiplatelet,
       min(case when prmeds.ip4045 = 5  then '1' else '2' end) as preprocbb,
       min(case when prmeds.ip4045 = 6  then '1' else '2' end) as preprocdiuretic,
       min(case when prmeds.ip4045 = 7  then '1' else '2' end) as preprocprosta,
       min(case when prmeds.ip4045 = 9  then '1' else '2' end) as preprocvaso
from
    {{ref('stg_impact_cathstudy')}} as cath
    inner join {{source('ccis_ods', 'sensis_prmeds')}} as prmeds
      on cath.refno = prmeds.refno
    inner join {{source('ccis_ods', 'sensis_dicip4045')}} as dicip4045
      on dicip4045.code = prmeds.ip4045
group by cath.refno
),

preproc_cond as (
select cath.pat_key,
        cath.studate,
        cath.refno,
        ip3245 as svdefect,
        ip4030 as nec,
        ip4035 as sepsis,
        ip4040 as preg
from
     {{ref('stg_impact_cathstudy')}} as cath
     inner join {{source('ccis_ods', 'sensis_prepcon')}} as prepcon
        on cath.refno = prepcon.refno
),

preproc_rhy as (
select cath.refno,
       min(case when precg.ip4060 = 1  then '1' else '2' end) as preprocsinus,
       min(case when precg.ip4060 = 2  then '1' else '2' end) as preprocaet,
       min(case when precg.ip4060 = 3  then '1' else '2' end) as preprocsvt,
       min(case when precg.ip4060 = 4  then '1' else '2' end) as preprocafib,
       min(case when precg.ip4060 = 5  then '1' else '2' end) as preprocjunct,
       min(case when precg.ip4060 = 6  then '1' else '2' end) as preprocidio,
       min(case when precg.ip4060 = 7  then '1' else '2' end) as preprocavb2,
       min(case when precg.ip4060 = 8  then '1' else '2' end) as preprocavb3,
       min(case when precg.ip4060 = 9  then '1' else '2' end) as preprocpaced
from
     {{ref('stg_impact_cathstudy')}} as cath
      inner join {{source('ccis_ods', 'sensis_precg')}} as precg
         on cath.refno = precg.refno
group by cath.refno
)

select
      cath.refno,
      case when coalesce(preproc_labs.hgb, hemo.hemoglobin) > 99 then null
            else coalesce(preproc_labs.hgb, hemo.hemoglobin) end as preprochgb,
       cast(creat.creatinine as numeric(4, 2)) as preproccreat,
       cast(o2sat.o2sat as integer) as preproco2,
       svdefect,
       nec,
       sepsis,
       preg,
       case when preprocmed = 1 then preprocantiarr else null end as preprocantiarr,
       case when preprocmed = 1 then preprocanticoag else null end as preprocanticoag,
       case when preprocmed = 1 then preprocantihyp else null end as preprocantihyp,
       case when preprocmed = 1 then preprocantiplatelet else null end as preprocantiplatelet,
       case when preprocmed = 1 then preprocbb else null end as preprocbb,
       case when preprocmed = 1 then preprocdiuretic else null end as preprocdiuretic,
       case when preprocmed = 1 then preprocprosta else null end as preprocprosta,
       case when preprocmed = 1 then preprocvaso else null end as preprocvaso,
       coalesce(preprocsinus, '2') as preprocsinus,
       coalesce(preprocaet, '2') as preprocaet,
       coalesce(preprocsvt, '2') as preprocsvt,
       coalesce(preprocafib, '2') as preprocafib,
       coalesce(preprocjunct, '2') as preprocjunct,
       coalesce(preprocidio, '2') as preprocidio,
       coalesce(preprocavb2, '2') as preprocavb2,
       coalesce(preprocavb3, '2') as preprocavb3,
       coalesce(preprocpaced, '2') as preprocpaced,
       case when coalesce(preproc_labs.hgb, hemo.hemoglobin) is null then 1 else 2 end as preprochgbnd,
       case when creat.creatinine is null then 1 else 2 end as preproccreatnd,
       coalesce(preprocmed, '2') as preprocmed
from
    {{ref('stg_impact_cathstudy')}} as cath
     left join preproc_labs
        on preproc_labs.refno = cath.refno
     left join preproc_hemo as hemo
        on cath.refno = hemo.refno
     left join preproc_creat as creat
        on cath.refno = creat.refno
     left join preproc_o2sat as o2sat
        on cath.refno = o2sat.refno
     left join preproc_meds as premeds on cath.refno = premeds.refno
     left join preproc_cond as precond on cath.refno = precond.refno
     left join preproc_rhy as prerhy on cath.refno = prerhy.refno
