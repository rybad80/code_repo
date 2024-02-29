with
cb_cases as
(
select 
    p.procedure_id
     ,date (p.procedure_date) as cath_date
     ,medical_record_num mrn
from 
    cdw_ods..clinibase_procedures p
	left join cdw_ods..clinibase_patients pat     on p.occurance_id = pat.occurance_id
	left join cdw_ods..clinibase_proc_type_diag d on d.id = p.type
	left join cdwprd..patient v                       on medical_record_num = pat_mrn_id
	inner join ocqi_prd..fact_pulmonary_vein_stenosis as pvs on  pvs.mrn = cast(medical_record_num as varchar(10))
) 

, cb_pressures1 as
(
select 
      procedure_id,
      condition_seq,
      condition,
      site,
      systolic,
      diastolic,
      mean      
from 
     cdw_ods..clinibase_hemodynamic
where
     condition_seq = 1 
)

, cb_pressures2 as
(
select 
      procedure_id,
      pdr_site site,
      pdr_condnum condition_seq,
      pdc_name condition,
      pdr_time,
      pdr_press_sv as systolic,
      pdr_press_da as diastolic,
      pdr_press_mean as mean,
      row_number() over (partition by procedure_id,site,pdr_condnum order by pdr_time) as pressure_seq
from 
     cdw_ods..clinibase_pdresult
     inner join cdw_ods..clinibase_pdcondition on clinibase_pdresult.pdr_pdc = clinibase_pdcondition.pdc_id  
where
     condition_seq = 1
)

, cb_pressures as

(select
      cb_cases.procedure_id
      , condition_seq
      , condition
      , site
      , systolic as systolic
      , diastolic as diastolic
      , mean as mean

 from 
      (--select * from cb_pressures1
      --union all
      select * from cb_pressures2 
      where pressure_seq = 1) pressures_union INNER JOIN cb_cases ON pressures_union.procedure_id = cb_cases.procedure_id
where
    systolic is not null and diastolic is not null and mean is not null   
) 

, cb_pvri as
(
select 
     cb_cases.procedure_id,
     condition,
     condnum,
     pbf as qp,
     sbf as qs,
     qpqs,
     pvri,
     svri--select *
from 
     cdw_ods..clinibase_pd_calculation
     inner join cb_cases on clinibase_pd_calculation.procedure_id = cb_cases.procedure_id
where
     condnum = 1     	 	
),

cb_pressures_calcs as (

select 
    cb_cases.procedure_id,
    cb_cases.procedure_id||'Cli' as cath_study_id,
    cb_cases.cath_date,
    cb_cases.mrn,
    replace(upper(cb_pressures.condition),'_',' ') as condition,
    cb_pressures.site,
    cb_pressures.systolic,
    cb_pressures.diastolic,
    cb_pressures.mean,
    cb_pvri.pvri  
from 
    cb_cases
    left join cb_pressures on cb_pressures.procedure_id = cb_cases.procedure_id
    left join cb_pvri on cb_pvri.procedure_id = cb_cases.procedure_id and cb_pressures.condition_seq = cb_pvri.condnum
),

sensis_cases as
(
select 
       sensis_study.REFNO
       ,sensis_study.STUDATE cath_date
	   ,pvs.mrn 
	   --select *
from 
    cdw_ods..sensis_study
	inner join cdw_ods..sensis_patient on sensis_study.PATNO = sensis_patient.patno
	inner join ocqi_prd..fact_pulmonary_vein_stenosis as pvs on  pvs.mrn = cast(sensis_patient.patid as varchar(10))     
)

, all_pressures as 
(
select 
      refno,
      site,
      seqnr,
      press1,
      press2,
      press3,
      scondnr,
      condnr,
      atime --select *
from
    cdw_ods..sensis_lp1 
	
union all

select 
      refno,
      site1,
      seqnr,
      fpress1,
      fpress2,
      fpress3,
      scondnr,
      condnr,
      atime --select *
from
    cdw_ods..sensis_lp2

union all

select 
      refno,
      site2,
      seqnr,
      spress1,
      spress2,
      spress3,
      scondnr,
      condnr,
      atime
from
    cdw_ods..sensis_lp2
) 

, conditions as
(
select 
     all_pressures.refno,
     condname,
     all_pressures.condnr,
     sitedesc,
     press1 as systolic,
     press2 as diastolic,
     press3 as mean,
     row_number() over (partition by all_pressures.refno, condname, all_pressures.condnr, sitedesc order by all_pressures.atime) as pressure_seq
      
from 
     all_pressures
     inner join cdw_ods..sensis_cn      on all_pressures.refno = sensis_cn.refno and all_pressures.condnr = sensis_cn.condnr
     inner join cdw_ods..sensis_sitelist on sensis_sitelist.code =  all_pressures.site  	
     inner join sensis_cases                 on sensis_cases.refno = all_pressures.refno
WHERE
     all_pressures.condnr = 1

)

, sensis_shunts as 
(
select
      refno,
	  condnr,
	  pfi as qp_index,
	  sfi as qs_index
from 
     cdw_ods..sensis_sh
)

, sensis_pvri as
( 
/*select 
      refno,
      round(parix,1) pvri --select * 
    from
        cdw_ods..sensis_cp where refno in ('100399','98978')
where 
     pvri is not null        
        
union
*/
select 
      refno,
      round(pvr,1) as pvri --select *
 from
     cdw_ods..sensis_hemodyn
where
     pvr is not null       
),

sensis_pressures_calcs as
(
select 
      sensis_cases.refno,
      sensis_cases.refno||'Sen' as cath_study_id,
      sensis_cases.cath_date,
      sensis_cases.mrn,
      conditions.condname as condition,
      conditions.sitedesc as site,
      conditions.systolic,
      conditions.diastolic,
      conditions.mean,
      sensis_pvri.pvri
  from
      sensis_cases
      left join conditions 
       on sensis_cases.refno = conditions.refno
      left join sensis_shunts 
       on sensis_cases.refno = sensis_shunts.refno 
         and conditions.condnr = sensis_shunts.condnr
      left join sensis_pvri
       on sensis_cases.refno = sensis_pvri.refno
order by
    sensis_cases.refno,
    condname,
    sitedesc)
,   
all_cath_hemodyn as (
    select * from cb_pressures_calcs
    union all
    select * from sensis_pressures_calcs
)    
,
highest_pa as (    
   select
         cath_study_id,
         cath_date,
         mrn,
         lower(condition) as condition,
         site as site_raw,
         case when lower(site) like 'pulmonary%artery%left%' 
                or lower(site) like '%lpa%' then 'LPA'
              when lower(site) like 'pulmonary%artery%right%' 
                or lower(site) like '%rpa%' then 'RPA'
              when lower(site) like 'pulmonary%artery%mid%' 
                or lower(site) like 'pulmonary%artery%main%' 
                or lower(site) like '%mpa%' then 'MPA'
         end as site,
         round(systolic,0) as systolic,
         round(diastolic,0) as diastolic,
         round(mean,0) as mean,
         round(pvri,2) as pvri,
         row_number() over (partition by cath_study_id order by mean desc) as mean_pa_seq
   from 
       all_cath_hemodyn 
   where 
       lower(site) like ('lpa%')
       or lower(site) like ('rpa%')
       or lower(site) like ('mpa%')
       or lower(site) like ('pulmonary artery%')
)

,
rv_ao as (    
   select
         cath_study_id,
         cath_date,
         mrn,
         max(case when site in ('Rv','Right Ventricle') then systolic end)
         /
         max(case when site in ('AoDt', 'Aorta Ascending') then systolic end)
         as rv_ao
   from 
       all_cath_hemodyn 
   where 
       lower(condition) = 'rest'
       and (site in ('AoDt', 'Aorta Ascending')
         or site in ('Rv','Right Ventricle')
         )
   group by
         cath_study_id,
         cath_date,
         mrn         
) 
 
select
     pvs.mrn,
     pvs.patient_name,     
     cath.cardiac_study_id,
     date(study_date) as timeline_date,
     'Cath Details' as timeline_category,
     highest_pa.condition,
     highest_pa.site,
     highest_pa.systolic,
     highest_pa.diastolic,
     highest_pa.mean,
     highest_pa.pvri,
     round(rv_ao,2) as rv_ao
from
    chop_analytics..cardiac_cath as cath
    inner join ocqi_prd..fact_pulmonary_vein_stenosis as pvs 
       on  pvs.mrn = cath.mrn
    inner join highest_pa 
       on highest_pa.cath_study_id = cath.cardiac_study_id
         and mean_pa_seq = 1
    inner join rv_ao
       on rv_ao.cath_study_id = cath.cardiac_study_id

