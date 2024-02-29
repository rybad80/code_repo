with all_data as (
--PVS diagnosis
select
     pvs.mrn,
     pvs.patient_name,
     min(date(encounter_date)) as timeline_date,
     'Diagnosis' as timeline_category,
     'PVS Diagnosis' as diagnosis_name,
      min(date(encounter_date))  as diagnosis_date,     
      null as sirolimus_dose,
      null as imatinib_dose,
      null as gleevec_dose,
      null as sildenafil_dose,
      null as tadalafil_dose,
      null as bosentan_dose,
      null as remodulin_dose,
      null as epoprostenol_dose,
      null as iloprost_dose,
      null as losartan_dose,
      null as aspirin_dose,
      null as lovenox_dose,
      null as plavix_dose,
      null as clopidogrel_dose,
      null as apixaban_dose,
      null as coumadin_dose,
      null as cath_procedure_name,
      null as cath_procedure_date,
      null as surgical_procedure_name,
      null as surgical_procedure_date        
from 
     ocqi_PRD..fact_pulmonary_vein_stenosis as pvs
     left join chop_analytics..diagnosis_encounter_all as dx
       on pvs.mrn = dx.mrn
        and lower(dx.diagnosis_name) like '%pulmon%stenosis%'      
group by
   pvs.mrn,
   pvs.patient_name
   
union all

--PVS medications
select
      pvs.mrn,
      pvs.patient_name,
      date(administration_date) as timeline_date,
      'Medications' as timeline_category,
      null as diagnosis_name,   
      null as diagnosis_date,         
      sum(case when lower(medication_name) like 'sirolimus%' or
            lower(generic_medication_name) like 'sirolimus%' then (cast(admin_dose as float)) end) as sirolimus_dose,
      sum(case when lower(generic_medication_name) like 'imatinib%' or
	        lower(medication_name) like 'imatinib%' then (cast(admin_dose as float)) end) as imatinib_dose,
      sum(case when lower(generic_medication_name) like 'gleevec%' or 
	        lower(medication_name) like 'gleevec%' then (cast(admin_dose as float)) end) as gleevec_dose, 
      sum(case when lower(generic_medication_name) like 'sildenafil%' or
	        lower(medication_name) like 'sildenafil%' then (cast(admin_dose as float)) end) as sildenafil_dose,
      sum(case when lower(generic_medication_name) like 'tadalafil%' or
	        lower(medication_name) like 'tadalafil%' then (cast(admin_dose as float)) end) as tadalafil_dose,
      sum(case when lower(generic_medication_name) like 'bosentan%' or
	     lower(medication_name) like 'bosentan%' then (cast(admin_dose as float)) end) as bosentan_dose,
      sum(case when lower(generic_medication_name) like 'remodulin%' or 
	     lower(medication_name) like 'remodulin%' then (cast(admin_dose as float)) end) as remodulin_dose, 
      sum(case when lower(generic_medication_name) like 'epoprostenol%' or
	        lower(medication_name) like 'epoprostenol%' then (cast(admin_dose as float)) end) as epoprostenol_dose,
      sum(case when lower(generic_medication_name) like 'iloprost%' or
	     lower(medication_name) like 'iloprost%' then (cast(admin_dose as float)) end) as iloprost_dose,
      sum(case when lower(generic_medication_name) like 'losartan%' or 
	        lower(medication_name) like 'losartan%' then (cast(admin_dose as float)) end) as losartan_dose, 
      sum(case when lower(generic_medication_name) like 'aspirin%' or 
	        lower(medication_name) like 'aspirin%' then (cast(admin_dose as float)) end) as aspirin_dose, 
      sum(case when lower(generic_medication_name) like 'lovenox%' or
	        lower(medication_name) like 'lovenox%' then (cast(admin_dose as float)) end) as lovenox_dose,
      sum(case when lower(generic_medication_name) like 'plavix%' or
	        lower(medication_name) like 'plavix%' then (cast(admin_dose as float)) end) as plavix_dose,
      sum(case when lower(generic_medication_name) like 'clopidogrel%' or
	        lower(medication_name) like 'clopidogrel%' then (cast(admin_dose as float)) end) as clopidogrel_dose,
      sum(case when lower(generic_medication_name) like 'apixaban%' or
	        lower(medication_name) like 'apixaban%' then (cast(admin_dose as float)) end) as apixaban_dose,
      sum(case when lower(generic_medication_name) like 'coumadin%' or
	        lower(medication_name) like 'coumadin%' then (cast(admin_dose as float)) end) as coumadin_dose,
      null as cath_procedure_name,
      null as cath_procedure_date,
      null as surgical_procedure_name,
      null as surgical_procedure_date 
from
      chop_analytics..medication_order_administration as mar
      inner join ocqi_PRD..fact_pulmonary_vein_stenosis as pvs on mar.mrn = pvs.mrn
where 
       administration_date is not null and 
      (lower(medication_name) like 'sirolimus%' or
      lower(medication_name) like 'imatinib%' or
      lower(medication_name) like 'gleevec%' or 
      lower(medication_name) like 'sildenafil%' or
      lower(medication_name) like 'tadalafil%' or
      lower(medication_name) like 'bosentan%' or
      lower(medication_name) like 'remodulin%' or 
      lower(medication_name) like 'epoprostenol%' or
      lower(medication_name) like 'iloprost%' or
      lower(medication_name) like 'losartan%' or 
      lower(medication_name) like 'aspirin%' or 
      lower(medication_name) like 'lovenox%' or
      lower(medication_name) like 'plavix%' or
      lower(medication_name) like 'clopidogrel%' or
      lower(medication_name) like 'apixaban%' or
      lower(medication_name) like 'coumadin%' or
      lower(generic_medication_name) like 'sirolimus%' or
      lower(generic_medication_name) like 'imatinib%' or
      lower(generic_medication_name) like 'gleevec%' or 
      lower(generic_medication_name) like 'sildenafil%' or
      lower(generic_medication_name) like 'tadalafil%' or
      lower(generic_medication_name) like 'bosentan%' or
      lower(generic_medication_name) like 'remodulin%' or 
      lower(generic_medication_name) like 'epoprostenol%' or
      lower(generic_medication_name) like 'iloprost%' or
      lower(generic_medication_name) like 'losartan%' or 
      lower(generic_medication_name) like 'aspirin%' or 
      lower(generic_medication_name) like 'lovenox%' or
      lower(generic_medication_name) like 'plavix%' or
      lower(generic_medication_name) like 'clopidogrel%' or
      lower(generic_medication_name) like 'apixaban%' or
      lower(generic_medication_name) like 'coumadin%')
group by 
      pvs.mrn,
      pvs.patient_name,
     date(administration_date)
      
 union all

--cath procedures

select
      pvs.mrn,
      pvs.patient_name,
     date(study_date) as timeline_date,
     'Cath Procedures' as timeline_category,
      null as diagnosis_name, 
      null as diagnosis_date,           
      null as sirolimus_dose,
      null as imatinib_dose,
      null as gleevec_dose,
      null as sildenafil_dose,
      null as tadalafil_dose,
      null as bosentan_dose,
      null as remodulin_dose,
      null as epoprostenol_dose,
      null as iloprost_dose,
      null as losartan_dose,
      null as aspirin_dose,
      null as lovenox_dose,
      null as plavix_dose,
      null as clopidogrel_dose,
      null as apixaban_dose,
      null as coumadin_dose,
     group_concat(procedure_performed_name,'; ')  as cath_procedure_name,
     date(study_date) as cath_procedure_date,
     null as surgical_procedure_name,
     null as surgical_procedure_date  --select *
from
    chop_analytics..cardiac_cath as cath
    inner join cath_study_procedure_performed as procs on cath.cardiac_study_id = procs.cath_study_id
    inner join ocqi_PRD..fact_pulmonary_vein_stenosis as pvs on  pvs.mrn = cath.mrn
group by 
      pvs.mrn,
      pvs.patient_name,
    date(study_date)
    
union all 

--surgical procedures
select
      pvs.mrn,
      pvs.patient_name,
     date(surg_date) as timeline_date,
     'Surgeries' as timeline_category,
      null as diagnosis_name,     
      null as diagnosis_date,       
      null as sirolimus_dose,
      null as imatinib_dose,
      null as gleevec_dose,
      null as sildenafil_dose,
      null as tadalafil_dose,
      null as bosentan_dose,
      null as remodulin_dose,
      null as epoprostenol_dose,
      null as iloprost_dose,
      null as losartan_dose,
      null as aspirin_dose,
      null as lovenox_dose,
      null as plavix_dose,
      null as clopidogrel_dose,
      null as apixaban_dose,
      null as coumadin_dose,
     null as cath_procedure_name,
     null as cath_procedure_date,
     primary_proc_name as surgical_procedure_name,
     date(surg_date) as surgical_procedure_date      
from
    chop_analytics..cardiac_surgery as surg
    inner join ocqi_PRD..fact_pulmonary_vein_stenosis as pvs on  pvs.mrn = surg.mrn
)


select 
      all_data.*
from
     all_data