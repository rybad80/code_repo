
select distinct
PATIENT.PAT_KEY,

patient.PAT_MRN_ID,
 vis.ENC_ID,

case when FLOWSHEET.FS_ID = '1007' then cdw.flowsheet_measure.MEAS_VAL end as PAT_BSA,
extract (YEAR from (age(dt.FULL_DT,patient.dob))) as PAT_AGE_YEAR,
extract (month from (age(dt.FULL_DT,patient.dob))) as PAT_AGE_MONTH,
extract (DAY from (age(dt.FULL_DT,patient.dob))) as PAT_AGE_DAY,
PATIENT.PAT_KEY as BSA_Join,
vis.VISIT_KEY,
dt.FULL_DT,
--emp.FULL_NM as Perfusionist_Name,
typ.EVENT_NM,
CASE WHEN typ.EVENT_ID in ('112700047') THEN 1 END AS EMERGENCY_BYPASS,
CASE WHEN typ.EVENT_ID in ('112700048') THEN 1 END AS STANDBY_BYPASS,
min(case when typ.EVENT_ID = 112700001 then substr(ed.EVENT_DT,1,100) WHEN typ.EVENT_ID = 112700047 then substr(ed.EVENT_DT,1,100) WHEN typ.EVENT_ID = 112700048 then substr(ed.EVENT_DT,1,100) end )
		over (PARTITION BY vis.VISIT_KEY   --, IP_FLWSHT_MEAS.ENTRY_TIME 
                 ORDER BY cdw.flowsheet_measure.ENTRY_DT)Perfusion_Start_Time,
max(case when typ.EVENT_ID = 112700002 then substr(ed.EVENT_DT,1,100) end)
				over (PARTITION BY vis.VISIT_KEY   --, IP_FLWSHT_MEAS.ENTRY_TIME 
                 ORDER BY cdw.flowsheet_measure.ENTRY_DT)Perfusion_Stop_Time,
min(case when typ.EVENT_ID = 112700007 then substr(ed.EVENT_DT,1,100) end) Cross_Clamp_Start_Time,
max(case when typ.EVENT_ID = 112700008 then substr(ed.EVENT_DT,1,100) end) Cross_Clamp_Stop_Time,
min(case when typ.EVENT_ID = 112700003 then substr(ed.EVENT_DT,1,100)  end)over (PARTITION BY vis.VISIT_KEY   --, IP_FLWSHT_MEAS.ENTRY_TIME 
                 ORDER BY cdw.flowsheet_measure.ENTRY_DT) Circ_Arrest_Start_Time_min,
max(case when typ.EVENT_ID = 112700003 then substr(ed.EVENT_DT,1,100)  end)over (PARTITION BY vis.VISIT_KEY   --, IP_FLWSHT_MEAS.ENTRY_TIME 
                 ORDER BY cdw.flowsheet_measure.ENTRY_DT) Circ_Arrest_Start_Time_max,
min(case when typ.EVENT_ID = 112700004 then substr(ed.EVENT_DT,1,100)  end)over (PARTITION BY vis.VISIT_KEY   --, IP_FLWSHT_MEAS.ENTRY_TIME 
                 ORDER BY cdw.flowsheet_measure.ENTRY_DT) Circ_Arrest_Stop_Time_min,
max(case when typ.EVENT_ID = 112700004 then substr(ed.EVENT_DT,1,100)  end)over (PARTITION BY vis.VISIT_KEY   --, IP_FLWSHT_MEAS.ENTRY_TIME 
                 ORDER BY cdw.flowsheet_measure.ENTRY_DT) Circ_Arrest_Stop_Time_max,
min(case when typ.EVENT_ID = 112700003 then substr(ed.EVENT_DT,1,100)  end)over (PARTITION BY vis.VISIT_KEY   --, IP_FLWSHT_MEAS.ENTRY_TIME 
                 ORDER BY cdw.flowsheet_measure.ENTRY_DT) Circ_Arrest_Start_Time_sample,
max(case when typ.EVENT_ID = 112700004 then substr(ed.EVENT_DT,1,100)  end)over (PARTITION BY vis.VISIT_KEY --, IP_FLWSHT_MEAS.ENTRY_TIME 
                 ORDER BY cdw.flowsheet_measure.ENTRY_DT)Circ_Arrest_Stop_Time_sample


 from cdw.VISIT vis 
left join cdw.VISIT_ED_EVENT ed on ed.VISIT_KEY = vis.VISIT_KEY
left join cdw.MASTER_EVENT_TYPE typ on  ed.EVENT_TYPE_KEY = typ.EVENT_TYPE_KEY
left join cdw.VISIT_STAY_INFO on cdw.VISIT_STAY_INFO.VISIT_KEY = ed.VISIT_KEY
left JOIN cdw.FLOWSHEET_RECORD  ON VISIT_STAY_INFO.vsi_key = cdw.FLOWSHEET_RECORD.vsi_key
left join cdw.VISIT_ADDL_INFO on ed.VISIT_KEY = cdw.VISIT_ADDL_INFO.VISIT_KEY
left JOIN CDW.CDW_DICTIONARY vis_dic ON visit_addl_info.DICT_PAT_CLASS_KEY = vis_dic.dict_key -- Pat Class
left JOIN CDW.CDW_DICTIONARY event_dic ON ed.EVENT_TYPE_KEY = event_dic.dict_key -- Pat Class
left join cdw.EMPLOYEE emp on emp.EMP_KEY = ed.EVENT_INIT_EMP_KEY
JOIN CDW.MASTER_DATE DT ON DT.DT_KEY = vis.CONTACT_DT_KEY 
--JOIN CDW.MASTER_DATE DT ON DT.DT_KEY = CDW.VISIT.CONTACT_DT_KEY 
--left join cdw.COVERAGE on VISit.CVG_KEY = coverage.CVG_KEY
JOIN cdw.flowsheet_measure ON cdw.flowsheet_record.FS_REC_KEY = cdw.flowsheet_measure.FS_REC_KEY --AND visit_stay_info. = flowsheet_measure.occurance
join FLOWSHEET on flowsheet_measure.FS_KEY = FLOWSHEET.FS_KEY
join cdw.patient on ed.PAT_KEY= patient.PAT_KEY

where flowsheet_measure.ENTRY_DT >= to_date('4/16/2018', 'MM/DD/YYYY') 
and  DT.FULL_DT  >= to_date('4/16/2018', 'MM/DD/YYYY')
and typ.EVENT_ID in ('112700001' , '112700002', '112700007', '112700008', '112700003', '112700004', '1120000006','1120000005','112700047','112700048','112700045','112700049') 
--and FLOWSHEET.FS_ID in ('7617','112700001','112700013','112700021','112700009','112700008', '112700016','112700014','11140','1007')
and ed.EVENT_STAT is null
AND PATIENT.TEST_PAT_IND <> 1  -- eliminates all test patients
AND UPPER(cdw.PATIENT.FULL_NM) NOT LIKE '%TEST%'
AND UPPER(cdw.PATIENT.FULL_NM) NOT LIKE '%ZZCHOPTEST%'
AND UPPER(cdw.PATIENT.FULL_NM) NOT LIKE '%TEST MD%'
--AND (typ.EVENT_NM LIKE 'CHOP AN PERFUSION START DATA COLLECTION')
group 
by PATIENT.PAT_KEY,
emp.FULL_NM,
patient.FULL_NM,
patient.PAT_MRN_ID,
patient.DOB,
patient.SEX,
vis.VISIT_KEY,
dt.FULL_DT,
typ.EVENT_NM,
cdw.flowsheet_measure.MEAS_VAL,
cdw.flowsheet_measure.ENTRY_DT,
cdw.FLOWSHEET.DISP_NM,
FLOWSHEET.FS_ID,
ed.EVENT_DT,
typ.EVENT_ID,
 vis.ENC_ID	
