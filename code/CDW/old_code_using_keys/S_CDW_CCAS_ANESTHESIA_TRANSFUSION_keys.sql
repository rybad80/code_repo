--CREATE TABLE REGISTRY_STS_ANESTHESIA_TRANSFUSION
--AS

WITH ENCS AS 
(SELECT DISTINCT
       ANESLINK.OR_LOG_KEY
	   ,OR_LOG.LOG_KEY	
	   ,ANES_KEY
	   ,ANES_EVENT_VISIT_KEY
	   ,ANES_VISIT_KEY
	   ,OR_CASE.OR_CASE_KEY
	   ,OR_LOG_VISIT_KEY
	   ,PROC_VISIT_KEY
	   ,ANESLINK.VISIT_KEY
	   ,VSI_ANES.VSI_KEY ANES_VSI_KEY
	   ,VAI_HSP.VSI_KEY HSP_VAI_KEY
	   --,ANESLINK.ANES_START_TM ANES_START_DTTM
       ,PAT.PAT_MRN_ID
       ,OR_LOG.LOG_ID
	   ,TO_CHAR(ANESLINK.ANES_START_TM,'MM/DD/YYYY HH24:MI') ANES_START_DTTM
	   ,ANESLINK.ANES_START_TM ANES_START_TM  
	   ,ANESLINK.ANES_END_TM ANES_END_TM
	   ,COALESCE(TEE,2) TEE
	   ,INDUCT.INDUCT_TM
	   ,ISNULL(TO_CHAR(INDUCT.INDUCT_TM,'MM/DD/YYYY HH24:MI'),TO_CHAR(ANESLINK.ANES_START_TM,'MM/DD/YYYY HH24:MI')) INDUCTION_DTTM
	   ,ANESREADY.ANESREADY_TM
	   ,ANESSTOP.ANESSTOP_TM
	   ,PROCSTOP.PROCSTOP_TM
	   ,TO_CHAR(ANESREADY.ANESREADY_TM,'MM/DD/YYYY HH24:MI') ANES_READY_DTTM
	   ,COALESCE(HANDOFF.HANDOFF_TM,ANESSTOP_TM) HANDOFF_TM
	   ,TO_CHAR(HANDOFF.HANDOFF_TM,'MM/DD/YYYY HH24:MI') HANDOFF_DTTM
	   ,ANES_STOP_DOC.ANES_STOP_DOC_TM
FROM  ANESTHESIA_ENCOUNTER_LINK ANESLINK INNER JOIN OR_CASE ON ANESLINK.OR_CASE_KEY = OR_CASE.OR_CASE_KEY
                                         INNER JOIN OR_LOG ON OR_CASE.LOG_KEY = OR_LOG.LOG_KEY
										 LEFT JOIN VISIT_ADDL_INFO VAI_HSP ON VAI_HSP.VISIT_KEY = ANESLINK.VISIT_KEY
										 LEFT JOIN VISIT_STAY_INFO VSI_ANES ON VSI_ANES.VISIT_KEY = ANESLINK.ANES_VISIT_KEY
										 LEFT JOIN PATIENT PAT ON PAT.PAT_KEY = OR_CASE.PAT_KEY
										 JOIN or_log_anes_staff ANESSTAFF ON ANESSTAFF.LOG_KEY = OR_LOG.LOG_KEY
										 JOIN CDW_DICTIONARY SERVICE ON SERVICE.DICT_KEY = OR_CASE.DICT_OR_SVC_KEY
										 LEFT JOIN (SELECT 1 TEE ,VISIT_KEY FROM VISIT_ED_EVENT EVT JOIN MASTER_EVENT_TYPE EVTTYPE ON EVT.EVENT_TYPE_KEY = EVTTYPE.EVENT_TYPE_KEY WHERE EVTTYPE.EVENT_ID = '1120000051') TEE ON TEE.VISIT_KEY = ANESLINK.ANES_VISIT_KEY
										 LEFT JOIN (SELECT VISIT_KEY, MAX(EVENT_DT) INDUCT_TM FROM VISIT_ED_EVENT EVT JOIN MASTER_EVENT_TYPE EVTTYPE ON EVT.EVENT_TYPE_KEY = EVTTYPE.EVENT_TYPE_KEY WHERE EVTTYPE.EVENT_ID = '1120000008' group by VISIT_KEY) INDUCT ON INDUCT.VISIT_KEY = ANESLINK.ANES_VISIT_KEY
										 LEFT JOIN (SELECT VISIT_KEY, MAX(EVENT_DT) ANESREADY_TM FROM VISIT_ED_EVENT EVT JOIN MASTER_EVENT_TYPE EVTTYPE ON EVT.EVENT_TYPE_KEY = EVTTYPE.EVENT_TYPE_KEY WHERE EVTTYPE.EVENT_ID = '1120000049' group by VISIT_KEY) ANESREADY ON ANESREADY.VISIT_KEY = ANESLINK.ANES_VISIT_KEY
										 LEFT JOIN (SELECT VISIT_KEY, MAX(EVENT_DT) ANESSTOP_TM FROM VISIT_ED_EVENT EVT JOIN MASTER_EVENT_TYPE EVTTYPE ON EVT.EVENT_TYPE_KEY = EVTTYPE.EVENT_TYPE_KEY WHERE EVTTYPE.EVENT_ID = '1120000002' group by VISIT_KEY) ANESSTOP ON ANESSTOP.VISIT_KEY = ANESLINK.ANES_VISIT_KEY  
										 LEFT JOIN (SELECT VISIT_KEY, MAX(EVENT_DT) PROCSTOP_TM FROM VISIT_ED_EVENT EVT JOIN MASTER_EVENT_TYPE EVTTYPE ON EVT.EVENT_TYPE_KEY = EVTTYPE.EVENT_TYPE_KEY WHERE EVTTYPE.EVENT_ID = '100248' group by VISIT_KEY) PROCSTOP ON PROCSTOP.VISIT_KEY = ANESLINK.ANES_VISIT_KEY 
										 LEFT JOIN (SELECT VISIT_KEY, MAX(EVENT_DT) HANDOFF_TM FROM VISIT_ED_EVENT EVT JOIN MASTER_EVENT_TYPE EVTTYPE ON EVT.EVENT_TYPE_KEY = EVTTYPE.EVENT_TYPE_KEY WHERE EVTTYPE.EVENT_ID = '1120000046' group by VISIT_KEY) HANDOFF ON HANDOFF.VISIT_KEY = ANESLINK.ANES_VISIT_KEY
								     	 LEFT JOIN (SELECT VISIT_KEY, MAX(EVENT_DT) ANES_STOP_DOC_TM FROM VISIT_ED_EVENT EVT JOIN MASTER_EVENT_TYPE EVTTYPE ON EVT.EVENT_TYPE_KEY = EVTTYPE.EVENT_TYPE_KEY WHERE EVTTYPE.EVENT_ID = '1120000015' group by VISIT_KEY) ANES_STOP_DOC ON ANES_STOP_DOC.VISIT_KEY = ANESLINK.ANES_VISIT_KEY 
WHERE 1=1
  AND SERVICE.SRC_ID = 910 --Cardiac Service 
 )

 ,TRANSF AS 
(SELECT vsi_key 
		,MIN(1) as TRANSFUSION
        ,ISNULL(MIN(CASE WHEN FS_KEY = 159169 THEN 1 ELSE 2 END),0) AS CELLSAVER
        ,ISNULL(MIN(CASE WHEN FS_KEY = 159498 THEN UNITS END),0) AS PRBC_UNITS
		,ISNULL(MIN(CASE WHEN FS_KEY = 159034 THEN UNITS END),0) AS FFP_UNITS
		,ISNULL(MIN(CASE WHEN FS_KEY = 159432 THEN UNITS END),0) AS FRESHPLAS_UNITS
		,ISNULL(MIN(CASE WHEN FS_KEY = 000000 THEN UNITS END),0) AS SDPHERESIS_UNITS
		,ISNULL(MIN(CASE WHEN FS_KEY = 159265 THEN UNITS END),0) AS PLATE_UNITS
		,ISNULL(MIN(CASE WHEN FS_KEY = 159162 THEN UNITS END),0) AS CYRO_UNITS
		,ISNULL(MIN(CASE WHEN FS_KEY = 172128 THEN UNITS END),0) AS FWB_UNITS
		,ISNULL(MIN(CASE WHEN FS_KEY = 159529 THEN UNITS END),0) AS WB_UNITS 
FROM
	(SELECT vsi.vsi_key, F.FS_KEY, COUNT(DISTINCT MEAS_VAL) UNITS
	from ENCS  
	     JOIN ANESTHESIA_ENCOUNTER_LINK c ON ENCS.OR_LOG_KEY = C.OR_LOG_KEY 
		join CDW.VISIT_STAY_INFO vsi on c.ANES_VISIT_KEY = vsi.VISIT_KEY
		join CDW.FLOWSHEET_RECORD fr on vsi.VSI_KEY = fr.VSI_KEY
		join CDW.FLOWSHEET_MEASURE fm on fr.FS_REC_KEY = fm.FS_REC_KEY
		join CDW.FLOWSHEET f on fm.FS_KEY = f.FS_KEY
	where F.FS_KEY IN (159162,135493,159034,135499,159265,135490,159498,135508,159529,159432,172128,159169)
	group by vsi.vsi_key, F.FS_KEY
	) UNITS
GROUP BY VSI_KEY
)


SELECT 

		cast(ISNULL(LOG_ID,'') as integer) CaseNumber
		,cast(ISNULL(CELLSAVER,2) as integer) CellSavSal
		,cast(ISNULL(Transfusion,2) as integer) Transfusion
		,cast(PRBC_UNITS as integer) BldProdPRBCDur
		,cast(FFP_UNITS as integer) BldProdFFPDur
		,cast(FRESHPLAS_UNITS as integer) BldProdFreshPDur
		,cast(SDPHERESIS_UNITS as integer) BldProdSnglPlatDur
		,cast(PLATE_UNITS as integer) BldProdIndPlatDur
		,cast(CYRO_UNITS as integer) BldProdCryoDur
		,cast(FWB_UNITS as integer) BldProdFreshWBDur
		,cast(WB_UNITS as integer)  BldProdWBDur

FROM

( 
SELECT ENCS.*,
	   TRANSF.*
FROM ENCS	 LEFT JOIN TRANSF ON TRANSF.VSI_KEY = ENCS.ANES_VSI_KEY					 
WHERE 1=1 

) A

ORDER BY 1