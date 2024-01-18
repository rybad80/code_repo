--CREATE TABLE REGISTRY_STS_ANESTHESIA_NIRS
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
--  AND SERVICE.SRC_ID IN (910,923) --Cardiac Service 
   and ANESSTAFF.DICT_OR_ANES_TYPE_KEY= 241354
	AND VSI_ANES.VSI_KEY > 0
 )
 
 SELECT
    	cast(ISNULL(LOG_ID,'') as integer) CaseNumber
       , 1 AS NIRSCERUSED
       , 2 AS NIRSCERPRE
       , 1 AS NIRSCERINTRA
       , 2 AS NIRSCERPOST
       , 2 AS NIRSSOMUSED
       , NULL NIRSSOMPRE
       , NULL NIRSSOMINTRA
       , NULL NIRSSOMPOST
	   , CAST(NOW() AS DATE) AS LOADDT

 FROM ENCS