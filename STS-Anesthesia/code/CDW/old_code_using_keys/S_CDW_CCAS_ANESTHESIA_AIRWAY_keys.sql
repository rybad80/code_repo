--CREATE TABLE REGISTRY_STS_ANESTHESIA_AIRWAY
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
 
 , AIRWAY AS
(
SELECT  FR.VSI_KEY
       ,PLACE_DT AIRWAY_PLACE_DT
       ,REMOVE_DT AIRWAY_REM_DT
	   ,MIN(CASE WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) = '' THEN 472
	             WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) LIKE '%BAG%' THEN 473 
				 WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) LIKE '%CANNULAE%' THEN 474 
				 WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) = 'LARYNGEAL MASK AIRWAY' THEN 475 
				 WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) LIKE '%ENDOTRACHEAL%' THEN 476 
				 WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) LIKE '%TRACHEOSTOMY%' THEN 477 
				 WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) LIKE '%SIMPLE MASK%' THEN 2808 
	         END) AS AIRWAYTYPE
	   ,MIN(CASE WHEN F.FS_ID=1127170174 AND MEAS_VAL = '1' THEN 465
	             WHEN F.FS_ID=1127170174 AND MEAS_VAL = '1.5' THEN 466
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '2' THEN 467
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '2.5' THEN 468
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '3' THEN 469
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '4' THEN 470
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '5' THEN 471 END) AS AIRWAYSIZELMA
	   ,MIN(CASE WHEN F.FS_ID=1127170174 AND MEAS_VAL = '2.5' THEN 453 
	             WHEN F.FS_ID=1127170174 AND MEAS_VAL = '3' THEN 454
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '3.5' THEN 455
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '4' THEN 456
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '4.5' THEN 457
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '5' THEN 458
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '5.5' THEN 459
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '6' THEN 460
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '6.5' THEN 461
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '7' THEN 462
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '7.5' THEN 463
				 WHEN F.FS_ID=1127170174 AND MEAS_VAL = '8' THEN 464
				 ELSE 2807 END) AS ENDOTRACH_SZ
	   ,MIN(CASE WHEN F.FS_ID=40010869 AND UPPER(MEAS_VAL) = 'YES' THEN 1
	             WHEN F.FS_ID=40010869 AND UPPER(MEAS_VAL) = 'NO' THEN 2 
				 ELSE '1119' END) AS CUFFED
	   ,MIN(CASE WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) LIKE '%ORAL%' THEN 450
	             WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) LIKE '%NASAL%' THEN 451
	   			 WHEN F.FS_ID=40010841 AND UPPER(MEAS_VAL) LIKE '%TRACHEOSTOMY%' THEN 452
		    END) AS AIRWAYSITE
	   ,ENDOBRONCH_ISO.ENDOBRONCH_ISO
	   ,NULL  AS ENDOBRONCH_ISO_METH --N/A
	   ,2 AS ICUTYPE_USED --NOT USED   
	  -- ,F.FS_KEY
	  -- ,F.DISP_NM
	  -- ,MEAS_VAL SELECT DISTINCT F.FS_KEY, F.DISP_NM
from PATIENT_LDA lda
	join VISIT_STAY_INFO_ROWS vsr on lda.PAT_LDA_KEY = vsr.PAT_LDA_KEY
	join FLOWSHEET_RECORD fr on vsr.VSI_KEY = fr.VSI_KEY
	join FLOWSHEET_MEASURE fm on fr.FS_REC_KEY = fm.FS_REC_KEY
	join FLOWSHEET f on fm.FS_KEY = f.FS_KEY
	join FLOWSHEET_TEMPLATE_GROUP ftg on vsr.FS_KEY = ftg.FS_KEY
	join FLOWSHEET_TEMPLATE ft on ftg.FS_TEMP_KEY = ft.FS_TEMP_KEY
	join FLOWSHEET_LDA_GROUP lda_group on vsr.FS_KEY = lda_group.FS_KEY
	join CDW_DICTIONARY dict_type on lda_group.DICT_LDA_TYPE_KEY = dict_type.DICT_KEY
    JOIN ENCS ON ENCS.ANES_VSI_KEY = FR.VSI_KEY
	LEFT JOIN (SELECT VISIT_KEY, 1 AS ENDOBRONCH_ISO FROM VISIT_ED_EVENT EVENT WHERE EVENT_TYPE_KEY = 37355) ENDOBRONCH_ISO ON ENDOBRONCH_ISO.VISIT_KEY = ENCS.ANES_VISIT_KEY

where 1=1
  AND vsr.SEQ_NUM = fm.OCCURANCE /*Necessary to match the specific instance of the flowsheet recording to the correct measure value*/
  AND dict_type.SRC_ID in (209,5)
    and airway_PLACE_DT <> airway_REM_DT
group BY FR.VSI_KEY, PLACE_DT, REMOVE_DT, ENDOBRONCH_ISO
)


 SELECT 
    cast(ISNULL(LOG_ID,'') as integer)  CaseNumber
	,cast(ISNULL(AIRWAY_SITU,2) as integer) AIRWAYINSITU
	,cast(AIRWAYTYPE as integer) AIRWAYTYPE
	,cast(AIRWAYSIZELMA as integer) AIRWAYSIZELMA
	,cast(ENDOTRACH_SZ as integer) AIRWAYSIZEINTUB
	,cast(CUFFED as integer) CUFFED
	,cast(AIRWAYSITE as integer) AIRWAYSITE
	,cast(ISNULL(ENDOBRONCH_ISO,2) as integer) EndobroncIso
	,cast(ENDOBRONCH_ISO_METH as integer) EndobroncIsoMeth
	,ANESREADY_TM EndOfInductDT
 FROM
 (	   SELECT
             ENCS.*,
		     AIRWAY.*,
	         AIRWAY_SITU.AIRWAY_SITU
	   FROM ENCS
		     LEFT JOIN (SELECT AIRWAY.*, 1 AIRWAY_SITU, ROW_NUMBER() OVER (PARTITION BY AIRWAY.VSI_KEY ORDER BY AIRWAY_PLACE_DT DESC)  ROWNO
		                 FROM AIRWAY JOIN ENCS ON ENCS.ANES_VSI_KEY = AIRWAY.VSI_KEY 
				     	 WHERE AIRWAY.AIRWAY_PLACE_DT < ANES_START_DTTM and AIRWAY.AIRWAY_REM_DT > ANES_START_DTTM) AIRWAY_SITU ON AIRWAY_SITU.VSI_KEY = ENCS.ANES_VSI_KEY and AIRWAY_SITU.ROWNO = 1
		     LEFT JOIN (SELECT AIRWAY.*, ROW_NUMBER() OVER (PARTITION BY AIRWAY.VSI_KEY ORDER BY AIRWAY_PLACE_DT) ROWNO
		                 FROM AIRWAY JOIN ENCS ON ENCS.ANES_VSI_KEY = AIRWAY.VSI_KEY  
					    WHERE AIRWAY.AIRWAY_PLACE_DT >= ANES_START_DTTM ) AIRWAY ON AIRWAY.VSI_KEY = ENCS.ANES_VSI_KEY AND AIRWAY.ROWNO =1
 ) ENC
 
 ORDER BY 1