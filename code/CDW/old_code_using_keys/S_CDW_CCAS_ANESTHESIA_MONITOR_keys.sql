	--CREATE TABLE CDWDEV..REGISTRY_STS_ANESTHESIA_MONITOR
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
 ,
ARTERIAL_LINE AS
(SELECT DISTINCT 
		FR.VSI_KEY,
        PLACE_DT ART_LINE_PLACE_DT,
		REMOVE_DT ART_LINE_REMOVE_DT,
		'1' AS ARTERIAL_LINE,
        min(CASE WHEN UPPER(LDA_SITE) = 'RADIAL' THEN '1' ELSE '2' END) AS ART_RADIAL,
		min(CASE WHEN UPPER(LDA_SITE) = 'BRACHIAL' THEN '1' ELSE '2' END) AS ART_BRACHIAL,
		min(CASE WHEN UPPER(LDA_SITE) = 'AXILLARY' THEN '1' ELSE '2' END) AS ART_AXILLARY,
		min(CASE WHEN UPPER(LDA_SITE) = 'FEMORAL' THEN '1' ELSE '2' END) AS ART_FEMORAL,
		min(CASE WHEN UPPER(LDA_SITE) = 'ULNAR' THEN '1' ELSE '2' END) AS ART_ULNAR,
		min(CASE WHEN UPPER(LDA_SITE) = 'PEDAL' THEN '1' ELSE '2' END) AS ART_DORSALIS_PEDIS,
		min(CASE WHEN UPPER(LDA_SITE) = 'POSTERIOR TIBIAL' THEN '1' ELSE '2' END) AS ART_POSTERIOR_TIBIAL,
		min(CASE WHEN UPPER(LDA_SITE) = 'UMBILICAL' THEN '1' ELSE '2' END) AS ART_UMBILICAL,
		min(CASE WHEN UPPER(LDA_DISP) LIKE '%CUTDOWN%' THEN '1' ELSE '2' END) AS CUTDOWN,
	    min(CASE WHEN UPPER(LDA_DISP) LIKE '%CUTDOWN%' and UPPER(LDA_SITE) = 'RADIAL' THEN '1' ELSE '2' END) AS CUTDOWN_RADIAL,
	    min(CASE WHEN UPPER(LDA_DISP) LIKE '%CUTDOWN%' and (UPPER(LDA_SITE) = 'ULNAR'  or UPPER(MEAS_CMT) = 'MEDIAL') THEN '1' ELSE '2' END) AS CUTDOWN_ULNAR,
		min(CASE WHEN UPPER(LDA_DISP) LIKE '%CUTDOWN%' and UPPER(LDA_SITE) = 'FEMORAL' THEN '1' ELSE '2' END) AS CUTDOWN_FEMORAL,
		min(CASE WHEN UPPER(LDA_DISP) LIKE '%CUTDOWN%' AND UPPER(LDA_SITE) <> 'RADIAL' AND UPPER(LDA_SITE) <> 'ULNAR' AND UPPER(LDA_SITE) <> 'FEMORAL' AND UPPER(MEAS_CMT) <> 'MEDIAL' THEN '1' ELSE '2' END) AS CUTDOWN_OTHER,	
		min(CASE WHEN UPPER(MEAS_VAL) = 'ULTRASOUND' THEN '1' ELSE '2' END) AS ART_ULTRA,
		min(CASE WHEN UPPER(LDA_DESC) LIKE '%INTRACARDIAC%' THEN '1' ELSE '2' END) AS ART_SURG_PLACE
from CDW.PATIENT_LDA lda
	join CDW.VISIT_STAY_INFO_ROWS vsr on lda.PAT_LDA_KEY = vsr.PAT_LDA_KEY
	join CDW.FLOWSHEET_RECORD fr on vsr.VSI_KEY = fr.VSI_KEY
	join CDW.FLOWSHEET_MEASURE fm on fr.FS_REC_KEY = fm.FS_REC_KEY
	join CDW.FLOWSHEET f on fm.FS_KEY = f.FS_KEY
  --  JOIN ENCS ON ENCS.ANES_VSI_KEY = FR.VSI_KEY --CHANGED 7/9/2018 after data validation session
    JOIN ENCS ON ENCS.HSP_VAI_KEY = FR.VSI_KEY

where 1=1
  AND vsr.SEQ_NUM = fm.OCCURANCE /*Necessary to match the specific instance of the flowsheet recording to the correct measure value*/
  AND LDA.FS_KEY IN (138702,138697,139076,138841) --  UAC SINGLE AND UAC DOUBLE LUMENS, ARTERIAL LINE
 -- AND FR.VSI_KEY = '31604862'
 GROUP BY 1,2,3,4
)

,TMP AS 
	(select  
	      or_log.log_key,  
		 ENTRY_DT,--FLOW_MEA.FS_KEY, FLOW_MEA.MEAS_VAL
	     CAST(((fm.MEAS_VAL_num)-32)*(5/9.0) AS NUMERIC(4,1)) TMP,
		 ROW_NUMBER() OVER (PARTITION BY OR_LOG.log_key ORDER BY TMP) TMP_ORDER
	from or_log
		join anesthesia_encounteR_link ael on or_log.log_key = ael.or_log_key
		join visit_stay_info vsi on ael.ANES_VISIT_KEY = vsi.VISIT_KEY
		join flowsheet_record fr on vsi.VSI_KEY = fr.VSI_KEY
		join flowsheet_measure fm on fr.FS_REC_KEY = fm.FS_REC_KEY
		join flowsheet f on fm.FS_KEY = f.FS_KEY
		JOIN  ENCS  ON ENCS.OR_LOG_KEY = ael.OR_LOG_KEY and fm.REC_DT BETWEEN ENCS.ANES_START_TM AND ENCS.ANES_END_TM
	where 
	f.FS_ID in (7727,7717,7715,7725,7729,7723,7735,7733,7719)
	and meas_val_num > 95.0
	) 

,TMPSITE AS
	     ( SELECT
		    or_log.log_key,
			VAI.VSI_KEY,
			ENTRY_DT,
	        CASE WHEN UPPER(FLOW_MEA.MEAS_VAL) = 'AXILLARY' THEN 414
			         WHEN  UPPER(FLOW_MEA.MEAS_VAL) = 'ORAL' THEN 410 --Nasal
					 WHEN  UPPER(FLOW_MEA.MEAS_VAL) = 'RECTAL' THEN 413
					 WHEN  UPPER(FLOW_MEA.MEAS_VAL) = 'ESOPHAGEAL PROBE' THEN 411
					 WHEN  UPPER(FLOW_MEA.MEAS_VAL) = 'TEMPORAL' THEN  409 --Skin 2894 Tympanic?
					 WHEN  UPPER(FLOW_MEA.MEAS_VAL) = 'BLADDER PROBE' THEN 412
					 WHEN  UPPER(FLOW_MEA.MEAS_VAL) = 'CORE TEMPERATURE (ECMO, SWAN GANZ)' THEN 2895
					 WHEN  UPPER(FLOW_MEA.MEAS_VAL) IS NULL THEN NULL END TMP_SITE,
			ROW_NUMBER() OVER (PARTITION BY VAI.VSI_KEY ORDER BY ENTRY_DT, REC_DT) TMP_SITE_ORDER
		FROM 
		    or_log
			join anesthesia_encounteR_link ael on or_log.log_key = ael.or_log_key 
			join CDW.VISIT_ADDL_INFO VAI ON VAI.VISIT_KEY = ael.VISIT_KEY
		    JOIN CDW.FLOWSHEET_RECORD FLOW_REC ON FLOW_REC.VSI_KEY=VAI.VSI_KEY 
		    INNER JOIN CDW.FLOWSHEET_MEASURE FLOW_MEA ON FLOW_MEA.FS_REC_KEY = FLOW_REC.FS_REC_KEY
		    INNER JOIN CDW.FLOWSHEET FLOW ON FLOW.FS_KEY=FLOW_MEA.FS_KEY 
			JOIN ENCS ON ENCS.OR_LOG_KEY = ael.OR_LOG_KEY  and FLOW_MEA.REC_DT BETWEEN ENCS.ANES_START_TM AND ENCS.ANES_END_TM
		WHERE FLOW_MEA.FS_KEY IN (133512) --134071,148995,149098,148987,148979,149089,)
		 AND MEAS_VAL IS NOT NULL
      )


,
PCP AS
(SELECT VSI_KEY,
        PERC_PLACE_DT,
		PERC_REMOVE_DT,
		INS_BY,
		PCP,
        MIN(CASE WHEN SIDE = 'RIGHT' AND INS_VESSEL = 'INTERNAL JUGULAR' THEN '1' ELSE '2' END) AS RIGHT_INTERNAL_JUGULAR,
		MIN(CASE WHEN SIDE = 'RIGHT' AND INS_VESSEL = 'SUBCLAVIAN' THEN '1' ELSE '2' END) AS RIGHT_SUBCLAVIAN,
		MIN(CASE WHEN SIDE = 'RIGHT' AND INS_VESSEL = 'FEMORAL' THEN '1' ELSE '2' END) AS RIGHT_FEMORAL_VEIN,
        MIN(CASE WHEN SIDE = 'LEFT' AND INS_VESSEL = 'INTERNAL JUGULAR' THEN '1' ELSE '2' END) AS LEFT_INTERNAL_JUGULAR,
		MIN(CASE WHEN SIDE = 'LEFT' AND INS_VESSEL = 'SUBCLAVIAN' THEN '1' ELSE '2' END) AS LEFT_SUBCLAVIAN,
		MIN(CASE WHEN SIDE = 'LEFT' AND INS_VESSEL = 'FEMORAL' THEN '1' ELSE '2' END) AS LEFT_FEMORAL_VEIN,
		MIN(CASE WHEN SIDE IN ('RIGHT','LEFT') AND NOT(INS_VESSEL IN ('INTERNAL JUGULAR','SUBCLAVIAN','FEMORAL','INTERNAL JUGULAR','SUBCLAVIAN','FEMORAL')) THEN '1' ELSE '2' END) AS PCP_OTHER
        
FROM
	(SELECT DISTINCT 
			FR.VSI_KEY,
	        PLACE_DT AS PERC_PLACE_DT,
			REMOVE_DT AS PERC_REMOVE_DT,
		    '1' AS PCP,
			MAX(CASE WHEN F.FS_KEY = 133550  THEN UPPER(MEAS_VAL) ELSE '' END) AS SIDE,
			MAX(CASE WHEN F.FS_KEY = 133544  THEN UPPER(MEAS_VAL) ELSE '' END) AS INS_VESSEL,
	        MAX(CASE WHEN F.FS_KEY = 133572  THEN UPPER(MEAS_VAL) ELSE '' END) AS INS_TECH,
			MAX(CASE WHEN F.FS_KEY = 133563  THEN UPPER(MEAS_VAL) ELSE '' END) AS INS_BY
	from CDW.PATIENT_LDA lda 
	    join CDW.VISIT_STAY_INFO VSI ON VSI.VISIT_KEY = LDA.VISIT_KEY
		join CDW.VISIT_STAY_INFO_ROWS vsr on VSI.VSI_KEY = vsr.VSI_KEY
		join CDW.FLOWSHEET_RECORD fr on vsr.VSI_KEY = fr.VSI_KEY
		join CDW.FLOWSHEET_MEASURE fm on fr.FS_REC_KEY = fm.FS_REC_KEY
		join CDW.FLOWSHEET f on fm.FS_KEY = f.FS_KEY
		join CDW.FLOWSHEET_LDA_GROUP lda_group on vsr.FS_KEY = lda_group.FS_KEY
		JOIN ENCS ON ENCS.ANES_VSI_KEY = FR.VSI_KEY

	where 1=1
	  AND vsr.SEQ_NUM = fm.OCCURANCE /*Necessary to match the specific instance of the flowsheet recording to the correct measure value*/
	  AND lda_group.DICT_LDA_TYPE_KEY in (20862,20861,21280, 21290) -- PICC, CVA, LA OR RA
	  AND F.FS_KEY IN (133550,133544,133563,133572)
	GROUP BY 1,2,3,4
	)a
GROUP BY 1,2,3,4,5
)
 
SELECT 
     cast(ISNULL(LOG_ID,'') as integer)  CaseNumber
	,cast(ISNULL(ARTERIAL_LINE,'2')as integer) ArtLine
	,cast(ISNULL(ART_RADIAL,'2')as integer) ArtLineTypeRad
	,cast(ISNULL(ART_BRACHIAL,'2')as integer) ArtLineTypeBrach
	,cast(ISNULL(ART_AXILLARY,'2')as integer) ArtLineTypeAx
	,cast(ISNULL(ART_FEMORAL,'2')as integer) ArtLineTypeFem
	,cast(ISNULL(ART_ULNAR,'2')as integer) ArtLineTypeUlnar 
	,cast(ISNULL(ART_DORSALIS_PEDIS,'2')as integer) ArtLineTypeDors
	,cast(ISNULL(ART_POSTERIOR_TIBIAL,'2')as integer) ArtLineTypePost
	,cast(ISNULL(ART_UMBILICAL,'2')as integer)ArtLineTypeCent
	,cast(ISNULL(ART_LINE_SITU,'2')as integer) ArtLinePreProc
	,cast(ISNULL(CUTDOWN,'2')as integer) Cutdown
	,cast(ISNULL(CUTDOWN_RADIAL,'2')as integer) CutdownRad
	,cast(ISNULL(CUTDOWN_ULNAR,'2')as integer) CutdownUln
	,cast(ISNULL(CUTDOWN_FEMORAL,'2')as integer) CutdownFem
	,cast(ISNULL(CUTDOWN_OTHER,'2')as integer) CutdownOth 
	,cast(ISNULL(PCP,'2')as integer) PerCentPress
	,cast(ISNULL(RIGHT_INTERNAL_JUGULAR,'2')as integer) PCPLocRJug
	,cast(ISNULL(RIGHT_SUBCLAVIAN,'2')as integer) PCPLocRSub
	,cast(ISNULL(RIGHT_FEMORAL_VEIN,'2')as integer) PCPLocRFem
	,cast(ISNULL(LEFT_INTERNAL_JUGULAR,'2')as integer) PCPLocLJug
	,cast(ISNULL(LEFT_SUBCLAVIAN,'2')as integer) PCPLocLSub
	,cast(ISNULL(LEFT_FEMORAL_VEIN,'2')as integer) PCPLocLFem
	,cast(ISNULL(PCP_OTHER,'2')as integer) PCPLocOth
	,cast(ISNULL(CVP_PICC_LA_RA_LINE_IN_SITU,'2')as integer) CVPPICCPREPROC
	,cast(NULL as integer) CVPPlaced
	,cast(ISNULL(ART_SURG_PLACE,'2')as integer) SurgMonLines
	,cast(TMP as NUMERIC(4,1)) LowIntraopTemp
	,CASE WHEN TMP IS NULL THEN NULL ELSE cast(isnull(TMP_SITE,410) as integer) END IntraopTempSite
	,cast(ISNULL(TEE,'2')as integer) TEE
FROM
(SELECT ENCS.*,
	   ARTERIAL_LINE.*,
	   ARTERIAL_LINE_SITU.ART_LINE_SITU,
	   COALESCE(PCPSITU.PCP,PCPPOST.PCP,'2') PCP,
       COALESCE(PCPSITU.RIGHT_INTERNAL_JUGULAR,PCPPOST.RIGHT_INTERNAL_JUGULAR,'2') RIGHT_INTERNAL_JUGULAR,
	   COALESCE(PCPSITU.RIGHT_SUBCLAVIAN,PCPPOST.RIGHT_SUBCLAVIAN,'2') RIGHT_SUBCLAVIAN,
	   COALESCE(PCPSITU.RIGHT_FEMORAL_VEIN,PCPPOST.RIGHT_FEMORAL_VEIN,'2') RIGHT_FEMORAL_VEIN,
 	   COALESCE(PCPSITU.LEFT_INTERNAL_JUGULAR,PCPPOST.LEFT_INTERNAL_JUGULAR,'2') LEFT_INTERNAL_JUGULAR,
 	   COALESCE(PCPSITU.LEFT_SUBCLAVIAN,PCPPOST.LEFT_SUBCLAVIAN,'2') LEFT_SUBCLAVIAN,
	   COALESCE(PCPSITU.LEFT_FEMORAL_VEIN,PCPPOST.LEFT_FEMORAL_VEIN,'2') LEFT_FEMORAL_VEIN,
	   COALESCE(PCPSITU.PCP_OTHER,PCPPOST.PCP_OTHER,'2') PCP_OTHER,
	   CASE WHEN PCPSITU.PERC_PLACE_DT < ANES_START_DTTM and PCPSITU.PERC_REMOVE_DT > ANES_START_DTTM THEN '1' ELSE '2' END AS CVP_PICC_LA_RA_LINE_IN_SITU,
	   COALESCE(PCPSITU.INS_BY,PCPPOST.INS_BY,'2') CVP_PLACED_BY_ANES,
	   TMP.TMP,
	   TMPSITE.TMP_SITE
FROM ENCS
                    LEFT JOIN (SELECT ARTERIAL_LINE.*, '1' ART_LINE_SITU, ROW_NUMBER() OVER (PARTITION BY ARTERIAL_LINE.VSI_KEY ORDER BY ART_LINE_PLACE_DT DESC) ROWNO
					              FROM ARTERIAL_LINE JOIN ENCS ON ARTERIAL_LINE.VSI_KEY = ENCS.HSP_VAI_KEY 
								 WHERE ARTERIAL_LINE.ART_LINE_PLACE_DT < ANES_START_DTTM and ARTERIAL_LINE.ART_LINE_REMOVE_DT > ANES_START_DTTM) ARTERIAL_LINE_SITU ON ARTERIAL_LINE_SITU.VSI_KEY = ENCS.HSP_VAI_KEY and ARTERIAL_LINE_SITU.ROWNO = 1
					 LEFT JOIN (SELECT ARTERIAL_LINE.*, ROW_NUMBER() OVER (PARTITION BY ARTERIAL_LINE.VSI_KEY ORDER BY ART_LINE_PLACE_DT) ROWNO
					              FROM ARTERIAL_LINE JOIN ENCS ON ARTERIAL_LINE.VSI_KEY = ENCS.HSP_VAI_KEY 
								 WHERE ARTERIAL_LINE.ART_LINE_PLACE_DT >= ANES_START_DTTM) ARTERIAL_LINE ON ARTERIAL_LINE.VSI_KEY = ENCS.HSP_VAI_KEY and ARTERIAL_LINE.ROWNO = 1
					 LEFT JOIN (SELECT PCP.*, ROW_NUMBER() OVER (PARTITION BY PCP.VSI_KEY ORDER BY PERC_PLACE_DT DESC)  ROWNO
					              FROM PCP JOIN ENCS ON ENCS.HSP_VAI_KEY = PCP.VSI_KEY 
								 WHERE PCP.PERC_PLACE_DT < ANES_START_DTTM and PCP.PERC_REMOVE_DT > ANES_START_DTTM) PCPSITU ON PCPSITU.VSI_KEY = ENCS.HSP_VAI_KEY and PCPSITU.ROWNO = 1
					 LEFT JOIN (SELECT PCP.*, ROW_NUMBER() OVER (PARTITION BY PCP.VSI_KEY ORDER BY PERC_PLACE_DT) ROWNO
					              FROM PCP JOIN ENCS ON ENCS.HSP_VAI_KEY = PCP.VSI_KEY 
								 WHERE PCP.PERC_PLACE_DT >= ANES_START_DTTM ) PCPPOST ON PCPPOST.VSI_KEY = ENCS.HSP_VAI_KEY AND PCPPOST.ROWNO =1
					 LEFT JOIN TMP ON TMP.LOG_KEY = ENCS.LOG_KEY	AND TMP_ORDER = 1
					 LEFT JOIN TMPSITE ON TMPSITE.LOG_KEY = ENCS.LOG_KEY	AND TMP_SITE_ORDER = 1
	) A
ORDER BY 1