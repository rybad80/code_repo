WITH CATH_STUDY AS
(SELECT STUDY.REFNO, 
        STUDY.ADMISSID, 
		CAST(V1.ENC_ID AS INT)  SURG_ENC_ID,
		STUDY.ACCESSNO,
		STUDY.ORDNUM,
        STUDATE,
		SH_ACC_TM,
		POCT.PREND,
		PATIENT_MATCH.PAT_KEY, 
		PAT_MRN_ID,
		PD.HEIGHT,
		PD.WEIGHT  --SELECT *
FROM SENSIS_STUDY STUDY JOIN CDWPRD..PATIENT_MATCH ON STUDY.REFNO = PATIENT_MATCH.SRC_SYS_ID AND SRC_SYS_NM = 'SENSIS'
                                     JOIN CDWPRD..PATIENT ON PATIENT.PAT_KEY = PATIENT_MATCH.PAT_KEY
									 JOIN SENSIS_CT CT ON STUDY.REFNO = CT.REFNO
									 JOIN CDWPRD..PROCEDURE_ORDER PO ON PO.PROC_ORD_ID = STUDY.ORDNUM
									 JOIN CDWPRD..OR_CASE_ORDER OCO ON OCO.ORD_KEY = PO.PROC_ORD_KEY
									 JOIN CDWPRD..OR_LOG ON OR_LOG.CASE_KEY = OCO.OR_CASE_KEY
									 JOIN CDWPRD..VISIT V1 ON V1.VISIT_KEY = OR_LOG.VISIT_KEY
									 --JOIN VISIT V2 ON V2.VISIT_KEY = OR_LOG.ADMIT_VISIT_KEY
									 LEFT JOIN SENSIS_PD PD ON STUDY.REFNO = PD.REFNO
									 LEFT JOIN (SELECT REFNO, MIN(SHTIME) SH_ACC_TM FROM SENSIS_ASR GROUP BY REFNO) SHEATH ON STUDY.REFNO = SHEATH.REFNO
									 LEFT JOIN SENSIS_POCT POCT ON STUDY.REFNO = POCT.REFNO 
WHERE POCT.PRTYPE <> 10 --Fluoro 
--AND SURG_ENC_ID = '2060280144'
)

  SELECT
         SURG_ENC_ID
	   ,CASE WHEN IMPUSE IN (1,2,3) THEN 2004
		  WHEN IMPUSE IN (16,17,18) THEN 2009
		  WHEN IMPUSE IN (13,14,15) THEN 2008
			ELSE IMPUSE END AS CATHPROCID
		,IPDEVCE.IMPACT DEVID
        ,CASE WHEN IPDEVCE.IP7090 = 1 THEN 1467
		      WHEN IPDEVCE.IP7090 IN (2,9,10) THEN 1468
			  WHEN IPDEVCE.IP7090 = 3 THEN 1469
			  WHEN IPDEVCE.IP7090 = 4 THEN 1498  
			  WHEN IPDEVCE.IP7090 = 5 THEN 1499
			  WHEN IPDEVCE.IP7090 = 6 THEN 4157 
			  WHEN IPDEVCE.IP7090 = 7 THEN 1536 
			  WHEN IPDEVCE.IP7090 = 8 THEN 4158 
			  ELSE 1467 END AS DEVOUTCOME
		,IPDEVCE.IP7089 DEFECTCOUNTERASSN
		,CAST(SEQNO AS INT) SORT
		--,ipdevce.*
FROM CATH_STUDY STUDY JOIN SENSIS_IPDEVCE IPDEVCE ON STUDY.REFNO = IPDEVCE.REFNO
WHERE IPDEVCE.IMPACT IS NOT NULL AND IMPUSE IN (1,2,3,13,14,15,16,17,18) 

 
UNION ALL 
 
SELECT
        SURG_ENC_ID
		,3625 CATHPROCID
		,IP2DVCE.IMPACT DEVID
        ,CASE WHEN IP2DVCE.I11135 = 1 THEN 4118
		      WHEN IP2DVCE.I11135 = 2 THEN 4119
			  WHEN IP2DVCE.I11135 = 3 THEN 4120
			  WHEN IP2DVCE.I11135 = 4 THEN 4121
		ELSE IP2DVCE.I11135  END AS DEVOUTCOME
		,IP2DVCE.IP7089 DEFECTCOUNTERASSN
		,CAST(SEQNO AS INT) SORT

FROM CATH_STUDY STUDY JOIN SENSIS_IP2DVCE IP2DVCE ON STUDY.REFNO = IP2DVCE.REFNO
WHERE IP2DVCE.IMPACT IS NOT NULL 