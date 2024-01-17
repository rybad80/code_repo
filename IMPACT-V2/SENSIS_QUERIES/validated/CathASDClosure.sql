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
FROM SENSIS_STUDY STUDY JOIN CDWUAT..PATIENT_MATCH ON STUDY.REFNO = PATIENT_MATCH.SRC_SYS_ID AND SRC_SYS_NM = 'SENSIS'
                                     JOIN CDWUAT..PATIENT ON PATIENT.PAT_KEY = PATIENT_MATCH.PAT_KEY
									 JOIN SENSIS_CT CT ON STUDY.REFNO = CT.REFNO
									 JOIN CDWUAT..PROCEDURE_ORDER PO ON PO.PROC_ORD_ID = STUDY.ORDNUM
									 JOIN CDWUAT..OR_CASE_ORDER OCO ON OCO.ORD_KEY = PO.PROC_ORD_KEY
									 JOIN CDWUAT..OR_LOG ON OR_LOG.CASE_KEY = OCO.OR_CASE_KEY
									 JOIN CDWUAT..VISIT V1 ON V1.VISIT_KEY = OR_LOG.VISIT_KEY
									 --JOIN VISIT V2 ON V2.VISIT_KEY = OR_LOG.ADMIT_VISIT_KEY
									 LEFT JOIN SENSIS_PD PD ON STUDY.REFNO = PD.REFNO
									 LEFT JOIN (SELECT REFNO, MIN(SHTIME) SH_ACC_TM FROM SENSIS_ASR GROUP BY REFNO) SHEATH ON STUDY.REFNO = SHEATH.REFNO
									 LEFT JOIN SENSIS_POCT POCT ON STUDY.REFNO = POCT.REFNO 
WHERE POCT.PRTYPE <> 10 --Fluoro
)
  
    select
        SURG_ENC_ID
        ,CASE WHEN ASDATA.IP7000 = 1 THEN 1470
		      WHEN ASDATA.IP7000 = 2 THEN 1473
			  WHEN ASDATA.IP7000 = 3 THEN 1476
			  WHEN ASDATA.IP7000 = 4 THEN 1471
			  WHEN ASDATA.IP7000 = 5 THEN 1474
			  WHEN ASDATA.IP7000 = 6 THEN 1477
			  WHEN ASDATA.IP7000 = 7 THEN 1472
			  WHEN ASDATA.IP7000 = 8 THEN 1475
			  WHEN ASDATA.IP7000 = 9 THEN 4159
			  ELSE NULL END AS ASDPROCIND   
		,cast(ASDATA.IP7005 as NUMERIC(4,1)) ASDSEPTLENGTH
		,ASDATA.IP7010 ASDANEURYSM
		,ASDATA.IP7015 ASDMULTIFEN
	    ,CASE WHEN ASDATA.IP7005 IS NULL THEN 1 ELSE 0 END ASDSEPTLENGTHNA
  FROM CATH_STUDY STUDY JOIN SENSIS_ASDATA ASDATA ON STUDY.REFNO = ASDATA.REFNO
  
  --select * from CDW_ODS_UAT..SENSIS_ASDATA WHERE REFNO = 53352