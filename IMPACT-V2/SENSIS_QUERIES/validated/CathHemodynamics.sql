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
	   
		,CASE WHEN HEMODYN.SASAT IS NULL THEN 1 ELSE 0 END AS SYSTEMICARTSATNA
		 ,CAST(HEMODYN.SASAT AS NUMERIC(4,1)) AS SYSTEMICARTSAT
		,CASE WHEN HEMODYN.MVSAT IS NULL THEN 1 ELSE 0 END AS MIXVENSATNA
		,HEMODYN.MVSAT MIXVENSAT
		,CASE WHEN HEMODYN.LVS IS NULL THEN 1 ELSE 0 END AS SYSTEMVENTSYSPRESNA
		,CAST(HEMODYN.LVS AS NUMERIC(4,1)) SYSTEMVENTSYSPRES
		,CASE WHEN HEMODYN.LVD IS NULL THEN 1 ELSE 0 END AS SYSTEMVENTENDDIAPRESNA		
		,HEMODYN.LVD SYSTEMVENTENDDIAPRES
		,CASE WHEN HEMODYN.AOS IS NULL THEN 1 ELSE 0 END AS SYSTEMSYSBPNA
		,HEMODYN.AOS SYSTEMSYSBP
		,CASE WHEN HEMODYN.AOD IS NULL THEN 1 ELSE 0 END AS SYSTEMDIABPNA
		,HEMODYN.AOD SYSTEMDIABP
		,CASE WHEN HEMODYN.AOM IS NULL THEN 1 ELSE 0 END AS SYSTEMMEANBPNA
		,HEMODYN.AOM SYSTEMMEANBP
		,CASE WHEN HEMODYN.MPAS IS NULL THEN 1 ELSE 0 END AS PULMARTSYSPRESNA
		,CAST(HEMODYN.MPAS AS NUMERIC(4,1)) PULMARTSYSPRES
		,CASE WHEN HEMODYN.MPAM IS NULL THEN 1 ELSE 0 END AS PULMARTMEANPRESNA
		,CAST(HEMODYN.MPAM AS NUMERIC(4,1)) PULMARTMEANPRES
		,CASE WHEN HEMODYN.RV IS NULL THEN 1 ELSE 0 END AS PULMVENTSYSPRESNA
		,HEMODYN.RV PULMVENTSYSPRES
		,CASE WHEN HEMODYN.PVR IS NULL THEN 1 ELSE 0 END AS PULMVASCRESTINDNA
		,HEMODYN.PVR PULMVASCRESTIND
		,CASE WHEN HEMODYN.CI IS NULL THEN 1 ELSE 0 END AS CARDINDNA
		,CAST(HEMODYN.CI AS NUMERIC(3,1)) CARDIND
		,CASE WHEN HEMODYN.QPQS IS NULL THEN 1 ELSE 0 END AS QPQSRATIONA
		,CAST(HEMODYN.QPQS AS NUMERIC(3,1)) QPQSRATIO

  FROM CATH_STUDY STUDY JOIN SENSIS_HEMODYN HEMODYN ON STUDY.REFNO = HEMODYN.REFNO