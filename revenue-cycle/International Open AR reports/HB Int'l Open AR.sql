 
---Int'l Open AR for Hospital; bucket type ------ 

SELECT HAC.HSP_ACCT_ID ,
       max (LBI.INV_NUM) AS LAST_INV_NUM ,
       HAC.HB_PAT_MRN ,
       HAC.HSP_ACCT_NM ,
        DATE (HAC.ADM_DT) AS ADMIN_DATE,
       DATE (HAC.DISCH_DT) AS DISCH_DATE,
       LB.CHRG_TOT ,
       LB.PMT_TOT ,
       LB.ADJ_TOT ,
       LB.CURR_BAL ,
       HAC.TOT_ACCT_BAL,
       HAC.HB_PAT_COUNTRY ,
       PAT.COUNTRY ,
       DICT.DICT_CAT_NM ,
       DICT.DICT_NM ,
       PAY.PAYOR_ID ,
       PAY.PAYOR_NM ,
       CDWPRD.ADMIN.BENEFIT_PLAN.BP_NM ,
     CURRENT_DATE - DISCH_DATE AS days_from_disc,
     --Adding aging to the report 
      CASE 
    WHEN (days_from_disc BETWEEN 31 AND 60) THEN '31-60'
    WHEN (days_from_disc BETWEEN 61 AND 91) THEN '61-90'
    WHEN (days_from_disc BETWEEN 91 AND 180) THEN '91-180'
    WHEN (days_from_disc BETWEEN 181 AND 365) THEN '181-365'
    WHEN (days_from_disc  > 365) THEN '365+'
    ELSE '0-30' END AS aging ,
       
       CASE WHEN (PAY.PAYOR_NM='INVALID' OR PAY.PAYOR_NM='default') THEN NULL 
            ELSE PAY.PAYOR_NM 
       END

FROM CDWPRD..LIABILITY_BUCKET LB
left JOIN CDWPRD..LIABILITY_BUCKET_INVOICE LBI ON LB.LIAB_BKT_KEY  = LBI.LIAB_BKT_KEY 
inner JOIN CDWPRD..LIABILITY_BUCKET_ACCOUNT_XREF LBA ON LB.LIAB_BKT_KEY  = LBA.LIAB_BKT_KEY 
inner JOIN CDWPRD..HOSPITAL_ACCOUNT HAC ON LBA.HSP_ACCT_KEY = HAC.HSP_ACCT_KEY 
inner JOIN CDWPRD..PAYOR PAY ON LB.PAYOR_KEY = PAY.PAYOR_KEY 
inner JOIN CDWPRD..PATIENT PAT ON HAC.PAT_KEY = PAT.PAT_KEY 
--INNER JOIN CDW_ODS.ADMIN.HSP_BUCKET ON CDWPRD.ADMIN.LIABILITY_BUCKET.BKT_ID =CDW_ODS.ADMIN.HSP_BUCKET.BUCKET_ID 
INNER JOIN CDWPRD..CDW_DICTIONARY DICT ON LB.DICT_BKT_STAT_KEY = DICT.DICT_KEY 
INNER JOIN CDWPRD.ADMIN.FACT_AR_HISTORY_HB ON LB.LIAB_BKT_KEY  = CDWPRD.ADMIN.FACT_AR_HISTORY_HB.LIAB_BKT_KEY 
--INNER JOIN CDWPRD.FACT_AR_HISTORY_HB AR ON LB.LIAB_BKT_KEY   = CDWPRD.FACT_AR_HISTORY_HB.LIAB_BKT_KEY
JOIN CDWPRD.ADMIN.BENEFIT_PLAN ON LB.BP_KEY = CDWPRD.ADMIN.BENEFIT_PLAN.BP_KEY
where PAY.PAYOR_ID IN (1098,1195,1170,1171,1173,1174,1205,-1) 
  AND HAC.PRI_PAYOR_KEY IN (2388,4612,4613,4614,4615,7211,8411)
  AND HAC.TOT_ACCT_BAL <> '0' 
  AND LB.DICT_BKT_STAT_KEY IN (8727,8723)
  AND LB.CURR_BAL <> '0'
GROUP BY 
         HAC.HSP_ACCT_ID ,
         HAC.HB_PAT_MRN ,
         HAC.HSP_ACCT_NM ,
      ADMIN_DATE,
     DISCH_DATE,
         LB.CHRG_TOT ,
         LB.PMT_TOT ,
         LB.ADJ_TOT ,
         LB.CURR_BAL ,
         HAC.TOT_ACCT_BAL,
         HAC.HB_PAT_COUNTRY ,
         PAT.COUNTRY ,
         DICT.DICT_CAT_NM ,
         DICT.DICT_NM ,
         CASE WHEN (PAY.PAYOR_NM='INVALID' OR PAY.PAYOR_NM='default') THEN NULL 
            ELSE PAY.PAYOR_NM 
         END,
     
         PAY.PAYOR_NM ,
         aging,
         days_from_disc,
         CDWPRD.ADMIN.BENEFIT_PLAN.BP_NM ,
         PAY.PAYOR_ID 
         ORDER BY days_from_disc ASC;


