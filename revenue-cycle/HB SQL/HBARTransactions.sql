SELECT
  HOSPITAL_ACCOUNT.HSP_ACCT_ID,
  CDW_DICT_HA_D_ACCT_CLS.DICT_NM,
  add_months(date_trunc('MONTH',now()),-1),
  last_day(add_months(date_trunc('MONTH',now()),-1)),
  DATE(HOSPITAL_ACCOUNT.ADM_DT) as ADM_DT,
  HOSPITAL_ACCOUNT.HB_PAT_NM,
  CASE WHEN HSP_ACCT_NM LIKE '%HB SUSPENSE ACCOUNT%' THEN 1 ELSE 0 END AS SUSP_ACCT_IND,
  DATE(HOSPITAL_ACCOUNT.DISCH_DT) as DISCH_DT,
  DISCH.DEPT_NM AS DISCH_DEPT,
  CDW_MSTR_DT_HTR_PST_DT.FULL_DT as PST_DT,
  last_day(CDW_MSTR_DT_HTR_PST_DT.FULL_DT) as POST_MONTH,
  EMPLOYEE.FULL_NM,
  FACT_TRANSACTION_HB.TRANS_CMT,
  CDW_PAYOR_HB.PAYOR_ID,
  CDW_PAYOR_HB.RPT_GRP_6,
  CDW_PAYOR_HB.PAYOR_NM,
  FACT_TRANSACTION_HB.CREDIT_GL_NUM,
  FACT_TRANSACTION_HB.TX_ID,
  FACT_TRANSACTION_HB.DEBIT_GL_NUM,
  PROCEDURE.PROC_GRP_CAT_NM,
  PROCEDURE.PROC_GRP_NM,
  PROCEDURE.PROC_CD,
  PROCEDURE.PROC_NM,
  CDW_D_HTR_TX_TYP.DICT_NM as TRANSACTION_TYPE, 
  FACT_TRANSACTION_HB.BD_ADJ_AMT+FACT_TRANSACTION_HB.CON_ADJ_AMT+FACT_TRANSACTION_HB.MISC_BS_ADJ_AMT+FACT_TRANSACTION_HB.MISC_PL_ADJ_AMT+
  FACT_TRANSACTION_HB.OTHER_ADJ_AMT+FACT_TRANSACTION_HB.PMT_AMT+FACT_TRANSACTION_HB.CHRG_AMT as AMT
FROM
   CDWPRD.ADMIN.PROCEDURE 
   RIGHT OUTER JOIN CDWPRD.ADMIN.FACT_TRANSACTION_HB ON (FACT_TRANSACTION_HB.PROC_KEY=PROCEDURE.PROC_KEY)
   RIGHT OUTER JOIN CDWPRD.ADMIN.HOSPITAL_ACCOUNT ON (FACT_TRANSACTION_HB.HSP_ACCT_KEY=HOSPITAL_ACCOUNT.HSP_ACCT_KEY)
   LEFT OUTER JOIN CDWPRD.ADMIN.CDW_DICTIONARY  CDW_DICT_HA_D_ACCT_CLS ON (CDW_DICT_HA_D_ACCT_CLS.DICT_KEY=HOSPITAL_ACCOUNT.DICT_ACCT_CLASS_KEY)
   LEFT OUTER JOIN CDWPRD.ADMIN.CDW_DICTIONARY  CDW_D_HTR_D_ACCT_CLS ON (FACT_TRANSACTION_HB.DICT_ACCT_CLASS_KEY=CDW_D_HTR_D_ACCT_CLS.DICT_KEY)
   LEFT OUTER JOIN CDWPRD.ADMIN.PAYOR  CDW_PYR_HTR_PYR ON (CDW_PYR_HTR_PYR.PAYOR_KEY=FACT_TRANSACTION_HB.PAYOR_KEY)
   LEFT OUTER JOIN CDWPRD.ADMIN.MASTER_DATE  CDW_MSTR_DT_HTR_PST_DT ON (CDW_MSTR_DT_HTR_PST_DT.DT_KEY=FACT_TRANSACTION_HB.POST_DT_KEY)
   LEFT OUTER JOIN CDWPRD.ADMIN.CDW_DICTIONARY  CDW_D_HTR_TX_TYP ON (CDW_D_HTR_TX_TYP.DICT_KEY=FACT_TRANSACTION_HB.TRANS_TYPE_KEY)
   LEFT OUTER JOIN CDWPRD.ADMIN.EMPLOYEE ON (EMPLOYEE.EMP_KEY=FACT_TRANSACTION_HB.USER_EMP_KEY)
   LEFT OUTER JOIN CDWPRD.ADMIN.PAYOR  CDW_PAYOR_HB ON (CDW_PAYOR_HB.PAYOR_KEY=FACT_TRANSACTION_HB.PRIMARY_PAYOR_KEY)
   LEFT OUTER JOIN CDWPRD.ADMIN.DEPARTMENT DISCH ON DISCH.DEPT_KEY = HOSPITAL_ACCOUNT.DISCH_DEPT_KEY
where CDW_MSTR_DT_HTR_PST_DT.FULL_DT between add_months(date_trunc('month',current_date),-17) and add_months(last_day(current_date),-1)
   ;