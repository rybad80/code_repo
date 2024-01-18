Select
Distinct
ETR.TX_ID,
TRX.ACTION_TYPE_C,
--ZTT.NAME ACTION_TYPE,
TRX.ACTION_DATE,
TRX.ACTION_AMOUNT,
TRX.PAYOR_ID,
EPM.PAYOR_NAME,
TRX.DENIAL_CODE,
RMC.REMIT_CODE_NAME DENIAL_CODE_NAME,
last_day(TRX.POST_DATE) as POST_MONTH,
TRX.POST_DATE,
TRX.STMT_DATE STATEMENT_DATE,
TRX.ACTION_USER_ID,
EMP.NAME ACTION_USER,
TRX.ACTION_REMIT_CODES,
TRX.ACTION_HX_SOURCE_C,
TRX.ACCOUNT_ID,
ARP.SERV_PROVIDER_ID,
SER.PROV_NAME SERVICE_PROVIDER_NAME,
ATM.REFERRAL_PROV_ID,
REF.REFERRING_PROV_NAM REFERRING_PROV_NAME,
ARP.ORIGINAL_EPM_ID,
EPM1.PAYOR_NAME ORIGINAL_PAYOR_NAME,
ARP.ORIGINAL_FC_C,
ZFC.NAME ORIGINAL_FINANCIAL_CLASS,
ARP.FACILITY_ID,
POS.POS_NAME FACILITY_NAME,
ARP.DEPARTMENT_ID,
DEP.DEPARTMENT_NAME,
GL.GL_PREFIX as COST_CNTR_ID,
ARP.POS_ID,
POS1.POS_NAME ,
ARP.LOC_ID,
LOC.LOC_NAME LOCATION_NAME,
ARP.SERVICE_AREA_ID,
SA.SERV_AREA_NAME SERVICE_AREA_NAME,
ARP.MODIFIER_ONE,
ARP.MODIFIER_TWO,
ARP.MODIFIER_THREE,
ARP.MODIFIER_FOUR,
ATM.COVERAGE_PLAN_ID,
EPP.BENEFIT_PLAN_NAME COVERAGE_PLAN_NAME,
EAP.PROC_CODE,
EAP.PROC_NAME PROCEDURE_NAME,
ARP.BILLING_PROV_ID,
SER1.PROV_NAME BILLING_PROV_NAME,
ARP.CLAIM_DATE,
ARP.VISIT_NUMBER,
ETR.HSP_ACCOUNT_ID,
ARP.SERVICE_DATE,
CASE 
WHEN EPM.PAYOR_NAME = 'KEYSTONE FIRST MEDICAID HMO' and TRX.DENIAL_CODE = '9008' AND EAP.PROC_CODE IN ('90460','90461') THEN 'Exclude KF Vaccine' ELSE 'Include' END AS KF_VACCINE
from
CDW_ODS.ADMIN.CLARITY_TDL_TRAN ETR
JOIN CDW_ODS.ADMIN.ARPB_TX_ACTIONS TRX ON ETR.MATCH_TRX_ID = TRX.TX_ID AND TRX.ACTION_TYPE_C = 9
JOIN CDW_ODS.ADMIN.ARPB_TRANSACTIONS ARP ON ARP.TX_ID = TRX.TX_ID
LEFT JOIN CDW_ODS.ADMIN.ARPB_TX_MODERATE ATM ON ATM.TX_ID =  TRX.TX_ID
--LEFT JOIN CDW_ODS.ADMIN.ZC_TX_ACTION_TYPE ZTT ON ZTT.TX_ACTION_TYPE_C = TRX.ACTION_TYPE_C
LEFT JOIN CDW_ODS.ADMIN.CLARITY_EPM EPM ON EPM.PAYOR_ID = TRX.PAYOR_ID 
LEFT JOIN CDW_ODS.ADMIN.CLARITY_RMC RMC ON RMC.REMIT_CODE_ID = TRX.DENIAL_CODE
LEFT JOIN CDW_ODS.ADMIN.CLARITY_EMP EMP ON EMP.USER_ID  =  TRX.ACTION_USER_ID
--LEFT JOIN CDW_ODS.ADMIN.ZC_MTCH_DIST_SRC ZMS ON ZMS.MTCH_TX_HX_DIST_C = TRX.ACTION_HX_SOURCE_C
LEFT JOIN CDW_ODS.ADMIN.CLARITY_SER SER ON SER.PROV_ID  = ARP.SERV_PROVIDER_ID
LEFT JOIN CDW_ODS.ADMIN.REFERRAL_SOURCE REF ON REF.REFERRING_PROV_ID = ATM.REFERRAL_PROV_ID
LEFT JOIN CDW_ODS.ADMIN.CLARITY_EPM EPM1 ON EPM1.PAYOR_ID = ARP.ORIGINAL_EPM_ID
LEFT JOIN CDW_ODS.ADMIN.ZC_FINANCIAL_CLASS ZFC ON ZFC.FINANCIAL_CLASS =  ARP.ORIGINAL_FC_C
LEFT JOIN CDW_ODS.ADMIN.CLARITY_POS POS ON POS.POS_ID =  ARP.FACILITY_ID
LEFT JOIN CDW_ODS.ADMIN.CLARITY_DEP DEP ON DEP.DEPARTMENT_ID =  ARP.DEPARTMENT_ID
LEFT JOIN CDW_ODS.ADMIN.CLARITY_POS POS1 ON POS1.POS_ID =  ARP.POS_ID
LEFT JOIN CDW_ODS.ADMIN.CLARITY_LOC LOC ON LOC.LOC_ID =  ARP.LOC_ID
LEFT JOIN CDW_ODS.ADMIN.CLARITY_SA SA ON SA.SERV_AREA_ID =  ARP.SERVICE_AREA_ID
LEFT JOIN CDW_ODS.ADMIN.CLARITY_EPP EPP ON EPP.BENEFIT_PLAN_ID = ATM.COVERAGE_PLAN_ID
LEFT JOIN CDW_ODS.ADMIN.CLARITY_EAP EAP ON EAP.PROC_ID = TRX.PROC_ID
LEFT JOIN CDW_ODS.ADMIN.CLARITY_SER SER1 ON SER1.PROV_ID  = ARP.BILLING_PROV_ID
LEFT JOIN CDW_ODS.ADMIN.CLARITY_DEP GL ON GL.DEPARTMENT_ID = ETR.DEPT_ID
where 
ETR.TRAN_TYPE = 2 
AND
TRX.POST_DATE between add_months(date_trunc('month',current_date),-24) and add_months(last_day(current_date),-1)
and
TRX.DENIAL_CODE in ($(vRemitID))
and
TRX.ACTION_AMOUNT > 0
and
ARP.LOC_ID
in (1016,1029,1038,1030,1033,1020,1017,1018,1026)
;