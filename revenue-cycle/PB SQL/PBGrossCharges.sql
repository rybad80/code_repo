WITH AR_summary AS 
( SELECT 
last_day(TDL.post_date) as POST_MONTH,
tdl.LOC_ID,
dep.GL_PREFIX as COST_CNTR_ID,
Sum(CASE WHEN detail_type = 1 THEN tdl.ACTIVE_AR_AMOUNT ELSE 0 end) Charges,
Sum(CASE WHEN detail_type = 10 THEN tdl.ACTIVE_AR_AMOUNT ELSE 0 end) Charge_voids,
Sum(CASE WHEN detail_type IN (2,5,11,20,22,32,33) THEN tdl.ACTIVE_AR_AMOUNT ELSE 0 end) payments,
Sum(CASE WHEN detail_type IN (3,4)  THEN tdl.ACTIVE_AR_AMOUNT ELSE 0 end) Adjustments_all,
Sum(CASE WHEN detail_type = 5 THEN tdl.ACTIVE_AR_AMOUNT ELSE 0 end) Payment_reversal,
Sum(CASE WHEN detail_type IN (12,13)  THEN tdl.ACTIVE_AR_AMOUNT ELSE 0 end) Adjustment_void,
Sum(CASE WHEN PROC_ID IN (111553,111554) AND detail_type IN (4,13,31)  THEN tdl.ACTIVE_AR_AMOUNT ELSE 0 end) Bad_debt
FROM CDW_ODS.ADMIN.CLARITY_TDL_TRAN tdl
LEFT JOIN CDW_ODS.ADMIN.CLARITY_DEP DEP ON DEP.DEPARTMENT_ID = tdl.DEPT_ID
WHERE 
 tdl.LOC_ID IN (1016,1017,1018,1020,1026,1029,1030,1033,1038)
AND post_date between add_months(date_trunc('month',current_date),-15) and add_months(last_day(current_date),-1)
GROUP BY 1,2,3
) 
SELECT  
POST_MONTH,
LOC_ID,
COST_CNTR_ID,
Charges,
Charge_voids,
(Charges + Charge_voids )*-1 AS Gross_Charges,
payments,
(Adjustments_all - Bad_debt) AS Adjustments,
Payment_reversal,
Bad_debt
FROM AR_summary
;