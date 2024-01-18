SELECT 
TO_CHAR(post_date,'YYYY-MM-DD') AS POST_MONTH
,TDL.LOC_ID
,Sum(case when TDL.active_ar_amount > 0 then  TDL.active_ar_amount else 0 end) as Total_Debit_AR 
,Sum(case when TDL.active_ar_amount < 0 then  TDL.active_ar_amount else 0 end) as Total_Credit_AR
,Sum(case when tdl.active_ar_AMOUNT <> 0 then  TDL.active_ar_amount else 0 end) as Total_Outstanding_AR
,Sum(case when  (TDL.post_date - tdl.ORIG_Post_DATE)>90 AND TDL.active_ar_amount > 0 then  TDL.active_ar_amount else 0 end) as Total_Billed_AR_GT_90
,Sum(case when  (TDL.post_date - tdl.ORIG_Post_DATE)>365 AND TDL.active_ar_amount > 0 then  TDL.active_ar_amount else 0 end) as Total_Billed_AR_GT_365
FROM Clarity_TDL_Age TDL
LEFT JOIN CLARITY_DEP DEP ON TDL.DEPT_ID = DEP.DEPARTMENT_ID 
WHERE 
TDL.LOC_ID IN (1016,1017,1018,1020,1026,1029,1030,1033,1038) 
AND post_date between add_months(trunc(sysdate, 'month'), -14) and add_months(trunc(sysdate, 'month'),0)-1 
GROUP BY TO_CHAR(post_date,'YYYY-MM-DD'),TDL.LOC_ID
;