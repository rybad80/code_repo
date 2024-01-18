SELECT 
tdl.PERIOD
,tdl.account_id
,tdl.PAT_ID
,tdl.TX_ID
,tdl.TX_NUM
,tdl.VISIT_NUMBER
,tdl.active_ar_amount
,tdl.cur_fin_class
,tdl.cur_payor_id
,tdl.cur_plan_id
,tdl.cpt_code
,tdl.DETAIL_TYPE
,last_day(TDL.post_date) as POST_MONTH
,tdl.ORIG_POST_DATE
,tdl.POST_DATE
--,tdl.MATCH_TRX_ID
,tdl.PROC_ID
,ceap.PROC_CODE
,ceap.PROC_NAME
,tdl.LOC_ID
,cloc.LOC_NAME
,tdl.DEPT_ID
,dep.GL_PREFIX as COST_CNTR_ID
FROM Clarity_TDL_Age TDL
LEFT OUTER JOIN clarity_loc cloc ON TDL.loc_id=cloc.loc_id
LEFT JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = tdl.dept_id
LEFT OUTER JOIN clarity_eap ceap ON TDL.PROC_ID=ceap.PROC_ID
WHERE 
TDL.LOC_ID IN (1016,1017,1018,1020,1026,1029,1030,1033,1038) 
AND TDL.active_ar_amount < 0
AND post_date between add_months(trunc(current_date,'month'),-15) and add_months(last_day(current_date),-1)
;