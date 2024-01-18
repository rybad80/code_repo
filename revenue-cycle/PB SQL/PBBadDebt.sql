SELECT
tdl.period
,TDL.tx_id
,TDL.account_id
,last_day(TDL.post_date) as POST_MONTH
,TDL.post_date
,TDL.loc_id
,cloc.Loc_name "Revenue Location"
,ceap.proc_code
,ceap.proc_name
,cdep.department_name "Department Name"
,cdep.specialty
,TDL.amount
,TDL.ACTIVE_AR_AMOUNT
,csa.serv_area_name
,act.city
,act.state_c
,zs.name "State"
,act.zip
,act.fin_class_c "Primary Fin Class"
--,zfc.name "Primary Fin Class Name"
,tdl.detail_type
,dep.GL_PREFIX as COST_CNTR_ID
FROM CDW_ODS.ADMIN.CLARITY_TDL_TRAN tdl
LEFT JOIN CDW_ODS.ADMIN.CLARITY_DEP DEP ON DEP.DEPARTMENT_ID = tdl.DEPT_ID
LEFT OUTER JOIN CDW_ODS.ADMIN.CLARITY_EAP ceap ON TDL.proc_id=ceap.proc_id
LEFT OUTER JOIN CDW_ODS.ADMIN.CLARITY_LOC cloc ON TDL.loc_id=cloc.loc_id
LEFT OUTER JOIN CDW_ODS.ADMIN.CLARITY_DEP cdep ON TDL.dept_id=cdep.department_id
LEFT OUTER JOIN CDW_ODS.ADMIN.CLARITY_SA csa ON csa.serv_area_id=tdl.serv_area_id
LEFT OUTER JOIN CDW_ODS.ADMIN.ACCOUNT act ON act.account_id=TDL.account_id
--LEFT OUTER JOIN ZC_FIN_CLASS zfc ON zfc.fin_class_c=act.fin_class_c
LEFT OUTER JOIN CDW_ODS.ADMIN.ZC_STATE zs ON zs.state_c=act.state_c
 
WHERE ceap.proc_code IN ('1195','1196') 
AND tdl.detail_type IN (4,13,31) 
And TDL.LOC_ID IN (1016,1017,1018,1020,1026,1029,1030,1033,1038) 
AND post_date between add_months(date_trunc('month',current_date),-15) and add_months(last_day(current_date),-1)
ORDER BY TDL.tx_id
;