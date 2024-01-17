 select LOG_ID
		,PAT_MRN_ID
		,SCHED_START_DT
		,surgery_date
		,CAST(REPLACE(SUBSTRING(CAST(IN_ROOM AS VARCHAR(19)),12,5),':','') AS VARCHAR(4)) IN_ROOM
		,CAST(REPLACE(SUBSTRING(CAST(PROC_START_INCISION AS VARCHAR(19)),12,5),':','') AS VARCHAR(4))  PROC_START_INCISION
		,CAST(REPLACE(SUBSTRING(CAST(PROC_CLOSE_INCISION AS VARCHAR(19)),12,5),':','') AS VARCHAR(4))  PROC_CLOSE_INCISION
		,CAST(REPLACE(SUBSTRING(CAST(OUT_ROOM AS VARCHAR(19)),12,5),':','') AS VARCHAR(4)) OUT_ROOM
		,or_order
   from 
		(select 1,
		       or_log.log_id  
			   ,pat.pat_key
			   ,pat.pat_mrn_id
			   ,or_log.SCHED_START_DT
			   ,date(sched_start_dt) surgery_date
			   ,min(case when d_case_times.src_id = 5 then event_in_dt end) as in_room
			   ,min(case when d_case_times.src_id = 7 then event_in_dt end) as proc_start_incision
			   ,min(case when d_case_times.src_id = 8 then event_in_dt end) as proc_close_incision
			   ,min(case when d_case_times.src_id = 10 then event_in_dt end) as out_room
			   ,row_number() over (partition by pat.pat_key, date(sched_start_dt) order by sched_start_dt) or_order
			   
		from  or_case 
		       inner join or_log on or_case.log_key = or_log.log_key
			   left join patient pat on pat.pat_key = or_case.pat_key
			   join or_log_anes_staff anesstaff on anesstaff.log_key = or_log.log_key
			   join or_log_case_times ortimes on ortimes.log_key = or_log.log_key
		       join cdw_dictionary d_case_times on d_case_times.dict_key = ortimes.dict_or_pat_event_key
	group by 
	          or_log.log_id       
		      ,pat.pat_key
			  ,pat.pat_mrn_id
			  ,or_log.SCHED_START_DT
		) a		
