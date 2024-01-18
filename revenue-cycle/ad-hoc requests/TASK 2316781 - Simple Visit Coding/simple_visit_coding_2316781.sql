

select 
      identity_id as mrn,
      hsp_account.hsp_account_id as account_number,
      pat_name,
      adm_date_time as admit_date,
      disch_date_time as discharge_date,
      department_name as disch_department
from  
    hsp_account
    inner join identity_id
       on hsp_account.pat_id = identity_id.pat_id
        and identity_type_id = 0
    left join hsp_account_4
       on hsp_account.hsp_account_id = hsp_account_4.hsp_account_id
    left join clarity_dep
       on clarity_dep.department_id =  hsp_account.disch_dept_id
where 
     svc_success_c = 1
     and hsp_account.hsp_account_id in 
(
9008814072,
9008851894,
9008743173,
9008766560,
9008773508,
9008784593,
9008833689,
9008842884,
9008845769,
9008857137,
9008541964,
9008802069,
9008801635,
9008826355,
9008853147,
9008858254,
9008860902,
9008845345,
9008775261,
9008823035,
9008842072,
9008759417,
9008854373,
9008757620,
9008784921,
9008796014,
9008798116,
9008821402,
9008820879,
9008845916,
9008850454,
9008757574,
9008789156,
9008823520,
9008832726,
9008844773,
9008740921,
9008796563,
9008799127,
9008843719,
9008791366,
9008822379,
9008827046,
9008787891,
9008838716,
9008763081,
9008766956,
9008832784,
9008845608,
9008798477,
9008604691,
9008773402,
9008806942,
9008826679,
9008832751,
9008774340,
9008802244,
9008802282,
9008802685,
9008829036,
9008734046,
9008807361,
9008822809,
9008826128,
9008820298,
9008759142,
9008757942,
9008801845,
9008818978,
9008743171,
9008810275,
9008785335,
9008759038,
9008810588,
9008760646,
9008686678,
9008795749,
9008800744,
9008803463,
9008801641,
9008712254,
9008791535,
9008796528,
9008781521,
9008725643,
9008790295,
9008683998,
9008781956,
9008727890,
9008777744,
9008675366)