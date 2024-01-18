select
    anesthesia_encounter_link.anes_key,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.age_years,
    anesthesia_encounter_link.anes_id,
    date(anesthesia_encounter_link.anes_start_tm) as event_date,
    anesthesia_encounter_link.anes_start_tm as anesthesia_start_time,
    anesthesia_encounter_link.anes_proc_name as procedure_name,
    master_date.c_yyyy as calendar_year,
    master_date.f_yyyy as fiscal_year,
    master_date.fy_yyyy_qtr as fiscal_quarter,
    stg_encounter.visit_key,
    stg_encounter.pat_key
from  {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
    inner join {{ ref('ctis_registry')}} as ctis_registry
        on ctis_registry.pat_key = anesthesia_encounter_link.pat_key
    inner join {{ ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = anesthesia_encounter_link.visit_key
    inner join {{source('cdw', 'master_date')}} as master_date
        on master_date.full_dt = date(anesthesia_encounter_link.anes_start_tm)
where
	lower(anesthesia_encounter_link.anes_proc_name) like '%mehta cast%'
	and anesthesia_encounter_link.anes_start_tm is not null
