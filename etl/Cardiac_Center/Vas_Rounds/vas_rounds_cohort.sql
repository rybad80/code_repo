select stg_encounter.patient_name,
stg_encounter.mrn,
stg_encounter.pat_key,
stg_encounter.visit_key,
stg_encounter.csn as csn_number,
--department_group_name as department,
case when date(adt.enter_date) = cal.full_dt then 1 else 0 end as entered_today_ind,
case when date(adt.exit_date) = cal.full_dt then 1 else 0 end as exited_today_ind,
adt.enter_date,
adt.exit_date,
cal.full_dt as date_of_review,
stg_encounter.hospital_admit_date,
stg_encounter.hospital_discharge_date,
--stg_encounter.csn,
row_number() over (partition by stg_encounter.visit_key order by cal.full_dt) as review_day_order
from {{ ref('adt_department_group') }} as adt
inner join
   {{source('cdw', 'master_date')}} as cal on
        cal.full_dt >= date(adt.enter_date) and cal.full_dt <= date(coalesce(adt.exit_date, current_date))
and cal.full_dt >= '2020-06-11' and cal.full_dt <= current_date
inner join {{ ref('stg_encounter') }} as stg_encounter on adt.visit_key = stg_encounter.visit_key
where stg_encounter.hospital_admit_date >= '2019-01-01'
and   adt.enter_date >= '2019-01-01'
and adt.department_group_name like '%CICU%'
and entered_today_ind = 0
