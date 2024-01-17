with
line_days as (

select
    master_date.full_dt,
    stg_central_line.patient_name,
    stg_central_line.pat_key,
    stg_central_line.mrn
from
    {{ ref('stg_central_line') }} as stg_central_line
    inner join {{source('cdw','master_date')}} as master_date
        on master_date.full_dt between date(stg_central_line.start_date) and date(stg_central_line.end_date)
group by
    master_date.full_dt,
    stg_central_line.patient_name,
    stg_central_line.pat_key,
    stg_central_line.mrn
),

last_month_day as (

select
    line_days.pat_key,
    date_trunc('month', line_days.full_dt) as month_dt,
    max(line_days.full_dt) as last_day_of_month
from
    line_days
group by
    line_days.pat_key,
    date_trunc('month', line_days.full_dt)
),

op_ip_days as (

select
    line_days.full_dt,
    case when capacity_ip_hourly_census.census_date is not null then 1 else 0 end as ip_ind,
    line_days.patient_name,
    line_days.pat_key,
    line_days.mrn
from
    line_days
    -- Used Hourly census instead of midnight to capture days they are IP but missed midnight
    left join {{ref('capacity_ip_hourly_census')}} as capacity_ip_hourly_census
                on capacity_ip_hourly_census.pat_key = line_days.pat_key
                and capacity_ip_hourly_census.census_date = line_days.full_dt
group by
    line_days.full_dt,
    capacity_ip_hourly_census.census_date,
    line_days.patient_name,
    line_days.pat_key,
    line_days.mrn
),

month_sum as (

select
    op_ip_days.patient_name,
    op_ip_days.pat_key,
    op_ip_days.mrn,
    date_trunc('month', op_ip_days.full_dt) as month_dt,
    count(op_ip_days.full_dt) as op_line_days
from
    op_ip_days
where
    op_ip_days.ip_ind = 0
group by
    op_ip_days.patient_name,
    op_ip_days.pat_key,
    op_ip_days.mrn,
    date_trunc('month', op_ip_days.full_dt)
),

chop_pat as (

select
    encounter_all.pat_key,
    date_trunc('month', encounter_all.encounter_date) as encounter_month
from
    {{ref('encounter_all')}} as encounter_all
where
    encounter_all.encounter_date < current_date
    and encounter_all.encounter_date > '2018-05-01'
    and (
        upper(encounter_all.department_name) like '%HOME CARE%'
        or encounter_all.encounter_type_id = 160 --'Care Coordination'
        or encounter_all.specialty_care_ind = 1
        or encounter_all.inpatient_ind = 1
        )
)

select
    month_sum.patient_name,
    month_sum.pat_key,
    month_sum.mrn,
    month_sum.month_dt,
    last_month_day.last_day_of_month,
    month_sum.op_line_days
from
    month_sum
    inner join last_month_day
        on last_month_day.pat_key = month_sum.pat_key
        and last_month_day.month_dt = month_sum.month_dt
    inner join chop_pat
        on chop_pat.pat_key = month_sum.pat_key
        and chop_pat.encounter_month between
            (month_sum.month_dt - cast('2 months' as interval))
            and month_sum.month_dt
 group by
    month_sum.patient_name,
    month_sum.pat_key,
    month_sum.mrn,
    month_sum.month_dt,
    last_month_day.last_day_of_month,
    month_sum.op_line_days
