{{ config(meta = {
    'critical': true
}) }}

with all_cancelled_slots as (
    select
        stg_encounter.prov_key,
        stg_encounter.dept_key,
        stg_department_all.specialty_name,
        stg_department_all.department_name,
        provider.full_nm as provider_name,
        stg_encounter.eff_dt,
        stg_encounter.appointment_date,
        lead(stg_encounter.appointment_date, 1)
            over (partition by stg_encounter.prov_key, stg_encounter.dept_key, stg_encounter.eff_dt
                    order by stg_encounter.appointment_date, stg_encounter.scheduled_length_min,
                    stg_encounter.appointment_cancel_date) as next_dt,
        case
            when next_dt is null --indicates latest appointment for prov/dept/day
                then stg_encounter.scheduled_length_min
            when (stg_encounter.appointment_date
                    + cast(stg_encounter.scheduled_length_min || ' minute' as interval)) <= next_dt
                                        --indicates no overlap with next appointment
                then stg_encounter.scheduled_length_min
            when stg_encounter.appointment_date = next_dt --indicates same start time as next appointment
                then 0
            else extract(hour from (next_dt - stg_encounter.appointment_date)) * 60
                + extract(minute from (next_dt - stg_encounter.appointment_date))
                --else condition is met if appt_end_dt > next_dt, which indicates overlap with next appointment
        end as cancelled_minutes,
        case
            when stg_encounter.cancel_24hr_ind = 1 then '24hr'
            when stg_encounter.cancel_48hr_ind = 1 then '48hr'
            else 'no_flag' end as late_cancel_flag
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key
        inner join {{ref('stg_department_all')}} as stg_department_all
            on stg_department_all.dept_key = stg_encounter.dept_key
        inner join {{source('cdw', 'dim_visit_cncl_rsn')}} as dim_visit_cncl_rsn
            on stg_encounter.dim_visit_cncl_rsn_key = dim_visit_cncl_rsn.dim_visit_cncl_rsn_key
        left join {{source('cdw', 'provider_unavailable_day')}} as provider_unavailable_day
            on stg_encounter.prov_key = provider_unavailable_day.prov_key
            and stg_encounter.dept_key = provider_unavailable_day.dept_key
            and stg_encounter.eff_dt = provider_unavailable_day.unavailable_dt
        left join {{source('cdw', 'provider_unavailable_time')}} as provider_unavailable_time
            on stg_encounter.prov_key = provider_unavailable_time.prov_key
            and stg_encounter.dept_key = provider_unavailable_time.dept_key
            and stg_encounter.appointment_date = provider_unavailable_time.unavailable_start_tm
    where
        stg_encounter.appointment_status_id = '3' --cancelled
        and stg_encounter.encounter_type_id in ('50', '101') --appt or office stg_encounter
        and stg_encounter.patient_class_id in ('0', '2', '5', '6') --N/A, OP, Obsv, Recurr OP
        and lower(provider.prov_type) is not null
        and lower(provider_name) not like '%provider%'
        and lower(provider_name) not like '%nurse%'
        and lower(provider_name) not like '%study%'
        and lower(provider_name) not like '% prov'
        and lower(provider_name) not like '% room'
        and lower(provider_name) not like '% clinic'
        and lower(provider_name) not like '% shot'
        and coalesce(provider_unavailable_day.dim_unavailable_reason_key, 0) = 0
                                        --day does not have an unavailable reason
        and coalesce(provider_unavailable_time.dim_unavailable_reason_key, 0) = 0
                                        --slot time does not have an unavailable reason
        and dim_visit_cncl_rsn.visit_cncl_rsn_id not in (107, 142)
                                        --provider/department request, provider ill
        and stg_department_all.specialty_name != 'UNKNOWN'
        and stg_encounter.eff_dt between to_date('2016-07-01', 'yyyy-mm-dd') and current_date + 14
)

select
    row_number() over (order by prov_key, dept_key, appointment_date) as canc_slot_num,
    prov_key,
    specialty_name,
    department_name,
    provider_name,
    dept_key,
    eff_dt,
    appointment_date as canc_appt_dt,
    appointment_date + cast(cancelled_minutes || ' minute' as interval) as canc_slot_end_dt,
    cancelled_minutes,
    late_cancel_flag,
    1 as denominator
from
    all_cancelled_slots
where cancelled_minutes > 0
