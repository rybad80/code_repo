{{ config(meta = {
    'critical': true
}) }}

with cn_visits as (
    select
        visit_key,
        patient_key,
        pat_key,
    /*
        partition the patient visits by patient_key and date so if a patient sees
        more than one provider on a given date, we'll only use the first provider
    */
        row_number()
            over(
                partition by patient_key, eff_dt
                order by appointment_date desc
            )
            as visit_seq_num,
        case
            when stg_encounter.department_name like 'BROOMALL%'
                or stg_encounter.department_name like 'DREXEL HILL%'
                or stg_encounter.department_name like 'MEDIA%'
                then 'DELCO'
            when stg_encounter.department_name like 'CAPE MAY%'
                or stg_encounter.department_name like 'SOMERS PN%'
                or stg_encounter.department_name like 'SMITHVILLE%'
                then 'HARBORVIEW'
            else stg_encounter.department_name
            end as department_name,
        provider_name as full_nm,
        eff_dt as effective_date
    from
        {{ ref('stg_encounter') }} as stg_encounter
    inner join {{ ref('stg_department_all') }} as stg_department_all
        on stg_encounter.department_id = stg_department_all.department_id
    where
        los_proc_cd like '99%'
        and stg_department_all.intended_use_id = 1013
        and stg_department_all.revenue_location_group is not null
        and stg_department_all.department_name not like '%KF GEN PED%'
),

pcp_dates as (
  select
    stg_encounter.visit_key,
    stg_encounter.patient_key,
    stg_encounter.pat_key,
    coalesce(initcap(cn_visits.full_nm), 'NON-CHOP PCP') as full_nm,
    lag(
        cn_visits.department_name) over (
            partition by stg_encounter.patient_key
            order by cn_visits.effective_date
    ) as previous_department_name,
    coalesce(
        cn_visits.department_name, 'NON-CHOP PCP'
    ) as department_name,
    lead(
        cn_visits.department_name) over (
            partition by stg_encounter.patient_key
            order by cn_visits.effective_date
    ) as next_department_name,
    lag(
        cn_visits.effective_date) over (
            partition by stg_encounter.patient_key
            order by cn_visits.effective_date
    ) + 1 as previous_effective_date,
    cn_visits.effective_date,
    lead(
        cn_visits.effective_date) over (
            partition by stg_encounter.patient_key
            order by cn_visits.effective_date
    ) + 1 as next_effective_date,
    row_number() over (
        partition by stg_encounter.patient_key
        order by cn_visits.effective_date
    ) as row_num,
    case
        when row_num = 1
            or (
                row_num > 1 and cn_visits.department_name != previous_department_name
            ) then 1
        else 0
    end as start_ind,
    case
        when next_department_name is null
            or cn_visits.department_name != next_department_name then 1
        else 0
    end as end_ind,
    case when start_ind = 1 and row_num = 1 then '1900-01-01'
           when start_ind = 1 and row_num > 1 then previous_effective_date
           when end_ind = 1 and next_department_name is null then '9999-12-31'
           when end_ind = 1 and row_num > 1 then cn_visits.effective_date
           else cn_visits.effective_date
    end as date
    from
        {{ ref('stg_encounter') }} as stg_encounter
        left join cn_visits on stg_encounter.visit_key = cn_visits.visit_key
    where
        cn_visits.visit_seq_num = 1
),

start_date as (
    select
        *,
        row_number() over (
            partition by patient_key
            order by date asc
        ) as ctr
    from pcp_dates
    where start_ind = 1
),

end_date as (
    select
        *,
        row_number() over (
            partition by patient_key
            order by date asc
        ) as ctr
    from pcp_dates
    where end_ind = 1
)

select
    start_date.patient_key,
    start_date.pat_key,
    end_date.department_name as pcp_location, --most recent pcp_location
    end_date.full_nm as pcp_provider, --most recent pcp_provider
    start_date.date as start_date,
    case
        when start_date.next_department_name is null then '9999-12-31'
        when end_date.date = start_date.date then start_date.effective_date
        else end_date.date
    end as end_date
from
    start_date
    inner join end_date
        on start_date.ctr = end_date.ctr
        and start_date.patient_key = end_date.patient_key
order by
    start_date.patient_key,
    start_date.date
