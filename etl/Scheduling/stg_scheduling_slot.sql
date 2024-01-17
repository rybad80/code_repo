{{ config(meta = {
    'critical': true
}) }}

with office_visit_ind as (
    select
        department_id,
        max(physician_app_psych_visit_ind) as physician_app_psych_visit_ind
    from
        {{ ref('stg_office_visit_grouper') }}
    group by
        department_id
),
stage as (
    select
        schedule.slot_start_tm as slot_start_time,
        schedule.prov_key,
        schedule.dept_key,
        cast(schedule.slot_start_tm as date) as encounter_date,
        schedule.slot_lgth_min as slot_length_minute,
        schedule.appt_lgth_min as appt_lenth_minute,
        stg_department_all.revenue_location_group,
        stg_department_all.specialty_name,
        stg_department_all.department_name,
        stg_department_all.intended_use_name,
        cast(stg_department_all.department_id as bigint) as department_id,
        initcap(provider.full_nm) as provider_name,
        provider.prov_type as provider_type,
        provider.prov_id as provider_id,
        case
            when
                (
                lower(provider.prov_type) in ('physician',
                                                'midwife',
                                                'nurse practitioner',
                                                'physician assistant')
                or lower(provider.prov_type) like '%psycholog%'
                )
                and office_visit_ind.physician_app_psych_visit_ind = 1
            then 1
            else 0
        end as physician_app_psych_ind,
        case
            when lower(stg_department_all.specialty_name) in (
                'physical therapy',
                'speech',
                'audiology',
                'occupational therapy',
                'clinical nutrition'
            )
            then 1
            else 0
        end as ancillary_services_ind,
        case
            when lower(schedule.appt_stat) = 'canceled' then 0
            else schedule.csn
        end as csn,
        schedule.pat_key,
        case
            when lower(schedule.appt_stat) = 'canceled' then 0
            else schedule.visit_key
        end as visit_key,
        master_visit_type.visit_type_nm as visit_type,
        master_visit_type.visit_type_id,
        case
            when upper(schedule.appt_stat) = 'CANCELED' then 'NO APPOINTMENT'
            when upper(schedule.appt_stat) = 'DEFAULT' then 'NO APPOINTMENT'
            else appt_stat
        end as slot_appointment_status,
        case
            when upper(schedule.appt_stat) = 'CANCELED' then -2
            when upper(schedule.appt_stat) = 'DEFAULT' then -2
            else dict_appt_stat.src_id
        end as slot_appointment_status_id,
        schedule.block as slot_appointment_block,
        case
            when schedule.slot_status_num in (1, 6) then 'UNAVAILABLE'
            when schedule.slot_status_num in (4, 5, 7, 8) then 'OPEN'
            when schedule.slot_status_num in (2, 3) then 'SCHEDULED'
            else 'UNKNOWN'
        end as slot_status,
        case
            when schedule.slot_status_num in (1, 6)
                then upper(dict_slot_unavail_rsn.dict_nm) else null
        end as unavailable_reason,
        case
            when schedule.slot_status_num = 5
                then upper(dict_tm_held_rsn.dict_nm) else null
        end as hold_reason,
        case
            when schedule.slot_status_num in (1, 6)
                then 'UNAVAIL: ' || upper(dict_slot_unavail_rsn.dict_nm)
            when schedule.slot_status_num = 5
                then 'OPEN: ' || upper(dict_tm_held_rsn.dict_nm)
            when schedule.slot_status_num = 4
                then 'OPEN: ' || upper(schedule.appt_stat)
            when schedule.slot_status_num in (7, 8)
                then 'OPEN: No Appointment'
            when schedule.slot_status_num = 2
                then 'SCHED: ' || upper(master_visit_type.visit_type_nm)
            when schedule.slot_status_num = 3
                then 'SCHED: ' || upper(schedule.block)
            else null
        end as slot_status_detail,
        case
            when schedule.slot_status_num in (1, 5, 6) then 1
            else 0 end
        as unavailable_hold_ind,
        schedule.slot_day_unavail_ind as day_unavailable_ind,
        schedule.slot_tm_unavail_ind as time_unavailable_ind,
        case when schedule.slot_status_num not in (1, 5, 6) then 1 else 0 end as available_ind,
        case when schedule.slot_status_num in (4, 5, 7, 8) then 1 else 0 end as open_ind,
        case when schedule.slot_status_num in (2, 3) then 1 else 0 end as scheduled_ind,
        case when department_care_network.specialty_care_ind = 1 then 1 else 0 end as specialty_care_slot_ind,
        case when department_care_network.primary_care_ind = 1 then 1 else 0 end as primary_care_slot_ind,
        case
            when date_part('hour', schedule.slot_start_tm) < 8 then 1 else 0
        end as early_appointment_ind,
        case when date_part('hour', schedule.slot_start_tm) >= 16
                and department_care_network.department_id not in (
                                                        62,
                                                        101022016,
                                                        101001076,
                                                        101003033,
                                                        101012070
                                                        )
            then 1
            else 0 end
        as evening_appointment_ind,
        case
        when date_part('dow', schedule.slot_start_tm) = 1 then 1
        when date_part('dow', schedule.slot_start_tm) = 7 then 1 else 0
        end as weekend_appointment_ind,
        stg_fill_rate_ind_lookup.fill_rate_incl_ind,
        case
            when lower(provider.full_nm) like 'nurse flu, provider%'
                then 1
            when ( lower(provider.prov_type) != 'resource'
                and lower(provider.full_nm) not like '%psychologist%'
                and lower(provider.full_nm) not like '%provider%'
                and lower(provider.full_nm) not like '%nurse%'
                and lower(provider.full_nm) not like '%study%'
                and lower(provider.full_nm) not like '% prov'
                and lower(provider.full_nm) not like '% room'
                and lower(provider.full_nm) not like '% clinic'
                and lower(provider.full_nm) not like '% shot'
                and lower(provider.full_nm) not like '% lab'
                and lower(provider.full_nm) not like 'transplant%'
                and lower(provider.full_nm) not like '% other'
                and stg_department_all.department_id not in (
                    92138043,
                    84187043,
                    1012107,
                    89476031,
                    101001089,
                    10270011,
                    101012136
                    )
                ) then 1
            else 0
        end as provider_ind,
        case
            when lower(provider.prov_type) = 'resource' then 1
            else 0
        end as resource_ind,
        case
            when lower(provider.prov_type) = 'resource'
                and lower(stg_department_all.specialty_name) = 'radiology'
                and lookup_provider_resources.radiology_ind = 1 then 1
            else 0
        end as radiology_resource_ind,
        schedule.slot_status_num,
        row_number() over (
            partition by
                schedule.dept_key,
                provider.full_nm,
                schedule.slot_start_tm
            order by
                slot_status_num,
                schedule.visit_key,
                schedule.create_dt,
                schedule.prov_key
        ) as line
    from
        {{ ref('stg_scheduling_base') }} as schedule
        inner join {{ ref('stg_fill_rate_ind_lookup') }} as stg_fill_rate_ind_lookup
            on stg_fill_rate_ind_lookup.dept_key = schedule.dept_key
            and stg_fill_rate_ind_lookup.prov_key = schedule.prov_key
            and coalesce(stg_fill_rate_ind_lookup.appt_block_key, '-99')
                = coalesce(schedule.appt_block_key, '-99')
        inner join {{ref('stg_department_all')}} as stg_department_all
            on stg_department_all.dept_key = schedule.dept_key
        inner join {{ source('cdw', 'provider') }} as provider
            on provider.prov_key = schedule.prov_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_slot_unavail_rsn
            on schedule.dict_slot_unavail_rsn_key = dict_slot_unavail_rsn.dict_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_tm_held_rsn
            on schedule.dict_tm_held_rsn_key = dict_tm_held_rsn.dict_key
        left join {{ref('department_care_network')}} as department_care_network
            on department_care_network.dept_key = schedule.dept_key
        left join {{ ref('lookup_provider_resources') }} as lookup_provider_resources
            on provider.prov_id = lookup_provider_resources.prov_id
        left join {{source('cdw', 'cdw_dictionary')}} as dict_appt_stat
            on schedule.dict_appt_stat_key = dict_appt_stat.dict_key
        left join {{ source('cdw', 'master_visit_type') }} as master_visit_type
            on schedule.appt_visit_type_key = master_visit_type.visit_type_key
        left join office_visit_ind as office_visit_ind
            on stg_department_all.department_id = office_visit_ind.department_id
    where
        (resource_ind = 1 or provider_ind = 1)
)
select
    *
from
    stage
where
    line = 1
