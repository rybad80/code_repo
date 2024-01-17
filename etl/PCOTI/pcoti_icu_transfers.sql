with locations as (
    select
        stg_pcoti_event_adt_grouped.*,
        lag(stg_pcoti_event_adt_grouped.event_type_abbrev, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as prev_event_type_abbrev,
        lag(stg_pcoti_event_adt_grouped.dept_key, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as prev_dept_key,
        lag(stg_pcoti_event_adt_grouped.department_name, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as prev_department_name,
        lag(stg_pcoti_event_adt_grouped.department_group_name, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as prev_department_group_name,
        lag(stg_pcoti_event_adt_grouped.bed_care_group, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as prev_bed_care_group,
        lag(stg_pcoti_event_adt_grouped.campus_name, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as prev_campus_name,

        lead(stg_pcoti_event_adt_grouped.event_type_abbrev, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as next_event_type_abbrev,
        lead(stg_pcoti_event_adt_grouped.dept_key, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as next_dept_key,
        lead(stg_pcoti_event_adt_grouped.department_name, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as next_department_name,
        lead(stg_pcoti_event_adt_grouped.department_group_name, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as next_department_group_name,
        lead(stg_pcoti_event_adt_grouped.bed_care_group, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as next_bed_care_group,
        lead(stg_pcoti_event_adt_grouped.campus_name, 1) over (
            partition by stg_pcoti_event_adt_grouped.pat_key, stg_pcoti_event_adt_grouped.visit_key
            order by stg_pcoti_event_adt_grouped.event_start_date
        ) as next_campus_name
    from
        {{ ref('stg_pcoti_event_adt_grouped') }} as stg_pcoti_event_adt_grouped
),

icu_transfers as (
    select
        locations.*
    from
        locations
    where
        locations.event_type_abbrev in (
            'LOC_ICU_PICU',
            'LOC_ICU_NICU',
            'LOC_ICU_CICU'
        )
),

redcap_catcode as (
    select
        pat_key,
        visit_key,
        event_type_name,
        event_type_abbrev,
        event_start_date,
        event_end_date
    from
        {{ ref('stg_pcoti_event_redcap') }}
    where
        event_type_abbrev in (
            'REDCAP_CAT_CALL',
            'REDCAP_CODE_OTHER',
            'REDCAP_CODE_ARC',
            'REDCAP_CODE_ARC_CPA',
            'REDCAP_CODE_CPA'
        )
),

other_events as (
    select * from {{ ref('stg_pcoti_event_intubation') }}
    union all
    select * from {{ ref('stg_pcoti_event_vasopressor') }}
    union all
    select * from {{ ref('stg_pcoti_event_surgery') }}
    union all
    select * from redcap_catcode
),

icu_transfers_augmented as (
    select
        icu_transfers.pat_key,
        icu_transfers.visit_key,
        'ICU Emergent Transfer' as event_type_name,
        'ICU_EMERG_XFER' as event_type_abbrev,
        icu_transfers.event_type_name as icu_event_type_name,
        icu_transfers.event_type_abbrev as icu_event_type_abbrev,
        icu_transfers.department_group_name as to_department_group_name,
        icu_transfers.bed_care_group as to_bed_care_group,
        icu_transfers.campus_name as to_campus_name,
        icu_transfers.event_start_date as icu_enter_date,
        icu_transfers.event_end_date as icu_exit_date,
        (date_part('epoch', icu_transfers.event_end_date)
            - date_part('epoch', icu_transfers.event_start_date)) / 3600::float as hrs_in_icu,
        icu_transfers.prev_event_type_abbrev,
        icu_transfers.prev_dept_key as from_dept_key,
        icu_transfers.prev_department_name as from_department_name,
        icu_transfers.prev_department_group_name as from_department_group_name,
        icu_transfers.prev_bed_care_group as from_bed_care_group,
        icu_transfers.prev_campus_name as from_campus_name,
        icu_transfers.next_event_type_abbrev,
        icu_transfers.next_dept_key,
        icu_transfers.next_department_name,
        icu_transfers.next_department_group_name,
        icu_transfers.next_bed_care_group,
        icu_transfers.next_campus_name,
        other_events.event_type_abbrev as other_event_type_abbrev,
        other_events.event_start_date as other_event_start_date,
        other_events.event_end_date as other_event_end_date,
        (date_part('epoch', other_events.event_start_date)
            - date_part('epoch', icu_transfers.event_start_date))
            / 3600::float as hrs_from_event_start_to_icu_enter,
        (date_part('epoch', other_events.event_end_date)
            - date_part('epoch', icu_transfers.event_start_date)) / 3600::float as hrs_from_event_end_to_icu_enter
    from
        icu_transfers
        left join other_events
            on icu_transfers.pat_key = other_events.pat_key
            and icu_transfers.visit_key = other_events.visit_key
),

create_indicators as (
    select
        *,
        case
            when prev_event_type_abbrev = 'LOC_FLOOR' then 1
            else 0
        end as transfer_from_floor_ind,
        case
            when hrs_in_icu >= 2 then 1
            else 0
        end as gte_2hrs_in_icu_ind,
        case
            when other_event_type_abbrev in ('SURG_PROC', 'SURG_ANES')
            and hrs_from_event_end_to_icu_enter >= -4
            and hrs_from_event_end_to_icu_enter <= 0.5
            then 1
            else 0
        end as surg_proc_ind,
        case
            when other_event_type_abbrev = 'INTUB'
            and hrs_from_event_start_to_icu_enter >= -1
            and hrs_from_event_start_to_icu_enter <= 1
            then 1
            else 0
        end as intubation_pre1hr_post1hr_ind,
        case
            when other_event_type_abbrev like 'VASOPRESSOR%'
            and hrs_from_event_start_to_icu_enter >= -1
            and hrs_from_event_start_to_icu_enter <= 1
            then 1
            else 0
        end as vasopressor_pre1hr_post1hr_ind,
        case
            when other_event_type_abbrev = 'FLUID_BOLUS_GT60'
            and hrs_from_event_start_to_icu_enter >= -1
            and hrs_from_event_start_to_icu_enter <= 1
            then 1
            else 0
        end as fluid_bolus_pre1hr_post1hr_ind,
        case
            when other_event_type_abbrev in (
                'REDCAP_CAT_CALL',
                'REDCAP_CODE_OTHER',
                'REDCAP_CODE_ARC',
                'REDCAP_CODE_ARC_CPA',
                'REDCAP_CODE_CPA'
            )
            and hrs_from_event_start_to_icu_enter >= -24
            and hrs_from_event_start_to_icu_enter <= 0
            then 1
            else 0
        end as cat_code_pre24hr_ind
    from
        icu_transfers_augmented
),

summarize_indicators as (
    select
        pat_key,
        visit_key,
        event_type_name,
        event_type_abbrev,
        to_department_group_name,
        to_bed_care_group,
        to_campus_name,
        icu_enter_date,
        icu_exit_date,
        prev_event_type_abbrev,
        from_dept_key,
        from_department_name,
        from_department_group_name,
        from_bed_care_group,
        from_campus_name,
        next_event_type_abbrev,
        next_dept_key,
        next_department_name,
        next_department_group_name,
        next_bed_care_group,
        next_campus_name,
        max(transfer_from_floor_ind) as transfer_from_floor_ind,
        max(gte_2hrs_in_icu_ind) as gte_2hrs_in_icu_ind,
        max(surg_proc_ind) as surg_proc_ind,
        max(intubation_pre1hr_post1hr_ind) as intubation_pre1hr_post1hr_ind,
        max(vasopressor_pre1hr_post1hr_ind) as vasopressor_pre1hr_post1hr_ind,
        max(fluid_bolus_pre1hr_post1hr_ind) as fluid_bolus_pre1hr_post1hr_ind,
        max(cat_code_pre24hr_ind) as cat_code_pre24hr_ind,
        max(
            case when surg_proc_ind = 1 then other_event_end_date end
        ) as surg_last_end_date,
        min(
            case when intubation_pre1hr_post1hr_ind = 1 then other_event_start_date end
        ) as intubation_first_start_date,
        min(
            case when vasopressor_pre1hr_post1hr_ind = 1 then other_event_start_date end
        ) as vasopressor_first_start_date,
        min(
            case when fluid_bolus_pre1hr_post1hr_ind = 1 then other_event_start_date end
        ) as fluid_bolus_gt60_date,
        min(
            case when cat_code_pre24hr_ind = 1 then other_event_start_date end
        ) as cat_code_first_date
    from
        create_indicators
    group by
        pat_key,
        visit_key,
        event_type_name,
        event_type_abbrev,
        icu_event_type_name,
        icu_event_type_abbrev,
        to_department_group_name,
        to_bed_care_group,
        to_campus_name,
        icu_enter_date,
        icu_exit_date,
        prev_event_type_abbrev,
        from_dept_key,
        from_department_name,
        from_department_group_name,
        from_bed_care_group,
        from_campus_name,
        next_event_type_abbrev,
        next_dept_key,
        next_department_name,
        next_department_group_name,
        next_bed_care_group,
        next_campus_name
),

final_indicators as (
    select
        pat_key,
        visit_key,
        event_type_name,
        event_type_abbrev,
        to_department_group_name,
        to_bed_care_group,
        to_campus_name,
        -- remove one second from icu_enter_date so transfer comes before ADT event
        -- when episode events are viewed chronologically
        icu_enter_date - interval '1 second' as icu_enter_date,
        icu_exit_date,
        prev_event_type_abbrev,
        from_dept_key,
        from_department_name,
        from_department_group_name,
        from_bed_care_group,
        from_campus_name,
        next_event_type_abbrev,
        next_dept_key,
        next_department_name,
        next_department_group_name,
        next_bed_care_group,
        next_campus_name,
        transfer_from_floor_ind,
        gte_2hrs_in_icu_ind,
        surg_proc_ind,
        intubation_pre1hr_post1hr_ind,
        vasopressor_pre1hr_post1hr_ind,
        fluid_bolus_pre1hr_post1hr_ind,
        cat_code_pre24hr_ind,
        surg_last_end_date,
        intubation_first_start_date,
        vasopressor_first_start_date,
        fluid_bolus_gt60_date,
        cat_code_first_date,
        case
            when cat_code_pre24hr_ind = 1
            or (
                transfer_from_floor_ind = 1
                and gte_2hrs_in_icu_ind = 1
                and surg_proc_ind = 0
            ) then 1
            else 0
        end as unplanned_transfer_ind,
        case
            when transfer_from_floor_ind = 1
            and surg_proc_ind = 0
            and intubation_pre1hr_post1hr_ind
                + vasopressor_pre1hr_post1hr_ind
                + fluid_bolus_pre1hr_post1hr_ind > 0
            then 1
            else 0
        end as emergent_transfer_ind,
        case
            when
                emergent_transfer_ind = 1 and unplanned_transfer_ind = 1
                then 'ICU Transfer - Emergent & Unplanned'
            when emergent_transfer_ind = 1 and unplanned_transfer_ind = 0 then 'ICU Transfer - Emergent'
            when emergent_transfer_ind = 0 and unplanned_transfer_ind = 1 then 'ICU Transfer - Unplanned'
            else 'ICU Transfer - Other'
        end as icu_xfer_event_type_name,
        case
            when emergent_transfer_ind = 1 and unplanned_transfer_ind = 1 then 'ICU_XFER_EMERGENT_UNPLANNED'
            when emergent_transfer_ind = 1 and unplanned_transfer_ind = 0 then 'ICU_XFER_EMERGENT'
            when emergent_transfer_ind = 0 and unplanned_transfer_ind = 1 then 'ICU_XFER_UNPLANNED'
            else 'ICU_XFER_OTHER'
        end as icu_xfer_event_type_abbrev
    from
        summarize_indicators
),

icu_final as (
select
    {{ dbt_utils.surrogate_key([
        'final_indicators.pat_key',
        'final_indicators.visit_key',
        'final_indicators.icu_xfer_event_type_abbrev',
        'final_indicators.icu_enter_date'
    ]) }} as episode_event_key,
    {{ dbt_utils.surrogate_key([
        'final_indicators.pat_key',
        'final_indicators.visit_key'
    ]) }} as episode_key,
    final_indicators.pat_key,
    final_indicators.visit_key,
    final_indicators.icu_xfer_event_type_name,
    final_indicators.icu_xfer_event_type_abbrev,
    final_indicators.to_department_group_name,
    final_indicators.to_bed_care_group,
    final_indicators.to_campus_name,
    -- remove one second from icu_enter_date so transfer comes before ADT event
    -- when episode events are viewed chronologically
    final_indicators.icu_enter_date - interval '1 second' as icu_enter_date,
    final_indicators.icu_exit_date,
    final_indicators.prev_event_type_abbrev,
    final_indicators.from_dept_key,
    final_indicators.from_department_name,
    final_indicators.from_department_group_name,
    final_indicators.from_bed_care_group,
    final_indicators.from_campus_name,
    final_indicators.next_event_type_abbrev,
    final_indicators.next_dept_key,
    final_indicators.next_department_name,
    final_indicators.next_department_group_name,
    final_indicators.next_bed_care_group,
    final_indicators.next_campus_name,
    final_indicators.transfer_from_floor_ind,
    final_indicators.gte_2hrs_in_icu_ind,
    final_indicators.surg_proc_ind,
    final_indicators.intubation_pre1hr_post1hr_ind,
    final_indicators.vasopressor_pre1hr_post1hr_ind,
    final_indicators.fluid_bolus_pre1hr_post1hr_ind,
    final_indicators.cat_code_pre24hr_ind,
    final_indicators.surg_last_end_date,
    final_indicators.intubation_first_start_date,
    final_indicators.vasopressor_first_start_date,
    final_indicators.fluid_bolus_gt60_date,
    final_indicators.cat_code_first_date,
    final_indicators.unplanned_transfer_ind,
    final_indicators.emergent_transfer_ind
from
    final_indicators
),

last_ip_service_before_icu as (
    select
        icu_final.episode_event_key,
        max(
            case
                when adt_service.service = 'NOT APPLICABLE' then null
                else adt_service.service
            end
        ) as from_ip_service_name
    from
        icu_final
        left join {{ ref('adt_service')}} as adt_service
            on icu_final.pat_key = adt_service.pat_key
            and icu_final.visit_key = adt_service.visit_key
            and icu_final.icu_enter_date - interval '5 minutes' >= adt_service.service_start_datetime
            and icu_final.icu_enter_date - interval '5 minutes' <= adt_service.service_end_datetime
    group by
        icu_final.episode_event_key
)

select
    icu_final.*,
    last_ip_service_before_icu.from_ip_service_name
from
    icu_final
    left join last_ip_service_before_icu
        on icu_final.episode_event_key = last_ip_service_before_icu.episode_event_key
