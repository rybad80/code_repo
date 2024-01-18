with cl_bev as (
    select
        cl_bev.ept_csn as pat_enc_csn_id,
        cl_bev.linked_event_id as event_id,
        clarity_adt.event_time as event_time,
        clarity_adt.next_out_event_id,
        cl_bev_events.status_c,
        cl_bev_events.instant_tm as event_action_dt_tm,
        clarity_adt.department_id
    from
        {{source('clarity_ods','cl_bev')}} as cl_bev
        left join {{source('clarity_ods','cl_bev_events')}} as cl_bev_events
            on cl_bev_events.record_id = cl_bev.record_id
        left join {{source('clarity_ods','clarity_adt')}} as clarity_adt
            on cl_bev.linked_event_id = clarity_adt.event_id
        where cl_bev_events.instant_tm >= '2017-07-01'
        and cl_bev.ept_csn is not null
        and cl_bev.linked_event_id is not null
        and cl_bev_events.status_c in (1, 3, 5)
        and cancel_event_tm is null

    union all

    select
        hl_req_info.req_start_appt_pat_enc_csn_id as pat_enc_csn_id,
        clarity_adt.event_id,
        clarity_adt.event_time,
        clarity_adt.next_out_event_id,
        hl_asgn_info_audit.status_c,
        hl_asgn_info_audit.event_local_dttm as event_action_dt_tm,
        clarity_adt.department_id
    from
        {{source('clarity_ods','hl_req_info')}} as hl_req_info
        left join {{source('clarity_ods','hl_asgn_info_audit')}} as hl_asgn_info_audit
            on hl_asgn_info_audit.hlr_id = hl_req_info.hlr_id
        left join {{source('clarity_ods','clarity_adt')}} as clarity_adt
            on hl_req_info.req_event_id = clarity_adt.event_id
        where clarity_adt.pat_enc_csn_id is not null
        and clarity_adt.event_id  is not null
        and clarity_adt.pat_enc_csn_id is not null
        and hl_asgn_info_audit.status_c in (0, 25, 35)
        and hl_req_info.req_cancel_rsn_c is null
        and hl_req_info.req_task_subtype_c in (2, 3)
),

clean as (
    select
        pat_enc_csn_id,
        next_out_event_id,
        event_id,
        max(case
                when fact_department_rollup.intended_use_dept_grp_abbr = 'ED' then 'ED'
                else 'Non-ED' end
        ) as drill_down,
        max(case
            when status_c in (1, 0)
            then event_time end
        ) as clean_request_date,
        max(case
            when status_c in (3, 25)
            then event_action_dt_tm end
        ) as clean_in_progress_date,
        max(case
            when status_c in (5, 35)
            then event_action_dt_tm end
        ) as clean_date
    from
        cl_bev
        left join {{source('cdw_analytics','fact_department_rollup')}} as fact_department_rollup
            on fact_department_rollup.dept_id = cl_bev.department_id
            and fact_department_rollup.dept_align_dt = date(cl_bev.event_time)
    group by
        event_id,
        pat_enc_csn_id,
        next_out_event_id
),

metrics as (
    select
        lookup_capacity_metrics.start_date,
        lookup_capacity_metrics.end_date,
        lookup_capacity_metrics.drill_down,
        max(
            case
                when
                    lower(
                        lookup_capacity_metrics.metric_name
                    ) = 'clean request to clean room'
                then lookup_capacity_metrics.target end
        ) as request_to_clean_target,
        max(
            case
                when
                    lower(
                        lookup_capacity_metrics.metric_name
                    ) = 'clean request to in progress'
                then lookup_capacity_metrics.target end
        ) as request_to_in_progress_target,
        max(
            case
                when
                    lower(
                        lookup_capacity_metrics.metric_name
                    ) = 'in progress to room clean'
                then lookup_capacity_metrics.target end
        ) as in_progress_to_clean_target
    from
        {{ref('lookup_capacity_metrics')}} as lookup_capacity_metrics
    where
        lower(lookup_capacity_metrics.metric_name) like '%clean%'
    group by
        lookup_capacity_metrics.start_date,
        lookup_capacity_metrics.end_date,
        lookup_capacity_metrics.drill_down
)

select
    adt_bed.visit_key,
    adt_bed.visit_event_key,
    adt_bed.dept_key,
    adt_bed.bed_key,
    adt_bed.bed_name,
    adt_bed.department_name,
    adt_bed.department_group_name,
    fact_department_rollup.loc_dept_grp_abbr as location_group_name,
    adt_bed.bed_care_group,
    adt_bed.department_center_abbr,
    clean.clean_request_date,
    clean.clean_in_progress_date,
    clean.clean_date,
    case
        when clarity_adt.department_id = 10292012 then 'ED' else 'Non-ED'
    end as department_metric_drill_down,
    case
        when
            clean.clean_in_progress_date >= clean.clean_request_date
        then
            extract( --noqa: PRS
                epoch from clean.clean_in_progress_date - clean.clean_request_date
            ) / 60.0
    end as clean_request_to_in_progress_mins,
    case
        when
            clean.clean_date >= clean.clean_in_progress_date
        then
            extract( --noqa: PRS
                epoch from clean.clean_date - clean.clean_in_progress_date
            ) / 60.0
    end as clean_in_progess_to_clean_mins,
    case
        when
            clean.clean_date >= clean.clean_request_date
        then
            extract( --noqa: PRS
                epoch from clean.clean_date - clean.clean_request_date
            ) / 60.0
    end as clean_request_to_clean_mins,
    case
        when
            clean_request_to_in_progress_mins <= request_to_in_progress_target
        then
            1
        when
            clean_request_to_in_progress_mins > request_to_in_progress_target
        then
            0
    end as clean_request_to_in_progress_target_ind,
    case
        when
            clean_in_progess_to_clean_mins <= in_progress_to_clean_target
        then 1
        when
            clean_in_progess_to_clean_mins > in_progress_to_clean_target
        then
             0
    end as clean_in_progress_to_clean_target_ind,
    case
        when
            clean_request_to_clean_mins <= request_to_clean_target
        then
            1
        when
            clean_request_to_clean_mins > request_to_clean_target
        then
            0
    end as clean_request_to_clean_target_ind
from clean
inner join {{source('clarity_ods','clarity_adt')}} as clarity_adt
    on clarity_adt.next_out_event_id = clean.event_id
inner join {{source('cdw','visit_event')}} as visit_event
    on visit_event.adt_event_id = clarity_adt.event_id
inner join {{ref('adt_bed')}} as adt_bed
    on adt_bed.visit_event_key = visit_event.visit_event_key
inner join {{source('cdw_analytics', 'fact_department_rollup')}} as fact_department_rollup
    on fact_department_rollup.dept_id = clarity_adt.department_id
    and fact_department_rollup.dept_align_dt = date(visit_event.eff_event_dt)
inner join metrics
    on metrics.drill_down = clean.drill_down
        and clean.clean_request_date >= metrics.start_date
        and clean.clean_request_date <= metrics.end_date
