-- get raw adt events at visit_event_key level
with adt_raw as (
    select
        adt_department.pat_key,
        adt_department.visit_key,
        adt_department.visit_event_key,
        adt_department.all_department_order as visit_event_seq,
        adt_department.dept_key,
        adt_department.department_name,
        adt_department.department_group_name,
        adt_department.bed_care_group,
        case
            when lower(adt_department.department_center_abbr) like '%kop%' then 'KOPH'
            else 'PHL'
        end as campus_name,
        adt_department.hospital_admit_date,
        adt_department.enter_date,
        adt_department.exit_date_or_current_date
    from
        {{ ref('adt_department') }} as adt_department
    where
        adt_department.hospital_admit_date >= '2017-01-01'
        and adt_department.bed_care_group is not null
),

-- determine points where a patient switches bed_care_group
adt_events_w_resets as (
    select
        adt_raw.*,
        lag(adt_raw.bed_care_group, 1, adt_raw.bed_care_group) over (
            partition by adt_raw.visit_key
            order by adt_raw.enter_date
        ) as prev_bed_care_group,
        case
            when adt_raw.bed_care_group != prev_bed_care_group then 1
            else 0
        end as reset_ind
    from
        adt_raw
),

-- add grouper at bed_care_group level
adt_bed_care_grp_seq as (
    select
        adt_events_w_resets.pat_key,
        adt_events_w_resets.visit_key,
        sum(adt_events_w_resets.reset_ind) over (
            partition by adt_events_w_resets.visit_key
            order by adt_events_w_resets.visit_event_seq asc rows unbounded preceding
        ) as bed_care_change_seq,
        adt_events_w_resets.visit_event_key,
        adt_events_w_resets.visit_event_seq,
        adt_events_w_resets.dept_key,
        adt_events_w_resets.department_name,
        adt_events_w_resets.department_group_name,
        adt_events_w_resets.bed_care_group,
        adt_events_w_resets.campus_name,
        adt_events_w_resets.hospital_admit_date,
        adt_events_w_resets.enter_date,
        adt_events_w_resets.exit_date_or_current_date
    from
        adt_events_w_resets
),

adt_last_dept_in_bed_grp as (
    select
        inner_qry.*
    from (
            select
                adt_bed_care_grp_seq.pat_key,
                adt_bed_care_grp_seq.visit_key,
                adt_bed_care_grp_seq.bed_care_change_seq,
                adt_bed_care_grp_seq.department_group_name,
                adt_bed_care_grp_seq.dept_key as final_dept_key,
                adt_bed_care_grp_seq.department_name as final_department_name,
                row_number() over (
                    partition by adt_bed_care_grp_seq.pat_key,
                        adt_bed_care_grp_seq.visit_key,
                        adt_bed_care_grp_seq.bed_care_change_seq,
                        adt_bed_care_grp_seq.department_group_name
                    order by adt_bed_care_grp_seq.enter_date desc
                ) as dept_seq
            from
                adt_bed_care_grp_seq
        ) as inner_qry
    where
        inner_qry.dept_seq = 1
)

select
    adt_bed_care_grp_seq.*,
    adt_last_dept_in_bed_grp.final_dept_key,
    adt_last_dept_in_bed_grp.final_department_name
from
    adt_bed_care_grp_seq
    inner join adt_last_dept_in_bed_grp
        on adt_bed_care_grp_seq.pat_key = adt_last_dept_in_bed_grp.pat_key
        and adt_bed_care_grp_seq.visit_key = adt_last_dept_in_bed_grp.visit_key
        and adt_bed_care_grp_seq.bed_care_change_seq = adt_last_dept_in_bed_grp.bed_care_change_seq
        and adt_bed_care_grp_seq.department_group_name = adt_last_dept_in_bed_grp.department_group_name
