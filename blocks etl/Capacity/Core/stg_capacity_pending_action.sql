with buassigns as (
    select
            pend_id,
            department_id,
            lag(department_id) over (partition by pend_id order by line) as last_unit,
            case
                when
                    coalesce(department_id, '0') != lag(coalesce(department_id, '0'))
                        over (partition by pend_id order by line)
                then
                    update_inst
            end as unit_change_time,
            case
                when
                    coalesce(bed_id, '0') != lag(coalesce(bed_id, '0'))
                        over (partition by pend_id order by line)
                then update_inst
            end as bed_change_time,
            case
                when
                    pend_req_stat_c = 4 --'Approved'
                then
                    update_inst
            end as approved_time,
            case
                when
                    pend_req_stat_c = 7 --'Bed Ready'
                then
                    update_inst
            end as bed_ready_time
        from
            {{source('clarity_ods','bed_plan_hx')}}
        where
            linked_event_id is null
),

assignmenttimes as (
    select
        buassigns.*/* w department eliminations  */
    from
        buassigns
        inner join {{ref('stg_department_all')}} as dep
            on buassigns.department_id = dep.department_id
            and dep.department_id not in
                (101001060,  /* BED MANAGEMENT CENT* */
                 101003001,  /* KOPH EMERGENCY DEP */
                 10292012,   /* MAIN EMERGENCY DEPT */
                 101001045,  /* MAIN MRI */
                 101001042   /* BILL-PREADM TESTING */
                )
),

with_unit_bed as (/* UnitBed subquery */
    select
        assignmenttimes.pend_id,
        max(assignmenttimes.unit_change_time) as unit_assignment,
        max(assignmenttimes.bed_change_time) as bed_assignment,
         /* the latest if approved or ready more than once */
        max(assignmenttimes.approved_time) as first_approved_at,
        max(assignmenttimes.bed_ready_time) as first_bed_ready_at
    from
        assignmenttimes
    group by
        assignmenttimes.pend_id
),

unitbeddata as (
    select
        pend_action.pat_enc_csn_id,
        unit_bed.*,
        clarity_adt.department_id,
        pend_action.linked_event_id,
        dense_rank() over (partition by pend_action.linked_event_id
                            order by pend_action.pend_id desc) as order_num
    from
        {{source('clarity_ods','pend_action')}} as pend_action
         /* the Transfer Out for the pend_action */
        left join {{source('clarity_ods','clarity_adt')}} as clarity_adt
            on pend_action.linked_event_id = clarity_adt.xfer_in_event_id
        left join with_unit_bed as unit_bed
            on pend_action.pend_id = unit_bed.pend_id

),

finalunitbed as (
        select
            *
        from
            unitbeddata
        where
            order_num = 1 /*  drops off mutiple PEND_IDs for a VISIT_EVENT  */
),

unionset as (
    /* Bed Assignment */
    select
        pend_id,
        bed_assignment as event_action_dt_tm,
        'Bed Assign' as event_action_nm,
        pat_enc_csn_id,
        linked_event_id,
        department_id
    from
        finalunitbed
    where
        bed_assignment is not null
    union distinct
    /* Unit Assignment */
    select
        pend_id,
        unit_assignment as event_action_dt_tm,
        'Unit Assign' as event_action_nm,
        pat_enc_csn_id,
        linked_event_id,
        department_id
    from
        finalunitbed
    where
        unit_assignment is not null
    union distinct
    /* Bed Approved (adding Ready if Ready) */
    select
        pend_id,
        first_approved_at as event_action_dt_tm,
        'Bed Approval' as event_action_nm,
        pat_enc_csn_id,
        linked_event_id,
        department_id
    from
        finalunitbed
    where
        first_approved_at is not null
)

select
    pend_id,
    to_timestamp(timezone(event_action_dt_tm,
                'GMT', 'America/New_York'), 'YYYY-MM-DD HH24:MI:SS') as event_action_dt_tm,
    event_action_nm,
    pat_enc_csn_id,
    linked_event_id,
    department_id
from
    unionset
where
    department_id is not null
