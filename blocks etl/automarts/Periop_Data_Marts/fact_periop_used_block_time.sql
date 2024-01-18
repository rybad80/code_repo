with used as ( --region used time
    select
    --643806 = avail time, 643805 = sched util, 643807 = performed util
    case when util_block.dict_snapshot_num_key = 643806 then 1
            when util_block.dict_snapshot_num_key = 643807 then 2
            when util_block.dict_snapshot_num_key = 643805 then 3
            when  util_block.dict_snapshot_num_key = 643804 then 4 --room util
            else null end as snapshot_num,
    case when util_block.proc_tm_ind = 1 then 'Y'
            when util_block.proc_tm_ind = 0 then 'N'
            else null end as proc_time,
    util_block.snapshot_dt_key,
    to_char(to_date(util_block.snapshot_dt_key, 'YYYYMMDD'), 'YYYY-MM-DD') as date, --noqa: L029
    periop.log_id,
    periop.log_key,
    periop.loc,
    periop.room,
    case when lower(room) = 'cardiac add on room' then 'CARDIAC'
            when lower(room) = 'chop or nicu' then 'NICU'
            when lower(room) = 'chop or picu' then 'PICU'
            when lower(room) = 'chop or er' then 'ER'
            when lower(room) like '%bucks%' then 'BUCKS DAY SURG'
            when lower(room) like '%exton%' then 'EXTON DAY SURG'
            when lower(room) like '%voorhees%' then 'VOORHEES DAY SURG'
            when lower(room) in ('chop kop or #1', 'chop kop or #2') then 'KING OF PRUSSIA DAY SURG'
            when
                lower(
                    room
                ) in (
                    'chop koph or #01', 'chop koph or #02', 'chop koph or #03', 'chop koph or #04'
                ) then 'KING OF PRUSSIA HOSPITAL'
            when lower(room) like '%bwv%' then 'BRANDYWINE VALLEY DAY SURG'
            when lower(room) like '%chop or #%' then 'PERIOP COMPLEX'
            when lower(room) like '%chop add on room%' then 'PERIOP COMPLEX'
            when lower(room) like '%chop or procedure rm #%' then 'PERIOP COMPLEX'
            when lower(room) like '%c section%' then 'SDU'
            when lower(room) like '%fetal%' then 'SDU'
            when lower(room) like '%pacu%' then 'PACU'
            when lower(room) like '%cardiac%' then 'CARDIAC'
        else null end as adj_loc,
    dict_svc.dict_nm as block_service,
    periop.service as surg_service,
    periop.log_surgeon_primary,
    prov.full_nm as surg_block,
    util_block.slot_type,
    to_char(util_block.slot_strt_dt, 'HH24:MI:SS') as used_slot_start,
    to_char(util_block.slot_end_dt, 'HH24:MI:SS') as used_slot_end,
    case when lower(slot_type) = 'correct' and proc_time = 'Y' then slot_lgth_min else 0 end as in_block_proc_time,
    case
        when lower(slot_type) = 'correct' and proc_time = 'N' then slot_lgth_min else 0
    end as in_block_setup_cleanup_time,
    case
        when lower(slot_type) = 'overbook' and proc_time = 'Y' then slot_lgth_min else 0
    end as in_block_overbook_proc_time,
    case
        when lower(slot_type) = 'overbook' and proc_time = 'N' then slot_lgth_min else 0
    end as in_block_overbook_setup_cleanup_time,
    case
        when lower(slot_type) = 'outside' and proc_time = 'Y'
            then slot_lgth_min else 0
    end as out_block_proc_time,
    case
        when lower(slot_type) = 'outside' and proc_time = 'N' then slot_lgth_min else 0
    end as out_block_setup_cleanup_time,
    case_times.sched_case_lgth

    from {{ source('cdw', 'or_utilization_block') }} as util_block
    left join {{ ref('fact_periop') }} as periop on periop.log_key = util_block.log_key
    left join {{ ref('fact_periop_delays') }} as case_times on case_times.log_id = periop.log_id
    left join {{ source('cdw', 'provider') }} as prov on prov.prov_key = util_block.surg_prov_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as dict_svc
        on dict_svc.dict_key = util_block.dict_or_svc_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_snapshot
            on dict_snapshot.dict_key = util_block.dict_snapshot_num_key

    where
    snapshot_num = 2
    and lower(slot_type) in ('correct', 'overbook', 'outside')
    and snapshot_dt_key > 20130501 --after optime implementation
    and posted_ind = 1 --165449
    and util_block.cur_rec_ind = 1 --CDW doesnt delete any rows, this is needed to match clarity data

    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
--end region
),

    cal as ( --region calendar
    select
    full_dt as dt,
    month_nm,
    day_nm,
    row_number() over (partition by cy_yyyy_mm_nm, day_nm order by c_day) as day_instance

    from {{ source('cdw', 'master_date') }}

    --end region
),

sched as (-- region identify first scheduled date
	select
        fact_periop.log_id,
        min(or_case_audit_hist.audit_act_dt) as first_booked_dt
	from
        {{ ref('fact_periop') }} as fact_periop
        inner join {{ source('cdw', 'or_log') }} as orl on orl.log_id = fact_periop.log_id
        inner join {{ source('cdw', 'or_case') }} as orc on orc.log_key = orl.log_key
        inner join {{ source('cdw', 'or_case_audit_history') }} as or_case_audit_hist
                on or_case_audit_hist.or_case_key = orc.or_case_key
        inner join {{ source('cdw', 'dim_or_audit_action') }} as dim_or_audit_action
                on dim_or_audit_action.dim_or_audit_act_key = or_case_audit_hist.dim_or_audit_act_key
	where
		lower(or_audit_act_nm) = 'scheduled'
	group by --noqa: L054
        fact_periop.log_id
--end region
)

select
cal.dt,
cal.day_instance,
sched.first_booked_dt as sched_dt,
times.in_room,
extract(epoch from in_room - first_booked_dt) / 86400.00 as lead_time,
used.*,

/*NOTE: block util only uses in block proc time and in block setup cleanup */
sum(
    in_block_proc_time + in_block_setup_cleanup_time
) over(partition by log_surgeon_primary, block_service, used.log_key, dt order by dt) as used_time,
sum(
    in_block_setup_cleanup_time
) over(partition by log_surgeon_primary, block_service, used.log_key, dt order by dt) as setup_time,

--below values give you total time used for procedure and setup/cleanup, regardless of block status
sum(
    in_block_proc_time + in_block_overbook_proc_time + out_block_proc_time
) over(partition by log_surgeon_primary, block_service, used.log_key, dt order by dt) as used_time_total,
sum(
    in_block_setup_cleanup_time + in_block_overbook_setup_cleanup_time + out_block_setup_cleanup_time
) over(partition by log_surgeon_primary, block_service, used.log_key, dt order by dt) as setup_time_total

from
    used
    inner join cal on cal.dt = used.date
    inner join sched on sched.log_id = used.log_id
    inner join {{ ref('fact_periop_timestamps') }} as times
        on times.log_id = used.log_id
