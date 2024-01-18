select
    fact_periop.log_id,
    fact_periop.log_key,
    orc_turnover.sched_gap as scheduled_gap,
    orc_turnover.sched_room_in_dt, -- pre_room_out_dt + setup + any sched_gap should = sched_room_in_dt
    orc_turnover.pre_sched_room_out_dt,
    orc_turnover.sched_room_out_to_in,
    orc_turnover.room_in_dt,
    orc_turnover.pre_room_out_dt,
    extract(epoch from orc_turnover.room_in_dt - orc_turnover.pre_room_out_dt) / 60 as room_out_to_in,
    case when (extract(epoch from orc_turnover.room_in_dt - orc_turnover.pre_room_out_dt) / 60) > 60
           then null else extract(epoch from orc_turnover.room_in_dt - orc_turnover.pre_room_out_dt) / 60
           end as room_out_to_in_adj,
    rsn.or_adj_turnovr_rsn_title as room_out_to_in_adj_rsn_nm,
    pre_orl.log_id as pre_log_id,
    case
        when pre_orc.add_on_case_ind = 0 and pre_orc.add_on_case_sch_ind = 0 then 0 else 1
    end as pre_add_on_case_sch_yn,
    pre_surg.full_nm as pre_primary_surgeon,
    pre_serv.dict_nm as pre_service_nm,
    pre_proc.or_proc_nm as pre_primary_proc_nm,
    orc_turnover.pre_num_procs,
    orc_turnover.pre_num_pnls,
    or_case.setup_offset as setup,
    pre_orc.clnup_offset as cleanup,
    -- usually reported at the weekly level
    (date(current_date) + (2 - extract(dow from current_date))) - 7 as prev_wk_start,
    case when fact_periop.service = pre_service_nm then 1 else 0 end as same_service_ind,
	--usually want to **exclude** cases w/ a scheduled gap over 1 hour from ant TAT calculation
	case when orc_turnover.sched_gap > 60 then 1 else 0 end as long_sched_gap_ind

from {{ ref('fact_periop') }} as fact_periop
    inner join {{ source('cdw', 'or_case') }} as or_case
        on or_case.log_key = fact_periop.log_key
    inner join {{ source('cdw', 'or_case_room_turnover') }} as orc_turnover
    -- contains info on or cases & info about the previous case in the same room.
    -- also contains columns that determine the scheduled and actual turnover length between the cases
        on orc_turnover.log_key = fact_periop.log_key
    -- will give info on the case immidiately preceeding the current case on the same day in the same room**
    inner join {{ source('cdw', 'or_log') }} as pre_orl
        on orc_turnover.pre_log_key = pre_orl.log_key
    inner join
        {{ source('cdw', 'or_case') }} as pre_orc
            on orc_turnover.pre_case_key = pre_orc.or_case_key
    inner join
        {{ source('cdw', 'provider') }} as pre_surg
            on pre_surg.prov_key = orc_turnover.pre_pri_prov_key
    inner join
        {{ source('cdw', 'or_procedure') }} as pre_proc
            on pre_proc.or_proc_key = orc_turnover.pre_pri_proc_key
    inner join
        {{ source('cdw', 'master_date') }} as master_date
            on master_date.dt_key  = orc_turnover.mstr_proc_dt_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as pre_serv            on
            orc_turnover.pre_dict_clarity_or_service = pre_serv.dict_key
    inner join
        {{ source('cdw', 'dim_or_adj_turnovr_rsn') }} as rsn         on
            orc_turnover.dim_or_adj_turnovr_rsn_key = rsn.dim_or_adj_turnovr_rsn_key

where
    master_date.weekday_ind = 1 --no TAT measured on weekends
    and orc_turnover.room_in_dt > orc_turnover.pre_room_out_dt
