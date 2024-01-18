with avail as (

select
case
    when
        util_block.dict_snapshot_num_key = 643806 then 1
    /*643806 = avail time, 643805 = sched util, 643807 = performed util */
		when util_block.dict_snapshot_num_key = 643807 then 2
		when util_block.dict_snapshot_num_key = 643805 then 3
		else null end as snapshot_num,
to_char(to_date(util_block.snapshot_dt_key, 'YYYYMMDD'), 'YYYY-MM-DD') as date, --noqa: L029
loc.loc_nm,
room.full_nm as room,
case when lower(room) = 'cardiac add on room' then 'CARDIAC'
	when lower(room) = 'chop or nicu' then 'NICU'
	when lower(room) = 'chop or picu' then 'PICU'
	when lower(room) = 'chop or er' then 'ER'
	when lower(room) like '%bucks%' then 'BUCKS DAY SURGERY'
	when lower(room) like '%exton%' then 'EXTON DAY SURGERY'
	when lower(room) like '%voorhees%' then 'VOORHEES DAY SURGERY'
	when lower(room) in ('chop kop or #1', 'chop kop or #2') then 'KING OF PRUSSIA DAY SURGERY'
	when
        lower(
            room
        ) in (
            'chop koph or #01', 'chop koph or #02', 'chop koph or #03', 'chop koph or #04'
        ) then 'KING OF PRUSSIA HOSPITAL'
	when lower(room) like '%bwv%' then 'BRANDYWINE VALLEY DAY SURGERY'
	when lower(room) like '%chop or #%' then 'PERIOP COMPLEX'
	when lower(room) like '%chop add on room%' then 'PERIOP COMPLEX'
	when lower(room) like '%chop or procedure rm #%' then 'PERIOP COMPLEX'
	when lower(room) like '%c section%' then 'SDU'
	when lower(room) like '%fetal%' then 'SDU'
	when lower(room) like '%pacu%' then 'PACU'
	when lower(room) like '%cardiac%' then 'CARDIAC'
	else null end as adj_loc,
dict_svc.dict_nm as service,
--,util_block.slot_type
to_char(util_block.slot_strt_dt, 'HH24:MI:SS') as avail_slot_start,
to_char(util_block.slot_end_dt, 'HH24:MI:SS') as avail_slot_end,
util_block.slot_lgth_min as avail_time

from {{ source('cdw', 'or_utilization_block') }} as util_block
inner join {{ source('cdw', 'provider') }} as room on room.prov_key = util_block.room_prov_key
inner join {{ source('cdw', 'or_log') }} as orl on orl.log_key = util_block.log_key
inner join {{ source('cdw', 'location') }} as loc on loc.loc_key = orl.loc_key
inner join {{ source('cdw', 'cdw_dictionary') }} as dict_svc on dict_svc.dict_key = util_block.dict_or_svc_key

where
snapshot_num = 1 --available time
and lower(slot_type) = 'block' --available time
and snapshot_dt_key > 20130501 --after optime implementation
and util_block.cur_rec_ind = 1 --CDW doesnt delete any rows, this is needed to match clarity data

),

cal as (
select
full_dt as dt,
month_nm,
day_nm,
row_number() over (partition by cy_yyyy_mm_nm, day_nm order by c_day) as day_instance

from {{ source('cdw', 'master_date') }}

)

select
cal.*,
avail.*

from avail
inner join cal on cal.dt = avail.date
