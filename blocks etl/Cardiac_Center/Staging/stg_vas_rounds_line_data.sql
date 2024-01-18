with
max_date as (
            select
                flowsheet_lda_group.fs_key,
                flowsheet_lda_group.seq_num,
                max(flowsheet_lda_group.contact_dt_key) as max_date
		from {{source('cdw', 'flowsheet_lda_group')}} as flowsheet_lda_group
		group by 1, 2
),
line_level as (
select
	cohort.patient_name,
	cohort.mrn,
	cohort.visit_key,
	case when lda_type.src_id in (1, 2) then 'Central'
        when (lda_type.src_id = 3 and  patient_lda.lda_desc like '%Midline Non-Central%') then 'Midline'
		when (lda_type.src_id = 3 and  patient_lda.lda_desc not like '%Midline Non-Central%') then 'PIV'
		when (lda_type.src_id = 8) then 'Arterial'
		else 'Chest Tube' end as line_cat,
	case when line_cat = 'PIV' then 1 else 0 end as line_peripheral,
	patient_lda.pat_lda_key,
	date(patient_lda.place_dt)  as date_place_dt,
   date(patient_lda.remove_dt) as date_remove_dt,
	patient_lda.place_dt,
	case when to_char(patient_lda.remove_dt, 'yyyy') = '2157' then
null else patient_lda.remove_dt end as remove_dt,
	case when to_char(patient_lda.remove_dt, 'yyyy') = '2157'
         then extract(epoch from ((current_date) - patient_lda.place_dt)) / 60 / 60 / 24.00
         else extract(epoch from (remove_dt - place_dt)) / 60 / 60 / 24.00 end as line_days,
    case
        when lower(patient_lda.lda_desc) like '%uvc%' or lower(patient_lda.lda_desc) like '%uac%' then 1 else 0
    end as line_uvc_uac,
	case when lower(patient_lda.lda_desc) like '%uvc%' and line_days > 14 then 1
         when lower(patient_lda.lda_desc) like '%uac%' and line_days > 7 then 1
         when
             lower(
                 patient_lda.lda_desc
             ) not like '%uvc%' and lower(patient_lda.lda_desc) not like '%uac%' then null
         else 0 end as line_uvc_uac_exceed,
	patient_lda.lda_desc,
	patient_lda.lda_disp,
	case when upper(patient_lda.lda_desc) like '%INTRACARDIAC%' then 1 else 0 end as ic_ind,
	patient_lda.lda_id,
	patient_lda.lda_site,
    case when upper(patient_lda.lda_desc) like '%SINGLE LUMEN%' then 1
         when upper(patient_lda.lda_desc) like '%DOUBLE LUMEN%' then 2
         when upper(patient_lda.lda_desc) like '%TRIPLE LUMEN%' then 3
         when line_cat = 'PIV' then 1
         when line_cat = 'Arterial' then 1
         else null end as num_lumens,
    case when date(remove_dt) < date(current_date) - 1 then 1 else 0 end as removed_2_days_ago_ind,
	case when adt.department_group_name in ('ORmain', 'COIC') then 1 else 0 end as placed_in_or
from {{ ref('stg_vas_rounds_cohort_visits') }} as cohort
	inner join {{source('cdw', 'patient_lda')}} as patient_lda on cohort.pat_key = patient_lda.pat_key
	inner join
	{{source('cdw', 'flowsheet_lda_group')}} as flowsheet_lda_group on
	patient_lda.fs_key = flowsheet_lda_group.fs_key
	inner join
		max_date on flowsheet_lda_group.fs_key = max_date.fs_key
			and flowsheet_lda_group.seq_num = max_date.seq_num
			and flowsheet_lda_group.contact_dt_key = max_date.max_date
	inner join {{source('cdw', 'cdw_dictionary')}} as lda_type on
	flowsheet_lda_group.dict_lda_type_key = lda_type.dict_key
	left join   {{ ref('adt_department_group') }} as adt on cohort.visit_key = adt.visit_key
	and patient_lda.place_dt >= adt.enter_date and patient_lda.place_dt < adt.exit_date
where
  date(patient_lda.place_dt) >= date(cohort.hospital_admit_date)
  and patient_lda.place_dt <= coalesce(cohort.hospital_discharge_date, current_date)
    and (lda_type.src_id in (1, 2, 3, 8))   --CVC, PICC, Arterial, PIVPLACE_DT, /chesttube
	and lda_type.src_id != 5 --airway
),
lda_fs_vals_raw as (
	select
		current_lines.pat_lda_key,
		f.fs_id,
		f.disp_nm,
		fm.rec_dt,
		meas_val,
		case
            when
                fs_id in (
                    40010031, 40000394, 40068158, 40068159, 40068157, 40068154, 40068150, 40068149
                ) then cast(date(date('12/31/1840') + meas_val_num) as varchar(20))
              when
                  fs_id in (
                      40002818, 40002857, 40010022, 40010015, 40072331, 40010058, 40000361
                  ) and meas_cmt is not null then meas_val || ' - ' || meas_cmt
else fm.meas_val
end as meas_val_use,
		fm.meas_val_num,
		dense_rank() over (partition by current_lines.pat_lda_key, f.fs_id order by fm.rec_dt desc)
			as rank_by_fs
	from line_level as current_lines
		inner join {{source('cdw', 'visit_stay_info_rows')}} as vsr on vsr.pat_lda_key = current_lines.pat_lda_key
		inner join {{source('cdw', 'flowsheet_record')}}  as fr on fr.vsi_key = vsr.vsi_key
		inner join {{source('cdw', 'flowsheet_measure')}} as fm on fm.fs_rec_key = fr.fs_rec_key
		inner join {{source('cdw', 'flowsheet')}} as f on f.fs_key = fm.fs_key

	where
		vsr.seq_num = fm.occurance
and f.fs_id in (40000356, --abx coating
                   40000394, --dressing due
                   40068158, --first lumen cap change due
                   40068154, --second lumen cap change due
                   40068149, --third lumen cap change due
                   10997, --insertion assistive device 
                   40002818, --removal reason
                      40002857, --removal reason
                   40010022, --removal reason
                   40010015, --removal reason
                   40072331, --removal reason
                   40010058, --removal reason
                   40000361 --removal reason
                   )
),
lda_fs_vals_1 as (
select
lda_fs_vals_raw.pat_lda_key,
max(case when lda_fs_vals_raw.fs_id = 40000356 then lda_fs_vals_raw.meas_val_use else null end) as abx,
max(
    case when lda_fs_vals_raw.fs_id = 40000394  then lda_fs_vals_raw.meas_val_use else null end
) as date_next_dressing_change_due,
max(
    case when lda_fs_vals_raw.fs_id = 10997 then lda_fs_vals_raw.meas_val_use else null end
) as insertion_assitive_device,
max(
    case
        when
            lda_fs_vals_raw.fs_id in (
                40002818, 40002857, 40010022, 40010015, 40072331, 40010058, 40000361
            ) then lda_fs_vals_raw.meas_val_use
        else null
    end
) as removal_reason
from lda_fs_vals_raw
where lda_fs_vals_raw.rank_by_fs = 1
group by lda_fs_vals_raw.pat_lda_key --noqa
),
lda_fs_vals_2 as (
select
lda_fs_vals_raw.pat_lda_key,
max(
    case when lda_fs_vals_raw.fs_id = 40068158 then lda_fs_vals_raw.meas_val_use else null end
) as date_next_first_lumen_cap_change_due,
max(
    case when lda_fs_vals_raw.fs_id = 40068154 then lda_fs_vals_raw.meas_val_use else null end
) as date_next_second_lumen_cap_change_due,
max(
    case when lda_fs_vals_raw.fs_id = 40068149 then lda_fs_vals_raw.meas_val_use else null end
) as date_next_third_lumen_cap_change_due
from lda_fs_vals_raw
where lda_fs_vals_raw.rank_by_fs = 1
group by lda_fs_vals_raw.pat_lda_key --noqa
)
select line_level.patient_name,
line_level.mrn,
line_level.visit_key,
line_level.line_cat,
line_level.line_peripheral,
line_level.pat_lda_key,
line_level.date_place_dt,
line_level.date_remove_dt,
line_level.place_dt,
line_level.remove_dt,
line_level.line_days,
line_level.line_uvc_uac,
line_level.line_uvc_uac_exceed,
line_level.lda_disp,
line_level.ic_ind,
line_level.lda_id,
line_level.lda_site,
line_level.num_lumens,
line_level.removed_2_days_ago_ind,
line_level.placed_in_or,
row_number() over (partition by line_level.visit_key order by line_level.place_dt) as line_num,
-- line number partitioned by each active status, used for line numbering system in redcap
row_number() over (
    partition by line_level.visit_key, line_level.removed_2_days_ago_ind order by place_dt
) as line_num_per_actv_sts,
case when lda_fs_vals_1.abx = 'Yes' then 1
     when lda_fs_vals_1.abx = 'No' then 0
     else null end as abx,
case when lda_fs_vals_1.insertion_assitive_device = 'Ultrasound' then 1 else 0 end as line_placed_via_us,
lda_fs_vals_1.date_next_dressing_change_due,
lda_fs_vals_2.date_next_first_lumen_cap_change_due,
lda_fs_vals_2.date_next_second_lumen_cap_change_due,
lda_fs_vals_2.date_next_third_lumen_cap_change_due,
lda_fs_vals_1.removal_reason,
line_level.lda_desc as line
from line_level
left join lda_fs_vals_1 on line_level.pat_lda_key = lda_fs_vals_1.pat_lda_key
left join lda_fs_vals_2 on line_level.pat_lda_key = lda_fs_vals_2.pat_lda_key
