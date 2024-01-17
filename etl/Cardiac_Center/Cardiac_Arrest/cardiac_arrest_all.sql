with surg_timestamp as (
    select
        surgery_encounter_timestamps.log_id,
        surgery_encounter_timestamps.log_key,
        surgery_encounter_timestamps.visit_key,
        surgery_encounter_timestamps.in_room_date,
        surgery_encounter_timestamps.out_room_date
    from
        {{ref('surgery_encounter_timestamps')}} as surgery_encounter_timestamps
),

surgical_arrests as(
 	select 
 		cardiac_unit_encounter.pat_key,
 		cardiac_unit_encounter.department_admit_date,
 		stg_cardiac_pc4_arrest.r_card_arrest_strt_dt,
  		max(case when (r_card_arrest_strt_dt >= adt_department_group.enter_date
                and r_card_arrest_strt_dt <= adt_department_group.exit_date
                and adt_department_group.department_group_name in ('PERIOP COMPLEX', 'CARDIAC O/IC'))
            or (r_card_arrest_strt_dt >= surg_timestamp.in_room_date
                and r_card_arrest_strt_dt <= surg_timestamp.out_room_date)
            or stg_cardiac_pc4_arrest.card_arrest_venue != 'CICU'
            then 1
            else 0 end) as surg_arrest_pre_ind
	from 
		{{ref('cardiac_unit_encounter')}} as cardiac_unit_encounter
		inner join {{ref('stg_cardiac_pc4_arrest')}} as stg_cardiac_pc4_arrest
            on stg_cardiac_pc4_arrest.r_enc_key = cardiac_unit_encounter.enc_key
		left join {{ref('adt_department_group')}} as adt_department_group
            on adt_department_group.visit_key = cardiac_unit_encounter.visit_key
        left join surg_timestamp
            on surg_timestamp.visit_key = cardiac_unit_encounter.visit_key
   group by
		cardiac_unit_encounter.pat_key,
 		cardiac_unit_encounter.department_admit_date,
 		stg_cardiac_pc4_arrest.r_card_arrest_strt_dt
 ),
arrests_all as (
    select
        {{
        dbt_utils.surrogate_key([
            'cardiac_unit_encounter.pat_key',
            'cardiac_unit_encounter.department_admit_date',
            'stg_cardiac_pc4_arrest.r_card_arrest_strt_dt'
            ])
        }} as arrest_key,
        cardiac_unit_encounter.enc_key, --as r_enc_key
        cardiac_unit_encounter.visit_key,
        cardiac_unit_encounter.hospital_admit_date,
        cardiac_unit_encounter.hospital_discharge_date,
        cardiac_unit_encounter.pat_key,
        cardiac_unit_encounter.mrn,
        cardiac_unit_encounter.patient_name,
        cardiac_unit_encounter.dob,
        cardiac_unit_encounter.department_admit_date as r_cicu_start_date,
        stg_cardiac_pc4_arrest.r_card_arrest_strt_dt as arrest_date,
        date(arrest_date) - date(cardiac_unit_encounter.dob) as arrest_age_days,
        case when arrest_age_days <= 30
            then 1
            else 0 end as neonate_ind,
        case when arrest_age_days > 30
            then 1
            else 0 end as non_neonate_ind,
        lag(arrest_date) over(
            partition by cardiac_unit_encounter.pat_key order by arrest_date
        ) as prev_arrest_date,
        round(
            extract(epoch from (arrest_date - prev_arrest_date)) / 3600.0, 0
        ) as arrest_diff_hr,
        stg_cardiac_pc4_mortality.r_mort_dt as death_date,
        round(
            extract(epoch from (stg_cardiac_pc4_mortality.r_mort_dt - arrest_date)) / 3600.0, 0
        ) as arrest_to_death_hr,
        case when date(stg_cardiac_pc4_mortality.r_mort_dt) >= date(cardiac_unit_encounter.hospital_admit_date)
            and date(stg_cardiac_pc4_mortality.r_mort_dt) <= date(cardiac_unit_encounter.hospital_discharge_date)
            then 1
            else 0
        end as visit_death_ind,
        surgical_arrests.surg_arrest_pre_ind,
		case when stg_cardiac_pc4_arrest.card_arrest_venue = 'CICU'
            then 0
            else surg_arrest_pre_ind
        end as surg_arrest_ind

    from
        {{ref('cardiac_unit_encounter')}} as cardiac_unit_encounter
        inner join {{ref('stg_cardiac_pc4_arrest')}} as stg_cardiac_pc4_arrest
            on stg_cardiac_pc4_arrest.r_enc_key = cardiac_unit_encounter.enc_key
        inner join {{ref('stg_cardiac_pc4_mortality')}} as stg_cardiac_pc4_mortality
            on stg_cardiac_pc4_mortality.pat_key = cardiac_unit_encounter.pat_key
        left join surgical_arrests
    	    on surgical_arrests.pat_key = cardiac_unit_encounter.pat_key
    	    and surgical_arrests.department_admit_date = cardiac_unit_encounter.department_admit_date
    	    and surgical_arrests.r_card_arrest_strt_dt = stg_cardiac_pc4_arrest.r_card_arrest_strt_dt
    where
        cardiac_unit_encounter.department_admit_date >= '01-01-2015'
        and lower(cardiac_unit_encounter.department_name) = 'cicu'
)

select
    arrests_all.arrest_key,
    arrests_all.visit_key,
    arrests_all.enc_key, --r_enc_key
    arrests_all.pat_key,
    arrests_all.mrn,
    arrests_all.dob,
    arrests_all.patient_name,
    arrests_all.r_cicu_start_date as cicu_start_date,
    arrests_all.hospital_admit_date,
    arrests_all.hospital_discharge_date,
    arrests_all.arrest_date,
    arrests_all.arrest_age_days,
    arrests_all.neonate_ind,
    arrests_all.non_neonate_ind,
    extract(
        epoch from (arrests_all.arrest_date - lag(arrests_all.arrest_date) over(order by arrests_all.arrest_date))
    ) / 86400.0 as arrest_diff_days,
    arrests_all.death_date,
    arrests_all.arrest_to_death_hr,
    case when arrests_all.arrest_to_death_hr <= 24
        then 1
        else 0 end as arrest_mortality_ind,
    arrests_all.visit_death_ind
from
    arrests_all
where
    coalesce(arrests_all.arrest_diff_hr, 7)  > 6
    and surg_arrest_ind = 0
