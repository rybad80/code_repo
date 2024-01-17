{{ config(
	materialized='table',
	dist='visit_key',
	meta={
		'critical': true
	}
) }}

with gps_master_stage as (
    select
		stg_gps_healthcloud.mrn,
		stg_gps_healthcloud.pat_key,
        stg_gps_healthcloud.pat_id,
		stg_gps_healthcloud.first_schedule_dt,
		stg_gps_healthcloud.enroll_date,
		stg_gps_healthcloud.disenroll_date,
		stg_gps_healthcloud.country,
		stg_gps_healthcloud.payment_source
    from
        {{ref('stg_gps_healthcloud')}} as stg_gps_healthcloud
),

/*after applying criteria based on where the dates were pulled from,
a few duplicates still exist because of bad dates in salesforce
-the following CTE allows us to remove these duplicates from the analysis*/

same_disenroll_stage as (
	select
		gps_master_stage.*,
		row_number() over(
			partition by
				gps_master_stage.pat_key,
				gps_master_stage.disenroll_date
			order by
				gps_master_stage.enroll_date desc
		) as row_order
	from
		gps_master_stage
)

select distinct
	same_disenroll_stage.mrn,
	same_disenroll_stage.pat_key,
	same_disenroll_stage.pat_id,
	stg_encounter.patient_key,
	stg_encounter.encounter_key,
    stg_encounter.visit_key,
    1 as global_patient_services_ind
from
    {{ref('stg_encounter')}} as stg_encounter
	inner join same_disenroll_stage
        on stg_encounter.pat_key = same_disenroll_stage.pat_key
where
    same_disenroll_stage.row_order = 1
    and (stg_encounter.encounter_date between same_disenroll_stage.enroll_date
            and coalesce(same_disenroll_stage.disenroll_date, current_date + interval '10 year')
		or (stg_encounter.encounter_date >= same_disenroll_stage.first_schedule_dt))
