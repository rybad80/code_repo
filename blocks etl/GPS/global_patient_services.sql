with gps_master_stage as (
    select
		stg_gps_healthcloud.mrn,
		stg_gps_healthcloud.pat_key,
        stg_gps_healthcloud.pat_id,
		stg_gps_healthcloud.patient_enroll_date_key,
		stg_gps_healthcloud.enroll_date,
		stg_gps_healthcloud.disenroll_date,
		stg_gps_healthcloud.country,
		stg_gps_healthcloud.payment_source,
		stg_gps_healthcloud.create_by,
		stg_gps_healthcloud.updated_date,
		stg_gps_healthcloud.update_by
    from
        {{ref('stg_gps_healthcloud')}} as stg_gps_healthcloud
),

/*after applying criteria based on where the dates were pulled from,
a few duplicates still exist because of bad dates in salesforce
-the following few tables allow us to remove these duplicates from the analysis*/

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

select
	same_disenroll_stage.mrn,
	same_disenroll_stage.pat_key,
	same_disenroll_stage.pat_id,
	patient.dob,
	patient.sex,
	initcap(patient.last_nm) as patient_last_name,
	initcap(patient.first_nm) as patient_first_name,
	same_disenroll_stage.enroll_date,
	same_disenroll_stage.disenroll_date,
	same_disenroll_stage.country,
	coalesce(same_disenroll_stage.payment_source, 'no coverage') as payment_source,
	same_disenroll_stage.patient_enroll_date_key,
	same_disenroll_stage.create_by,
	same_disenroll_stage.updated_date as salesforce_updated_date,
	same_disenroll_stage.update_by
from
	same_disenroll_stage
	left join {{source('cdw', 'patient')}} as patient
		on same_disenroll_stage.pat_key = patient.pat_key
where
	same_disenroll_stage.row_order = 1
