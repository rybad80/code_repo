select
	cardiac_arrest_cohort.mrn,
	cardiac_arrest_cohort.cicu_start_date,
	cardiac_arrest_cohort.cicu_end_date,
	cardiac_arrest_cohort.dob,
	cardiac_arrest_cohort.arrest_ind,
	master_date.full_dt,
	date(master_date.full_dt) - date(cardiac_arrest_cohort.dob) as age_days,
	case when age_days <= 30
		then 1
		else 0 end as neonate_ind,
	case when age_days > 30
		then 1
		else 0 end as non_neonate_ind,
	cardiac_arrest_cohort.cicu_los_days,
	cardiac_arrest_cohort.cicu_enc_key

from
	{{ref('cardiac_arrest_cohort')}} as cardiac_arrest_cohort
inner join {{source('cdw', 'master_date')}} as master_date
	on master_date.full_dt between date(cardiac_arrest_cohort.cicu_start_date)
		and date(cardiac_arrest_cohort.cicu_end_date)
