with timebreaks as (
	select
		master_date.full_dt + cast('01:00:00' as interval) * _v_vector_idx.idx as time_mark

	from
		{{source('cdw', '_v_vector_idx')}} as _v_vector_idx
	cross join {{source('cdw', 'master_date')}} as master_date

	where master_date.full_dt >= '01-01-2016'
		and _v_vector_idx.idx < 24

	group by
		time_mark
),

cicu_times as (
	select
		cardiac_arrest_cohort_model.cicu_enc_key,
		timebreaks.time_mark
	from
		{{ref('cardiac_arrest_cohort_model')}} as cardiac_arrest_cohort_model
	inner join timebreaks
		on timebreaks.time_mark >= cardiac_arrest_cohort_model.in_date
			and timebreaks.time_mark <= cardiac_arrest_cohort_model.out_date
	union
	select
		cardiac_arrest_cohort_model.cicu_enc_key,
		coalesce(cardiac_arrest_cohort_model.first_arrest_date, cardiac_arrest_cohort_model.rand_end_date) as time_mark
	from
		{{ref('cardiac_arrest_cohort_model')}} as cardiac_arrest_cohort_model
),

arrest_events as (
	select
		cardiac_arrest_cohort_model.cicu_enc_key,
        cicu_times.time_mark,
		min(case when cardiac_arrest_all.arrest_date is not null
			and cardiac_arrest_all.arrest_date >= cardiac_arrest_cohort_model.in_date
			and cardiac_arrest_all.arrest_date <= cardiac_arrest_cohort_model.out_date
			and cardiac_arrest_all.arrest_date >= cicu_times.time_mark
			then cardiac_arrest_all.arrest_date
			else null end) as next_arrest_date
	from
		cicu_times
		inner join {{ref('cardiac_arrest_cohort_model')}} as cardiac_arrest_cohort_model
			on cicu_times.cicu_enc_key = cardiac_arrest_cohort_model.cicu_enc_key
		left join {{ref('cardiac_unit_encounter')}} as cardiac_unit_encounter
			on cardiac_unit_encounter.pat_key = cardiac_arrest_cohort_model.pat_key
				and lower(cardiac_unit_encounter.department_name) = 'cicu'
		left join {{ref('cardiac_arrest_all')}} as cardiac_arrest_all
			on cardiac_arrest_all.enc_key = cardiac_unit_encounter.enc_key
	group by
		cardiac_arrest_cohort_model.cicu_enc_key,
        cicu_times.time_mark
)

select
	cardiac_arrest_cohort_model.visit_key,
	cardiac_arrest_cohort_model.pat_key,
	cardiac_arrest_cohort_model.dob,
	cardiac_arrest_cohort_model.mrn,
	cardiac_arrest_cohort_model.hospital_admit_date,
	cardiac_arrest_cohort_model.hospital_discharge_date,
	cardiac_arrest_cohort_model.in_date,
	cardiac_arrest_cohort_model.out_date,
	cardiac_arrest_cohort_model.cicu_los_hrs,
	cardiac_arrest_cohort_model.cicu_enc_key,
	cardiac_arrest_cohort_model.arrest_ind,
	cardiac_arrest_cohort_model.rand_end_date,
	cicu_times.time_mark,
	{{
        dbt_utils.surrogate_key([
            'cardiac_arrest_cohort_model.cicu_enc_key',
            'cicu_times.time_mark'
            ])
    }} as time_mark_key,
	date(cicu_times.time_mark) - date(cardiac_arrest_cohort_model.dob) as age_days,
	case when age_days < 365
		then 1
		else 0 end as lt_1yr_ind,
	case when age_days <= 30
		then 1
		else 0 end as lt_30days_ind,
	arrest_events.next_arrest_date,
	extract(epoch from cicu_times.time_mark - next_arrest_date) / 3600.0 as hrs_to_arrest_neg,
	extract(epoch from next_arrest_date - cicu_times.time_mark) / 3600.0 as hrs_to_arrest,
	extract(epoch from cicu_times.time_mark - cardiac_arrest_cohort_model.in_date) / 3600.0 as hrs_since_entry,
	case when hrs_to_arrest <= 8.0
		then 1
		else 0 end as arrest_8hr_ind,
	case when hrs_to_arrest <= 6.0
		then 1
		else 0 end as arrest_6hr_ind,
	case when hrs_to_arrest <= 4.0
		then 1
		else 0 end as arrest_4hr_ind,
	case when hrs_to_arrest <= 2.0
		then 1
		else 0 end as arrest_2hr_ind
from
	cicu_times
	inner join {{ref('cardiac_arrest_cohort_model')}} as cardiac_arrest_cohort_model
			on cicu_times.cicu_enc_key = cardiac_arrest_cohort_model.cicu_enc_key
	left join arrest_events
		on arrest_events.cicu_enc_key = cicu_times.cicu_enc_key
		and arrest_events.time_mark = cicu_times.time_mark
