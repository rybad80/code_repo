with sw_visit as (
    select
        diabetes_visit_cohort.patient_key,
        diabetes_visit_cohort.encounter_key,
        diabetes_visit_cohort.endo_vis_dt as sw_date
    from
        {{ ref('diabetes_visit_cohort') }} as diabetes_visit_cohort
    where
        lower(diabetes_visit_cohort.appt_stat) in ('completed', 'not applicable')
            and (lower(diabetes_visit_cohort.prov_type) = 'social worker'
                or lower(diabetes_visit_cohort.enc_type) = 'social work encounter')
    union all
    select
        flowsheet_all.patient_key,
        flowsheet_all.encounter_key,
        flowsheet_all.encounter_date as sw_date
    from
        {{ ref('flowsheet_all') }} as flowsheet_all
        inner join {{ ref('diabetes_visit_cohort') }} as diabetes_visit_cohort
            on flowsheet_all.encounter_key = diabetes_visit_cohort.encounter_key
    where
        flowsheet_all.flowsheet_id = '40014326' --Assessment Type (Social Work Assessment)
        and flowsheet_all.meas_val is not null
)

select
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	diabetes_patient_all.last_visit_date,
	max(case
        when diabetes_patient_all.diabetes_reporting_month > sw_visit.sw_date
		then sw_visit.sw_date
    end) as most_recent_sw_date,
	max(case
        when diabetes_patient_all.diabetes_reporting_month - interval('15 month') <= sw_visit.sw_date
			and diabetes_patient_all.diabetes_reporting_month > sw_visit.sw_date
        then 1 else 0
    end) as last_15mo_sw_visit_ind,
	max(case
        when diabetes_patient_all.diabetes_reporting_month - interval('12 month') <= sw_visit.sw_date
			and diabetes_patient_all.diabetes_reporting_month > sw_visit.sw_date
        then 1 else 0
    end) as last_12mo_sw_visit_ind
from
    {{ ref('diabetes_patient_all') }} as diabetes_patient_all
    left join sw_visit
        on sw_visit.patient_key = diabetes_patient_all.patient_key
group by
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	diabetes_patient_all.last_visit_date
