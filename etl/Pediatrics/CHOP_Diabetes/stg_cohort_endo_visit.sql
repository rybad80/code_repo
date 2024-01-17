with stg_cohort as (
    select
		diagnosis_encounter_all.pat_key,
        diagnosis_encounter_all.patient_key,
		diagnosis_encounter_all.visit_key,
        diagnosis_encounter_all.encounter_key,
        stg_encounter.dept_key
    from
		{{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
		inner join {{ source('cdw', 'epic_grouper_diagnosis') }} as epic_grouper_diagnosis
			on epic_grouper_diagnosis.dx_key = diagnosis_encounter_all.dx_key
		inner join {{ source('cdw', 'epic_grouper_item') }} as epic_grouper_item
			on epic_grouper_item.epic_grouper_key = epic_grouper_diagnosis.epic_grouper_key
		inner join {{ ref('stg_encounter') }} as stg_encounter
			on diagnosis_encounter_all.encounter_key = stg_encounter.encounter_key
    where
		lower(epic_grouper_item.epic_grouper_nm) = 'chop icd diabetes registry'
		and diagnosis_encounter_all.encounter_date <= current_date
    group by
		diagnosis_encounter_all.pat_key,
        diagnosis_encounter_all.patient_key,
		diagnosis_encounter_all.visit_key,
        diagnosis_encounter_all.encounter_key,
		stg_encounter.dept_key
),

/*all historical OP visit AND onset IP visits since CY23
with 'CHOP#7454' CHOP DIABETES ENDO DISPOSITION*/
cohort as (
    select
		stg_cohort.pat_key,
		stg_cohort.patient_key,
		stg_cohort.encounter_key,
        stg_cohort.visit_key
	from
		stg_cohort
		inner join {{ source('cdw', 'epic_grouper_department') }} as epic_grouper_department
			on epic_grouper_department.dept_key = stg_cohort.dept_key
		inner join {{ source('cdw', 'epic_grouper_item') }} as epic_grouper_item
			on epic_grouper_item.epic_grouper_key = epic_grouper_department.epic_grouper_key
		left join {{ ref('smart_data_element_all') }} as smart_data_element_all
			on smart_data_element_all.visit_key = stg_cohort.visit_key
				-- CHOP DIABETES ENDO DISPOSITION
				and smart_data_element_all.concept_id = 'CHOP#7454'
    where
		exists (
				select pat_key --noqa: L028
				from {{ ref('stg_diabetes_icr_active_flowsheets')}}
				where pat_key = stg_cohort.pat_key ) --noqa: L028
        --OP Visits at Diabetes Center FOR Children (DCC)
		and (lower(epic_grouper_item.epic_grouper_nm) = 'chop dep endocrinology'
		or smart_data_element_all.element_value is not null)
	group by
		stg_cohort.pat_key,
		stg_cohort.patient_key,
		stg_cohort.encounter_key,
        stg_cohort.visit_key
)

--lead last visit date by provider type
select
	stg_encounter.pat_key,
	stg_encounter.patient_key,
	stg_encounter.encounter_key,
	stg_encounter.visit_key,
	stg_encounter.mrn,
	stg_encounter.patient_name,
	stg_encounter.dob,
	stg_encounter.encounter_type as enc_type,
	stg_encounter.age_years, --at the time of visit
	stg_encounter.encounter_date as endo_vis_dt, --current encounter date  
	--last MD visit:
	case
		when lower(dim_provider.provider_type) = 'physician'
			and stg_encounter.encounter_type_id = '101' --office visit
		then last_value(stg_encounter.encounter_date) over(
			partition by
				stg_encounter.patient_key,
				dim_provider.provider_type
			order by
				stg_encounter.encounter_date
		)
	-- most recent physician/MD/DO visit
	end as last_md_vis_dt,
	--last NP visit:
	case
		when lower(dim_provider.provider_type) in ('nurse practitioner')
			and stg_encounter.encounter_type_id = '101' --office visit
		then last_value(stg_encounter.encounter_date) over (
			partition by
				stg_encounter.pat_key,
				dim_provider.provider_type
			order by
				stg_encounter.encounter_date
		)
	end as last_np_vis_dt,
	--last Education visit: same logic as QV Healthy Planet
	case
		when lower(stg_encounter.visit_type) in (
		'advanced pump class', 'ahm class',
		'ahm t1y1 class', 'diabetes edu less than 30 mins',
		'diabetes education', 'diabetes education t1y1',
		'insulin start', 'saline start',
		'pre technology', 'pump class',
		'safety skills class', 'upgrade pump',
		'cgms initiation', 'cgms interpretation'
		)
			or (stg_encounter.visit_type_id = '2318' --video visit diabetes
				and stg_encounter.encounter_type_id = '101' --office visit
				and lower(dim_provider.provider_type) in ('dietician, registered nurse')) --video CDE visit
		then last_value(stg_encounter.encounter_date) over (
			partition by
				stg_encounter.patient_key
			order by
				stg_encounter.encounter_date
			)
	end as last_edu_vis_dt,
	--current encounter information:
	stg_encounter.provider_name as provider_nm,
	stg_encounter.department_name,
	dim_provider.provider_type as prov_type,	--NO primary provider_type attribute in encounter block
	stg_encounter.visit_type,
	case when stg_encounter_inpatient.visit_key is not null then 1 else 0 end as inpatient_ind,
    coalesce(stg_encounter_outpatient_raw.specialty_care_ind, 0) as specialty_care_ind,
    case when stg_encounter_telehealth.visit_key is not null then 1 else 0 end as telehealth_ind,
	stg_encounter.appointment_status as appt_stat,
	dense_rank() over (
		partition by
			stg_encounter.patient_key
		order by
			stg_encounter.encounter_date desc
	) as enc_rn
from
	cohort
	inner join {{ ref('stg_encounter') }} as stg_encounter
		on cohort.encounter_key = stg_encounter.encounter_key
	inner join {{ ref('dim_provider') }} as dim_provider
		on dim_provider.provider_key = stg_encounter.provider_key
    left join {{ ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
        on stg_encounter_outpatient_raw.encounter_key = stg_encounter.encounter_key
	left join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
		on stg_encounter_inpatient.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_encounter_telehealth')}} as stg_encounter_telehealth
        on stg_encounter_telehealth.visit_key = stg_encounter.visit_key
where
	--'cancelled', 'no show', 'left without seen'
	stg_encounter.appointment_status_id not in ('3', '4', '5')
	and stg_encounter.encounter_type_id not in (
		/*bpa, cancelled, email correspondence, error, letter out
		mobile, mychart enc, no show, orders only, patient outreach
		refill, scanning enc, telephone, waitlist*/
		'119', '5', '305', '1066', '105',
		'307', '61', '202', '111', '69',
		'107', '203', '70', '40'
	)
