with base_visits as (
 select
     visit_key
from {{ref('procedure_order_clinical')}}
where lower(procedure_name) in ('nutritional rehabilitation protocol', 'ip pathway nutritional rehabiliation')
group by visit_key
 ),


base as (
select
	stg_encounter.visit_key,
	stg_patient.pat_key,
	stg_patient.patient_name,
	stg_encounter.mrn,
	stg_patient.dob,
	stg_patient.current_age,
	stg_patient.race,
	stg_patient.ethnicity,
	stg_patient.sex,
	round(stg_patient.current_age) as age_max,
	diagnosis_encounter_all.icd10_code as dx_hosp_acct_final,
	case when dx_hosp_acct_final in ('E43', 'E44.1', 'E44.0', 'E41', 'E46', 'R63.4') then 'Malnutrition'
		when dx_hosp_acct_final = 'R63.0' then 'Anorexia'
		when dx_hosp_acct_final in ('F50.01', 'F50.00') then 'Anorexia nervosa'
		when dx_hosp_acct_final = 'F50.02' then 'Bulimia nervosa'
		when dx_hosp_acct_final in ('F50.9', 'F50.8', 'F50.89') then 'Other eating disorder'
		when dx_hosp_acct_final = 'F50.82' then 'Avoidant/restrictive food intake'
		when dx_hosp_acct_final = 'R00.1' then 'Bradycardia'
		else 'No diagnostic code'
	end as dx_primary_grp,
	stg_encounter.hospital_admit_date,
	stg_encounter.hospital_discharge_date,
	stg_encounter.department_name,
	case when upper(stg_department_all.department_center_abbr) like '%KOP%' then 1 else 0 end as koph_ind,
	case when upper(stg_department_all.department_center_abbr) like 'PHL IP%' then 1 else 0 end as 	phl_main_ind,
	case
        when upper(stg_department_all.department_center_abbr) like '%KOP%' then 'KOPH'
		else stg_department_all.department_center_abbr
	end as location,
	svi.overall_category,
	case when lower(stg_encounter_payor.payor_group) = 'commercial' then 'Commercial'
		when lower(stg_encounter_payor.payor_group) = 'medical assistance' then 'Medicaid'
		else 'Medicaid'
	end as payor_group,
	master_date.f_yyyy
from {{ref('stg_encounter')}} as stg_encounter
inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
    on diagnosis_encounter_all.visit_key = stg_encounter.visit_key
inner join {{ref('stg_patient')}} as stg_patient
    on stg_encounter.pat_key = stg_patient.pat_key
inner join base_visits as base_visits
	on stg_encounter.visit_key = base_visits.visit_key
inner join  {{ref('stg_department_all')}}  as stg_department_all
    on stg_encounter.dept_key = stg_department_all.dept_key
left join {{source('ods', 'patient_geospatial_temp')}} as patient_geospatial_temp
	on stg_encounter.pat_key = patient_geospatial_temp.pat_key
left join {{source('cdc_ods', 'social_vulnerability_index')}} as svi
    on patient_geospatial_temp.census_tract_fips  = svi.fips
left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
	on stg_encounter.visit_key  = stg_encounter_payor.visit_key
left join {{source('cdw', 'master_date')}} as master_date
	on master_date.full_dt = date(stg_encounter.hospital_discharge_date)
where
	(stg_encounter.hospital_discharge_date >= '2018-01-01' or stg_encounter.hospital_discharge_date is null)
	and stg_encounter.age_years  >= 10
	and diagnosis_encounter_all.hsp_acct_final_primary_ind = 1
	),


los as (
select
	visit_key,
	sum(case when lower(service) in ('adolescent', 'behavioral health', 'medical behavioral', 'gi/nutrition')
		then service_los_days
        else 0 end
	) as los_days,
	sum(service_los_days) as hosp_los_days
	from {{ref('adt_service')}}
group by visit_key
),

readmission_visits as (
select
	visit_key
from {{ref('adt_service')}}
where lower(service) = 'adolescent'
group by visit_key
),

readmission as (
select
    stg_encounter_readmission_readmit.index_visit_key as visit_key,
    min(stg_encounter_readmission_readmit.days_to_readmission) as days_to_first_readmission,
    case when days_to_first_readmission <= 7 then 1 else 0 end as readmit_7day_ind,
    case when days_to_first_readmission <= 14 then 1 else 0 end as readmit_14day_ind,
    case when days_to_first_readmission <= 30 then 1 else 0 end as readmit_30day_ind,
    case when days_to_first_readmission <= 90 then 1 else 0 end as readmit_90day_ind,
    case when days_to_first_readmission <= 180 then 1 else 0 end as readmit_180day_ind
from {{ref('stg_encounter_readmission_readmit')}} as stg_encounter_readmission_readmit
inner join readmission_visits as readmission_visits
	on stg_encounter_readmission_readmit.readmit_visit_key  = readmission_visits.visit_key
group by stg_encounter_readmission_readmit.index_visit_key
),


kcal as (
select
    procedure_order_clinical.visit_key,
    ord_spec_quest.order_id,
    row_number() over (
        partition by procedure_order_clinical.visit_key order by ord_spec_quest.order_id
    ) as number_key,
    substr(ord_spec_quest.ord_quest_resp, length(ord_spec_quest.ord_quest_resp) - 10, 4) as kcals
from {{ref('procedure_order_clinical')}} as procedure_order_clinical
inner join {{source('clarity_ods', 'ord_spec_quest')}} as ord_spec_quest
    on ord_spec_quest.order_id = procedure_order_clinical.procedure_order_id
where procedure_order_clinical.cpt_code = '500DIET41' and ord_spec_quest.ord_quest_id = '500200940'

),


restraints as (
select
    visit_key,
    count(*) as restraint_orders,
    sum(violent_restraint_ind) as violent_restraint_orders,
    sum(non_violent_restraint_ind) as nonviolent_restraint_orders,
    sum(num_orders_required) as num_orders_required,
    1 as restraint_ind
from {{ref('stg_restraints_cohort')}}
group by visit_key
),


suicide as (
select
    visit_key,
    max(case when procedure_id = 96662 then 1 else 0 end) as si_bund_ind
from {{ref('procedure_order_clinical')}}
where cpt_code = '500NUR1336'
    and encounter_date >= '2018-01-01'
group by visit_key
),

ng_tubes as (
select
    flowsheet_rec_visit_key,
    min(date(placement_instant)) as ng_placed_date,
    count(*) as ng_completes
from {{ref('flowsheet_lda')}}
where date(placement_instant) > '2019-01-01'
    and flo_meas_id = '41002839' -- Tube Type
    and (lower(meas_value) like '%ng%' or lower(meas_value) like '%og%')
group by flowsheet_rec_visit_key

union all

select
    flowsheet_rec_visit_key,
    min(date(placement_instant)) as ng_placed_date,
    count(*) as ng_completes
from {{ref('flowsheet_lda')}}
where date(placement_instant) > '2019-01-01'
    and flo_meas_id = '40001971'
    and (lower(meas_value) like '%replogle%' or lower(meas_value) like '%salem sump%')
group by flowsheet_rec_visit_key
)


select
	base.visit_key,
	base.pat_key,
	base.patient_name,
	base.mrn,
	base.dob,
	base.dx_hosp_acct_final,
	base.dx_primary_grp,
		case when base.dx_primary_grp in ('Anorexia', 'Anorexia nervosa') then 'Anorexia'
		else base.dx_primary_grp end as dx_grouping,
	base.hospital_admit_date,
	base.hospital_discharge_date,
	date_trunc('month', hospital_discharge_date) as hospital_discharge_month,
	los.los_days,
	los.hosp_los_days,
	base.current_age,
	base.age_max,
	case when base.race = 'Indian' then 'Other'
         when base.race = 'Refused' then 'Other'
		when base.race = 'Black or African American' then 'Black' else race end as race,
	base.ethnicity,
	base.sex,
	base.overall_category as svi_category,
	base.payor_group,
	case when base.department_name like '4 WEST%' then '4 West'
         when base.department_name = '3EASTCSH' then '3 East CSH'
		else initcap(base.department_name) end as discharge_dept,
	base.location,
	readmission.readmit_7day_ind,
	readmission.readmit_14day_ind,
	readmission.readmit_30day_ind,
	readmission.readmit_90day_ind,
	readmission.readmit_180day_ind,
	kcal.kcals,
	restraints.restraint_orders,
	restraints.violent_restraint_orders,
	restraints.nonviolent_restraint_orders,
	restraints.violent_restraint_orders + restraints.nonviolent_restraint_orders as total_restraints,
	restraints.restraint_ind,
	restraints.num_orders_required,
	case when restraints.nonviolent_restraint_orders  > 0 then 'nonviolent'
		when restraints.violent_restraint_orders > 0 then 'violent' end as restraint_type,
	suicide.si_bund_ind,
	ng_tubes.ng_placed_date,
	date_trunc('month', ng_tubes.ng_placed_date) as ng_placed_month,
	case when date(ng_tubes.ng_placed_date) - date(base.hospital_admit_date) <= 7 then 1 else 0 end as first_ng_ind,
	ng_tubes.ng_completes,
	case when ng_tubes.ng_completes > 0 then 1 else 0 end as ng_ind,
	base.f_yyyy
from base as base
left join readmission as readmission
	on base.visit_key = readmission.visit_key
left join kcal as kcal
	on base.visit_key = kcal.visit_key and kcal.number_key = 1
left join restraints as restraints
	on base.visit_key = restraints.visit_key
left join suicide as suicide
	on base.visit_key = suicide.visit_key
left join ng_tubes as ng_tubes
	on base.visit_key = ng_tubes.flowsheet_rec_visit_key
left join los as los
	on base.visit_key = los.visit_key
