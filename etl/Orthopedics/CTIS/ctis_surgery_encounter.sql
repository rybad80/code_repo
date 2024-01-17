with
distinct_procedures as (
   select distinct
        or_key,
        log_id,
        surgery_date,
        pat_key,
		scoliosis_category,
		max(
			case when scoliosis_category in ('fusion', 'instrumentation - spine') then surgery_date end
			) over(partition by pat_key) as last_instrumentation_date,
        max(
			case when surgery_date <= current_date then surgery_date end
			) over(partition by pat_key) as last_completed_surgery_date
    from
        {{ ref('ctis_surgery_timeline') }}
),
rollup_procedures as (--region
    select
        or_key,
		pat_key,
        log_id,
		surgery_date,
        case when surgery_date = last_completed_surgery_date then 1 else 0 end as last_completed_surgery_ind,
		case when surgery_date = last_instrumentation_date then 1 else 0 end as last_instrumentation_ind,
        dense_rank() over(partition by pat_key order by surgery_date) as surgery_order,
        group_concat(scoliosis_category) as all_procedure_types,
        group_concat(
            case when scoliosis_category not in ('halo', 'i and d') then scoliosis_category end
            ) as scoliosis_correction_procedures,
		max(case when scoliosis_category = 'halo' then 1 else 0 end) as halo_ind,
        max(case when scoliosis_category = 'i and d' then 1 else 0 end) as incision_debridement_ind
    from
        distinct_procedures
    group by
        or_key,
		pat_key,
        log_id,
        surgery_date,
		last_instrumentation_date,
		last_completed_surgery_date
    --end region
)
select
    surgery_department_or_case_all.or_key,
    surgery_department_or_case_all.mrn,
    surgery_department_or_case_all.patient_name,
    surgery_department_or_case_all.dob,
    surgery_department_or_case_all.surgery_age_years,
    rollup_procedures.surgery_order,
    surgery_department_or_case_all.case_status,
    surgery_department_or_case_all.log_id,
    surgery_department_or_case_all.surgery_date,
    surgery_department_or_case_all.surgery_date - current_date as n_days_to_surgery,
    surgery_department_or_case_all.primary_surgeon,
    surgery_department_or_case_all.all_procedures,
    rollup_procedures.all_procedure_types,
    rollup_procedures.scoliosis_correction_procedures,
    case
        when rollup_procedures.scoliosis_correction_procedures
            like '%expansion%'
        then 1 else 0 end as expansion_ind,
    case
        when rollup_procedures.scoliosis_correction_procedures
            like '%exploration%'
        then 1 else 0 end as exploration_ind,
    case
        when rollup_procedures.scoliosis_correction_procedures
            like '%fusion%'
        then 1 else 0 end as fusion_ind,
    case
        when rollup_procedures.scoliosis_correction_procedures
            like '%instrumentation - spine%'
        then 1 else 0 end as instrumentation_spine_ind,
	case
        when rollup_procedures.scoliosis_correction_procedures
            like '%instrumentation - rib%'
        then 1 else 0 end as instrumentation_rib_ind,
    case
        when rollup_procedures.scoliosis_correction_procedures
            like '%mehta cast%'
        then 1 else 0 end as mehta_cast_ind,
    case
        when rollup_procedures.scoliosis_correction_procedures
            like '%osteotomy%'
        then 1 else 0 end as osteotomy_ind,
    case
        when rollup_procedures.scoliosis_correction_procedures
            like '%other%'
        then 1 else 0 end as other_ind,
    case
        when rollup_procedures.scoliosis_correction_procedures
            like '%removal%'
        then 1 else 0 end as removal_ind,
    case
        when rollup_procedures.scoliosis_correction_procedures
            like '%revision%'
        then 1 else 0 end as revision_ind,
    rollup_procedures.halo_ind,
    rollup_procedures.incision_debridement_ind,
    rollup_procedures.last_completed_surgery_ind,
	rollup_procedures.last_instrumentation_ind,

    surgery_department_or_case_all.fiscal_year,
    surgery_department_or_case_all.calendar_year,

    surgery_department_or_case_all.visit_key,
    surgery_department_or_case_all.log_key,
    surgery_department_or_case_all.pat_key,
    surgery_department_or_case_all.primary_surgeon_prov_key
from
    rollup_procedures
    inner join {{ ref('surgery_department_or_case_all') }} as surgery_department_or_case_all
        on surgery_department_or_case_all.or_key = rollup_procedures.or_key
