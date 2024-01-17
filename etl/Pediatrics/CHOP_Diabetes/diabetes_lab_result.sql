with lab_order as ( --all results from procedures ordered in clinical system
select
    diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	procedure_order_result_clinical.visit_key,
	procedure_order_result_clinical.procedure_name,
	procedure_order_result_clinical.result_component_name,
	procedure_order_result_clinical.procedure_group_name,
	procedure_order_result_clinical.abnormal_result_ind,
	procedure_order_result_clinical.result_value,
	procedure_order_result_clinical.result_value_numeric,
	procedure_order_result_clinical.result_date
from
    {{ref('diabetes_patient_all')}} as diabetes_patient_all
	inner join
        {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical on
            procedure_order_result_clinical.pat_key = diabetes_patient_all.pat_key
where
	lower(procedure_order_result_clinical.result_lab_status) = 'final result' --only include final lab results
    and diabetes_patient_all.diabetes_reporting_month > date(procedure_order_result_clinical.result_date)
group by
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	procedure_order_result_clinical.visit_key,
	procedure_order_result_clinical.procedure_name,
	procedure_order_result_clinical.result_component_name,
	procedure_order_result_clinical.procedure_group_name,
	procedure_order_result_clinical.abnormal_result_ind,
	procedure_order_result_clinical.result_value,
	procedure_order_result_clinical.result_value_numeric,
	procedure_order_result_clinical.result_date
),
a1c_order as ( --records of a1c procedures orders
select
	lab_order.diabetes_reporting_month,
	lab_order.patient_key,
	lab_order.visit_key,
	lab_order.result_component_name as a1c_order,
	lab_order.abnormal_result_ind,
	lab_order.result_value,
	lab_order.result_value_numeric,
	case when lab_order.result_value_numeric is not null then lab_order.result_value_numeric
		when lab_order.result_value like '%>%' or lab_order.result_value like '%<%'
			--convert '>14.0' to 14 as numeric 
			then cast(replace(replace(trim(upper(lab_order.result_value)), '>', ''), '<', '') as float(8))
		else lab_order.result_value_numeric end as a1c_result,
	lab_order.result_date,
	--rank per patient 
	row_number() over (
        partition by lab_order.diabetes_reporting_month, lab_order.patient_key order by lab_order.result_date desc
	) as a1c_rn
from
	lab_order
where
	lower(lab_order.result_component_name) like '%a1c%'
),
a1c_past_4_mo as (--average a1c value for each patient for all the a1cs taken in the prior 4 months or 15 months
select
	a1c_order.diabetes_reporting_month,
	a1c_order.patient_key,
	avg(case when date(a1c_order.result_date) >= a1c_order.diabetes_reporting_month - interval('4 month')
		and date(a1c_order.result_date) < a1c_order.diabetes_reporting_month then a1c_order.a1c_result
		end) as avg_a1c_past_4mo,
	avg(case when date(a1c_order.result_date) >= a1c_order.diabetes_reporting_month - interval('15 month')
		and date(a1c_order.result_date) < a1c_order.diabetes_reporting_month then a1c_order.a1c_result
		end) as avg_a1c_past_15mo,
	case
		when avg_a1c_past_4mo < 7 then '<7.0' --noqa: L028
		when avg_a1c_past_4mo >= 7 and avg_a1c_past_4mo < 7.5 then '7.0-7.5' --noqa: L028 
		when avg_a1c_past_4mo >= 7.5 and avg_a1c_past_4mo < 8 then '7.5-8.0' --noqa: L028
		when avg_a1c_past_4mo >= 8 and avg_a1c_past_4mo < 8.5 then '8.0-8.5' --noqa: L028
		when avg_a1c_past_4mo >= 8.5 and avg_a1c_past_4mo < 9 then '8.5-9.0' --noqa: L028
		when avg_a1c_past_4mo >= 9 and avg_a1c_past_4mo < 9.5 then '9.0-9.5' --noqa: L028
		when avg_a1c_past_4mo >= 9.5 and avg_a1c_past_4mo < 10 then '9.5-10.0' --noqa: L028
		when avg_a1c_past_4mo >= 10 and avg_a1c_past_4mo < 10.5 then '10.0-10.5' --noqa: L028
		when avg_a1c_past_4mo >= 10.5 and avg_a1c_past_4mo < 11 then '10.5-11.0' --noqa: L028
		when avg_a1c_past_4mo >= 11 and avg_a1c_past_4mo < 11.5 then '11.0-11.5' --noqa: L028
		when avg_a1c_past_4mo >= 11.5 and avg_a1c_past_4mo < 12 then '11.5-12.0' --noqa: L028
		when avg_a1c_past_4mo >= 12 and avg_a1c_past_4mo < 12.5 then '12.0-12.5' --noqa: L028
		when avg_a1c_past_4mo >= 12.5 and avg_a1c_past_4mo < 13 then '12.5-13.0' --noqa: L028
		when avg_a1c_past_4mo >= 13 and avg_a1c_past_4mo < 13.5 then '13.0-13.5' --noqa: L028
		when avg_a1c_past_4mo >= 13.5 and avg_a1c_past_4mo < 14 then '13.5-14.0' --noqa: L028
		when avg_a1c_past_4mo >= 14 then '>=14.0' --noqa: L028
		end as avg_a1c_range_4mo,
	case
		when avg_a1c_past_15mo < 7 then '<7.0' --noqa: L028
		when avg_a1c_past_15mo >= 7 and avg_a1c_past_15mo < 7.5 then '7.0-7.5' --noqa: L028
		when avg_a1c_past_15mo >= 7.5 and avg_a1c_past_15mo < 8 then '7.5-8.0' --noqa: L028
		when avg_a1c_past_15mo >= 8 and avg_a1c_past_15mo < 8.5 then '8.0-8.5' --noqa: L028
		when avg_a1c_past_15mo >= 8.5 and avg_a1c_past_15mo < 9 then '8.5-9.0' --noqa: L028
		when avg_a1c_past_15mo >= 9 and avg_a1c_past_15mo < 9.5 then '9.0-9.5' --noqa: L028
		when avg_a1c_past_15mo >= 9.5 and avg_a1c_past_15mo < 10 then '9.5-10.0' --noqa: L028
		when avg_a1c_past_15mo >= 10 and avg_a1c_past_15mo < 10.5 then '10.0-10.5' --noqa: L028
		when avg_a1c_past_15mo >= 10.5 and avg_a1c_past_15mo < 11 then '10.5-11.0' --noqa: L028
		when avg_a1c_past_15mo >= 11 and avg_a1c_past_15mo < 11.5 then '11.0-11.5' --noqa: L028
		when avg_a1c_past_15mo >= 11.5 and avg_a1c_past_15mo < 12 then '11.5-12.0' --noqa: L028
		when avg_a1c_past_15mo >= 12 and avg_a1c_past_15mo < 12.5 then '12.0-12.5' --noqa: L028
		when avg_a1c_past_15mo >= 12.5 and avg_a1c_past_15mo < 13 then '12.5-13.0' --noqa: L028
		when avg_a1c_past_15mo >= 13 and avg_a1c_past_15mo < 13.5 then '13.0-13.5' --noqa: L028
		when avg_a1c_past_15mo >= 13.5 and avg_a1c_past_15mo < 14 then '13.5-14.0' --noqa: L028
		when avg_a1c_past_15mo >= 14 then '>=14.0' --noqa: L028
		end as avg_a1c_range_15mo,
	sum(case when date(a1c_order.result_date) >= a1c_order.diabetes_reporting_month - interval('4 month')
		and date(a1c_order.result_date) < a1c_order.diabetes_reporting_month then 1
		else 0 end) as count_a1c_past_4mo,
	sum(case when date(a1c_order.result_date) >= a1c_order.diabetes_reporting_month - interval('15 month')
		and date(a1c_order.result_date) < a1c_order.diabetes_reporting_month then 1
		else 0 end) as count_a1c_past_15mo,
	max(case when a1c_order.a1c_rn = 1 then a1c_order.a1c_result end) as last_a1c_result,
	max(case when a1c_order.a1c_rn = 1 then a1c_order.result_date end) as last_a1c_date
from
	a1c_order
group by
	a1c_order.diabetes_reporting_month,
	a1c_order.patient_key
),
flo_most_recent_a1c as ( --a1c value from flowsheet 10060217 'most recent a1c value' per patient
						--teams will keep using this flo to document a1c value in new workflow since CY23 
select
	diabetes_patient_all.diabetes_reporting_month,
    diabetes_patient_all.patient_key,
    flowsheet_all.meas_val_num as a1c,
    flowsheet_all.encounter_date as a1c_date,
    row_number() over (
        partition by
            diabetes_patient_all.diabetes_reporting_month, diabetes_patient_all.patient_key
        order by flowsheet_all.encounter_date desc
    ) as a1c_rn
from
	{{ref('diabetes_patient_all')}} as diabetes_patient_all
    inner join {{ref('flowsheet_all')}} as flowsheet_all on diabetes_patient_all.pat_key = flowsheet_all.pat_key
where
    flowsheet_all.flowsheet_id in (
            10060217 --'most recent a1c value'
            )
    and flowsheet_all.meas_val is not null
    and diabetes_patient_all.diabetes_reporting_month > a1c_date
),
ldl_order as ( --records of ldl procedures orders
select
	lab_order.diabetes_reporting_month,
	lab_order.patient_key,
	lab_order.visit_key,
	lab_order.result_component_name as ldl_order,
	lab_order.abnormal_result_ind,
	lab_order.result_value,
	lab_order.result_value_numeric,
	case when lab_order.result_value_numeric < 100 then '<100'
		when lab_order.result_value_numeric >= 100 and lab_order.result_value_numeric < 130 then '100-130'
		when lab_order.result_value_numeric >= 130 and lab_order.result_value_numeric < 160 then '130-160'
		when lab_order.result_value_numeric >= 160 then '>=160'
		end as ldl_range,
	lab_order.result_date,
	case when date(lab_order.result_date) >= lab_order.diabetes_reporting_month - interval('15 month')
		and date(lab_order.result_date) < lab_order.diabetes_reporting_month
		then 1 else 0 end as ldl_past_15mo_ind,
	--rank per patient 
	row_number() over (
        partition by lab_order.diabetes_reporting_month, lab_order.patient_key order by lab_order.result_date desc
	) as ldl_rn
from
	lab_order
where
	lower(lab_order.result_component_name) like 'ldl%' --procedure_name like lipid panel
	and lower(lab_order.result_component_name) not like 'ldl/%' --exclude ldl/hdl ratio
)
--pull patient level lab results:
select
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
--a1c avg from procedure orders in last 4 months or 15 months:
	a1c_past_4_mo.avg_a1c_past_4mo,
	a1c_past_4_mo.avg_a1c_range_4mo,
	a1c_past_4_mo.avg_a1c_past_15mo,
	a1c_past_4_mo.avg_a1c_range_15mo,
--a1c most recent record final: 
	max(case when date(a1c_past_4_mo.last_a1c_date) < date(flo_most_recent_a1c.a1c_date) then flo_most_recent_a1c.a1c
		else a1c_past_4_mo.last_a1c_result end) as most_recent_a1c_result,
	max(
        case
            when
                date(
                    a1c_past_4_mo.last_a1c_date
                ) < date(flo_most_recent_a1c.a1c_date) then date(flo_most_recent_a1c.a1c_date)
		else date(a1c_past_4_mo.last_a1c_date) end) as most_recent_a1c_date,
--ldl result (most recent per patient):
    max(coalesce(ldl_order.ldl_past_15mo_ind, 0)) as ldl_past_15mo_ind,
	max(ldl_order.result_value_numeric) as ldl_most_recent_result,
	max(ldl_order.ldl_range) as ldl_most_recent_range,
	max(ldl_order.result_date) as ldl_most_recent_date
from
	{{ref('diabetes_patient_all')}} as diabetes_patient_all
	left join a1c_past_4_mo 	on a1c_past_4_mo.patient_key = diabetes_patient_all.patient_key
										and a1c_past_4_mo.diabetes_reporting_month = diabetes_patient_all.diabetes_reporting_month
	left join flo_most_recent_a1c on flo_most_recent_a1c.patient_key = diabetes_patient_all.patient_key
										and flo_most_recent_a1c.diabetes_reporting_month = diabetes_patient_all.diabetes_reporting_month
										and flo_most_recent_a1c.a1c_rn = 1
	left join ldl_order on ldl_order.patient_key = diabetes_patient_all.patient_key
										and ldl_order.diabetes_reporting_month = diabetes_patient_all.diabetes_reporting_month
										and ldl_order.ldl_rn = 1
group by
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	a1c_past_4_mo.avg_a1c_past_4mo,
	a1c_past_4_mo.avg_a1c_range_4mo,
	a1c_past_4_mo.avg_a1c_past_15mo,
	a1c_past_4_mo.avg_a1c_range_15mo
