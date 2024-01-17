{{ config(meta = {
    'critical': true
}) }}

with stg_gps_healthcloud as (
    select
        stg_patient.pat_key,
        stg_patient.pat_id,
        stg_patient.mrn,
        salesforce_hc_case.im_scheduled_date__c as first_schedule_dt, --christina's report says gm_scheduled_date
        salesforce_hc_case.closed_date_hcm__c as close_date,
        coalesce(salesforce_hc_case.country_ff_hcm__c, 'No country provided') as country,
        salesforce_hc_case.payment_source_hcm__c as payment_source,
        salesforce_hc_case.status, --noqa: L029
        cast(substring(salesforce_hc_case.createddate, 0, 10) as date) as active_date,
        case
            when lower(salesforce_hc_case.status) in ('current patient', 'scheduled')
                then current_date
            --close date is disenrollment date for baseline time period
            else cast(salesforce_hc_case.closed_date_hcm__c as date)
        end as last_cdw_updated_date,
        case
            when lower(salesforce_hc_case.status) in ('current patient', 'scheduled')
                then null
            else cast(salesforce_hc_case.closed_date_hcm__c as date)
		end as disenroll_date,
        'HEALTHCLOUD' as create_by,
        salesforce_hc_case.upd_dt as updated_date,
        'HEALTHCLOUD' as update_by
    from
        {{source ('salesforce_hc_ods', 'salesforce_hc_case')}} as salesforce_hc_case
        inner join {{ref ('stg_patient')}} as stg_patient
            on stg_patient.mrn = salesforce_hc_case.cg_mrn__c
    where
        salesforce_hc_case.recordtypeid = '0123i0000010DAmAAM' --GPS team only
        and salesforce_hc_case.status not in ('Closed-Not IM')
),


baseline_gps_cohort_enroll_stage as (
    select
        stg_gps_healthcloud.mrn,
		stg_gps_healthcloud.pat_key,
        stg_gps_healthcloud.pat_id,
        stg_gps_healthcloud.first_schedule_dt,
		stg_gps_healthcloud.active_date,
        stg_encounter.visit_type,
		stg_encounter.eff_dt, -- first in person touch
		stg_gps_healthcloud.disenroll_date,
		stg_gps_healthcloud.last_cdw_updated_date,
		stg_gps_healthcloud.country,
        stg_gps_healthcloud.payment_source,
        stg_gps_healthcloud.status,
        stg_gps_healthcloud.create_by,
        stg_gps_healthcloud.updated_date,
        stg_gps_healthcloud.update_by
    from
        stg_gps_healthcloud
        inner join {{ref('stg_encounter')}} as stg_encounter
			on stg_gps_healthcloud.pat_key = stg_encounter.pat_key
    where
        (
            (((stg_encounter.eff_dt >= stg_gps_healthcloud.first_schedule_dt
            and stg_encounter.eff_dt <= stg_gps_healthcloud.last_cdw_updated_date)
            or stg_gps_healthcloud.active_date >= stg_gps_healthcloud.first_schedule_dt)
            --excluding only research encounters
            and stg_encounter.encounter_type_id != ('11')
            /*id's for outpatient, recurring outpatient, n/a, ip, emergency, day surg,
            obs, all admit after surg patient classes*/
            and stg_encounter.patient_class_id in ('2', '6', '0', '1', '3', '4', '5', '7', '8', '10', '13')
            and lower(stg_encounter.visit_type) not like 'research%'
            )
            -- for visits related to opinion closed statuses in salesforce.
            or (stg_gps_healthcloud.first_schedule_dt is null
            and stg_encounter.eff_dt <= stg_gps_healthcloud.last_cdw_updated_date
            --letter (out) for Opinion Closed cases
            and stg_encounter.encounter_type_id in ('105')
            --id's for outpatient, recurring outpatient and n/a classes
            and stg_encounter.patient_class_id in ('2', '6', '0')
            )
            or stg_encounter.department_id = '101033100' -- bgr gps clinic
        )
),

baseline_gps_cohort_enroll as (
    select
        baseline_gps_cohort_enroll_stage.mrn,
        baseline_gps_cohort_enroll_stage.pat_key,
        baseline_gps_cohort_enroll_stage.pat_id,
        baseline_gps_cohort_enroll_stage.first_schedule_dt,
        min(baseline_gps_cohort_enroll_stage.active_date) as enroll_date,
        baseline_gps_cohort_enroll_stage.visit_type,
        --remove instances in salesforce with multiple disenrollment dates per enrollment date
        row_number() over(
            partition by
                baseline_gps_cohort_enroll_stage.mrn,
                min(baseline_gps_cohort_enroll_stage.active_date)
            order by
                baseline_gps_cohort_enroll_stage.disenroll_date desc
        ) as row_order,
        baseline_gps_cohort_enroll_stage.disenroll_date,
        baseline_gps_cohort_enroll_stage.country,
        baseline_gps_cohort_enroll_stage.payment_source,
        baseline_gps_cohort_enroll_stage.status,
        baseline_gps_cohort_enroll_stage.create_by,
        baseline_gps_cohort_enroll_stage.updated_date,
        baseline_gps_cohort_enroll_stage.update_by
    from
        baseline_gps_cohort_enroll_stage
    group by
        baseline_gps_cohort_enroll_stage.mrn,
        baseline_gps_cohort_enroll_stage.pat_key,
        baseline_gps_cohort_enroll_stage.pat_id,
        baseline_gps_cohort_enroll_stage.first_schedule_dt,
        baseline_gps_cohort_enroll_stage.visit_type,
        baseline_gps_cohort_enroll_stage.disenroll_date,
        baseline_gps_cohort_enroll_stage.country,
        baseline_gps_cohort_enroll_stage.payment_source,
        baseline_gps_cohort_enroll_stage.status,
        baseline_gps_cohort_enroll_stage.create_by,
        baseline_gps_cohort_enroll_stage.updated_date,
        baseline_gps_cohort_enroll_stage.update_by
)
/*
payors are derived based on the following hierarchy
1. embassy
2. self pay
3. private insurance (includes insurance and healthy ministry payment sources
4. research
5. no active payor
*/

select
    baseline_gps_cohort_enroll.mrn,
	baseline_gps_cohort_enroll.pat_key,
    baseline_gps_cohort_enroll.pat_id,
    baseline_gps_cohort_enroll.visit_type as visit_type_nm,
    baseline_gps_cohort_enroll.first_schedule_dt,
	baseline_gps_cohort_enroll.enroll_date,
	baseline_gps_cohort_enroll.disenroll_date,
    baseline_gps_cohort_enroll.status,
	initcap(baseline_gps_cohort_enroll.country) as country,
	case
        when lower(baseline_gps_cohort_enroll.payment_source) = 'embassy'
			then 'Embassy'
		when lower(baseline_gps_cohort_enroll.payment_source) in ('insurance', 'health ministry')
			then 'Insurance'
		when lower(baseline_gps_cohort_enroll.payment_source) = 'self pay'
			then 'Self Pay'
		when lower(baseline_gps_cohort_enroll.payment_source) not in (
            'insurance', 'health ministry', 'embassy', 'self pay')
            and baseline_gps_cohort_enroll.payment_source is not null
			then 'Other'
		else 'No Coverage'
	end as payment_source,
    {{
        dbt_utils.surrogate_key([
            'baseline_gps_cohort_enroll.pat_key',
            'baseline_gps_cohort_enroll.enroll_date',
            'baseline_gps_cohort_enroll.disenroll_date'
        ])
    }} as patient_enroll_date_key,
    baseline_gps_cohort_enroll.create_by,
    baseline_gps_cohort_enroll.updated_date,
    baseline_gps_cohort_enroll.update_by --noqa: L029
from
    baseline_gps_cohort_enroll
where
    baseline_gps_cohort_enroll.row_order = 1
        or baseline_gps_cohort_enroll.enroll_date is null
