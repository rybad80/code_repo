{{ config(
	materialized='table',
	dist='visit_key',
	meta={
		'critical': true
	}
) }}

with dept_groups as ( --noqa: PRS
    select
        stg_department_all.department_id,
        stg_department_all.department_name,
        fact_department_rollup_summary.unit_dept_grp_abbr,
        coalesce(fact_department_rollup_summary.min_dept_align_dt, '1900-01-01') as min_date,
        coalesce(fact_department_rollup_summary.max_dept_align_dt, current_date) as max_date,
        row_number() over (partition by fact_department_rollup_summary.dept_key
            order by min_dept_align_dt) as depts_seq_num,
        case
            when fact_department_rollup_summary.unit_dept_grp_abbr in ('PICU', 'CICU', 'NICU')
            then 1
            when stg_department_all.department_name in ('NICU EAST', 'NICU WEST')
            then 1
            else 0
        end as icu_ind,
        case
            when stg_department_all.department_name in ('MAIN EMERGENCY DEPT', 'KOPH EMERGENCY DEP')
            then 1
            else 0
        end as ed_ind,
        case
            when fact_department_rollup_summary.always_count_for_census_ind = 1
            then 1
            when stg_department_all.department_name in (-- pre 2014 ip department names
            '3 CENTER', '7 EAST', '7 NORTH', '7 WEST', 'NICU EAST', 'NICU WEST')
            then 1
            else 0
        end as always_count_for_census_ind,
        case
            when stg_department_all.department_name in ('MAIN EMERGENCY DEPT', 'KOPH EMERGENCY DEP')
            then 0
            else 1
        end as hosp_count_for_census_ind
    from
        {{ref('stg_department_all')}} as stg_department_all
        left join {{source('cdw_analytics', 'fact_department_rollup_summary')}} as fact_department_rollup_summary
            on fact_department_rollup_summary.dept_key = stg_department_all.dept_key
    where
        fact_department_rollup_summary.hosp_count_for_census_ind = 1
        or stg_department_all.department_name in (
            '3 CENTER', '7 EAST', '7 NORTH', '7 WEST', 'NICU EAST',
            'NICU WEST', 'MAIN EMERGENCY DEPT', 'KOPH EMERGENCY DEP'
        )
),

add_idx as (
    select
        pat_enc_csn_id,
        event_id,
        pat_id,
        effective_time,
        department_id,
        pat_service_c,
        null as pat_service,
        event_type_c,
        event_subtype_c
    from
        {{source('clarity_ods','clarity_adt')}}

    union all

    select
        csn as pat_enc_csn_id,
        adt_event_id as event_id,
        pat_id,
        eff_event_dt as effective_time,
        dept_id as department_id,
        cast(pat_service_id as varchar(66)) as pat_service_c,
        pat_service,
        adt_event_type_id as event_type_c,
        adt_event_subtype_id as event_subtype_c
    from
        {{source('manual_ods','idx_visit_event')}}
),

unionset as (
    select
        add_idx.pat_enc_csn_id,
        add_idx.event_id,
        add_idx.pat_id,
        add_idx.effective_time,
        add_idx.department_id,
        coalesce(add_idx.pat_service, zc_pat_service.name) as pat_service,
        coalesce(
            dept_groups.unit_dept_grp_abbr,
            dept_groups_imputation.unit_dept_grp_abbr,
            dept_groups_imputation.department_name
        ) as department_group,
        coalesce(
            dept_groups.hosp_count_for_census_ind,
            dept_groups_imputation.hosp_count_for_census_ind,
            0
        ) as hosp_count_for_census_ind,
        coalesce(
            dept_groups.always_count_for_census_ind,
            dept_groups_imputation.always_count_for_census_ind,
            0
        ) as always_count_for_census_ind,
        coalesce(
            dept_groups.icu_ind,
            dept_groups_imputation.icu_ind,
            0
        ) as icu_ind,
        coalesce(
            dept_groups.ed_ind,
            dept_groups_imputation.ed_ind,
            0
        ) as ed_ind,
        max(
			case
				when add_idx.event_type_c = 2
			then 1
			else 0
		end) over(partition by pat_enc_csn_id) as discharged_ind

    from
        add_idx
        left join {{source('clarity_ods','zc_pat_service')}} as zc_pat_service
            on zc_pat_service.internal_id = add_idx.pat_service_c
        left join dept_groups
            on dept_groups.department_id = add_idx.department_id
            and date(add_idx.effective_time) between dept_groups.min_date and dept_groups.max_date
        -- pre 2014 ip department names
        left join dept_groups as dept_groups_imputation
            on dept_groups_imputation.department_id = add_idx.department_id
            and dept_groups_imputation.depts_seq_num = 1
    where
        --(dept_groups.hosp_count_for_census_ind = 1
        --    or dept_groups_imputation.hosp_count_for_census_ind = 1
        --    or visit_event.dept_key = 0)
        add_idx.event_type_c in (1, 2, 3, 5) -- (Admission, Discharge, Transfer In, Patient Update)
        and add_idx.event_subtype_c != 2 -- Canceled

    union all

    select
        pat_enc_csn_id,
        adt_event_id as event_id,
        stg_cpru_20_hosp_24.pat_id,
        dept_enter_date as effective_time,
        stg_cpru_20_hosp_24.department_id,
        pat_service.dict_nm as pat_service,
        'CPRU' as department_group,
        1 as hosp_count_for_census_ind,
        1 as always_count_for_census_ind,
        0 as icu_ind,
        0 as ed_ind,
        cpru_discharge_ind as discharged_ind
    from
        {{ref('stg_cpru_20_hosp_24')}} as stg_cpru_20_hosp_24
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.csn = stg_cpru_20_hosp_24.pat_enc_csn_id
        inner join {{source('cdw','cdw_dictionary')}} as pat_service
            on pat_service.dict_key = stg_cpru_20_hosp_24.dict_pat_svc_key


),

get_order as (
    select
        pat_enc_csn_id,
        event_id,
        pat_id,
        effective_time,
        department_id,
        department_group,
        pat_service,
        always_count_for_census_ind,
        icu_ind,
        ed_ind,
        row_number() over (partition by pat_enc_csn_id
            order by always_count_for_census_ind desc, effective_time, event_id
        ) as ip_row_asc,
        row_number() over (partition by pat_enc_csn_id
            order by hosp_count_for_census_ind desc, effective_time desc, event_id desc
        ) as ip_row_desc,
        row_number() over (partition by pat_enc_csn_id
            order by effective_time desc, event_id desc
        ) as all_row_desc,
        lead(effective_time, 1, null) over ( --noqa: PRS
            partition by pat_enc_csn_id order by effective_time, event_id desc
        ) as end_event_dt,
        case
            when icu_ind = 1
            then extract(epoch from end_event_dt - effective_time) / 86400.0
            else 0
        end as icu_los_days,
        discharged_ind
    from
        unionset
),

inpatient as (
    select
        pat_enc_csn_id,
        pat_id,
        max(case when ip_row_asc = 1 then event_id end) as event_id,
        max(case when ip_row_asc = 1 then effective_time end) as ip_enter_date,
        max(case when ip_row_asc = 1 then department_id end) as admission_dept_id,
        max(case when ip_row_asc = 1 then department_group end) as admission_department_group,
        max(case when ip_row_asc = 1 then pat_service end) as admission_service,
        max(case when ip_row_desc = 1 then department_id end) as discharge_dept_id,
        max(case when ip_row_desc = 1 then department_group end) as discharge_department_group,
        max(case when all_row_desc = 1 then pat_service end) as discharge_service,
        max(always_count_for_census_ind) as considered_ip,
        max(icu_ind) as icu_ind,
        max(ed_ind) as ed_ind,
        sum(icu_los_days) as icu_los_days,
        max(discharged_ind) as discharged_ind
    from
        get_order
    group by
        pat_enc_csn_id,
        pat_id
)

select
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    inpatient.pat_enc_csn_id,
    inpatient.event_id,
    stg_encounter.pat_key,
    stg_encounter.patient_key,
    inpatient.pat_id,
    inpatient.ip_enter_date,
    admission_dept_id,
    admission_campus.dept_key as admission_dept_key,
    admission_campus.department_name as admission_department,
    admission_department_group,
    case
        when admission_department_group = 'CPRU'
        then '104'
        else  admission_campus.department_center_id
    end as admission_department_center_id,
    case
        when admission_department_group = 'CPRU'
        then 'PHL IP Cmps'
        else admission_campus.department_center_abbr
    end as admission_department_center_abbr,
    coalesce(admission_service, 'NOT APPLICABLE') as admission_service,
    case
        when discharged_ind = 1
        then discharge_campus.dept_key
    end as discharge_dept_key,
    case
        when discharged_ind = 1
        then discharge_dept_id
    end as discharge_dept_id,
    case
        when discharged_ind = 1
        then discharge_campus.department_name
    end as discharge_department,
    case
        when discharged_ind = 1
        then discharge_department_group
    end as discharge_department_group,
    case
        when discharge_department_group = 'CPRU'
            and discharged_ind = 1
        then '104'
        when discharged_ind = 1
        then discharge_campus.department_center_id
    end as discharge_department_center_id,
    case
        when discharge_department_group = 'CPRU'
            and discharged_ind = 1
        then 'PHL IP Cmps'
        when discharged_ind = 1
        then discharge_campus.department_center_abbr
    end as discharge_department_center_abbr,
    case
        when discharged_ind = 1
        then coalesce(discharge_service, 'NOT APPLICABLE')
    end as discharge_service,
    inpatient.icu_ind,
    inpatient.ed_ind,
    inpatient.icu_los_days
from
    inpatient
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = inpatient.pat_enc_csn_id
    inner join {{ref('stg_department_all')}} as admission_campus
        on admission_campus.department_id = inpatient.admission_dept_id
    left join {{ref('stg_department_all')}} as discharge_campus
        on discharge_campus.department_id = inpatient.discharge_dept_id
where
    considered_ip = 1
