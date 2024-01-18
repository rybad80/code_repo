/*
This cohort only includes currently admitted patients and patients with a discharge date >= 2017/07/01
*/

select
    encounter_inpatient.visit_key,
    encounter_inpatient.admission_event_key as visit_event_key,
    encounter_inpatient.pat_key,
    encounter_inpatient.admission_dept_key as dept_key,
    encounter_inpatient.mrn,
    encounter_inpatient.csn,
    encounter_inpatient.patient_name,
    encounter_inpatient.dob,
    encounter_inpatient.admission_source,
    encounter_inpatient.admission_service,
    encounter_inpatient.hospital_admit_date,
    encounter_inpatient.inpatient_admit_date  as inpatient_census_admit_date,
    coalesce(encounter_inpatient.ed_ind, 0) as ed_ind,
    stg_capacity_discharge_order.event_action_dt_tm as discharge_order_date,
    encounter_inpatient.hospital_discharge_date,
    stg_treatment_teams.admission_team,
    admit_department_rollup.dept_nm as admission_department_name,
    admit_department_rollup.unit_dept_grp_abbr as admission_department_group_name,
    admit_department_rollup.loc_dept_grp_abbr as admission_location_group_name,
    admit_department_rollup.bed_care_dept_grp_abbr as admission_bed_care_group,
    admit_department_rollup.department_center_abbr as admission_department_center_abbr,
    stg_treatment_teams.discharge_team,
    encounter_inpatient.discharge_dept_key,
    discharge_department_rollup.dept_nm as discharge_department_name,
    discharge_department_rollup.unit_dept_grp_abbr as discharge_department_group_name,
    discharge_department_rollup.loc_dept_grp_abbr as discharge_location_group_name,
    discharge_department_rollup.bed_care_dept_grp_abbr as discharge_bed_care_group,
    encounter_inpatient.discharge_department_center_abbr,
    encounter_inpatient.discharge_service,
    extract( --noqa: PRS
        epoch from encounter_inpatient.hospital_discharge_date
            - stg_capacity_discharge_order.event_action_dt_tm
    ) / 60 as discharge_order_to_discharge_mins,
    case
        when encounter_inpatient.discharge_service in
                                ('General Surgery', 'Neurosurgery', 'Oral and Maxillofacial Surgery',
                                    'Orthopedics', 'Otolaryngology', 'Plastic Surgery', 'Trauma',
                                    'Trauma PICU', 'Trauma Surgery', 'Urology')
        then 247
        when discharge_department_rollup.bed_care_dept_grp_abbr like '%ICU'
        then 138
        else 109
    end as discharge_order_to_discharge_target,
    case
        when discharge_order_to_discharge_mins <= discharge_order_to_discharge_target
            and discharge_order_to_discharge_mins >= 0
        then 1
        when discharge_order_to_discharge_mins > discharge_order_to_discharge_target
            and discharge_order_to_discharge_mins >= 0
        then 0
    end as discharge_order_to_discharge_target_ind,
    case
        when hour(encounter_inpatient.hospital_discharge_date) < 12 then 1 else 0
    end as discharge_before_noon_ind,
    encounter_inpatient.inpatient_los_days,
    encounter_inpatient.hospital_los_days,
    round(lookup_drg_expected_los.drg_elos, 3) as expected_hospital_los_days,
    {{
        dateadd(
            datepart = 'day',
            interval = 'expected_hospital_los_days',
            from_date_or_timestamp = 'encounter_inpatient.hospital_admit_date'
            )
    }} as expected_discharge_date,
    round(encounter_inpatient.hospital_los_days / expected_hospital_los_days, 3) as los_elos_ratio,
    dim_hospital_drg.drg_name as drg
from
    {{ref('encounter_inpatient')}} as encounter_inpatient
    left join {{source('cdw_analytics', 'fact_department_rollup')}} as admit_department_rollup
        on admit_department_rollup.dept_key = encounter_inpatient.admission_dept_key
        and admit_department_rollup.dept_align_dt = date_trunc('day', encounter_inpatient.inpatient_admit_date)
    left join {{source('cdw_analytics', 'fact_department_rollup')}} as discharge_department_rollup
        on discharge_department_rollup.dept_key = encounter_inpatient.discharge_dept_key
        and discharge_department_rollup.dept_align_dt = date_trunc(
            'day', encounter_inpatient.hospital_discharge_date
        )
    left join {{ref('stg_capacity_expected_los')}} as stg_capacity_expected_los
        on stg_capacity_expected_los.hsp_account_id = encounter_inpatient.hsp_account_id
    left join {{ref('lookup_drg_expected_los')}} as lookup_drg_expected_los
        on lookup_drg_expected_los.drg_id = stg_capacity_expected_los.drg_id
        and lookup_drg_expected_los.drg_elos is not null
        and date_trunc(
            'month', encounter_inpatient.hospital_discharge_date
        ) != date_trunc('month', current_date)
    left join {{ref('stg_capacity_discharge_order')}} as stg_capacity_discharge_order
        on stg_capacity_discharge_order.pat_enc_csn_id = encounter_inpatient.csn
        and event_action_nm = 'Discharge Order'
    left join {{ref('dim_hospital_drg')}} as dim_hospital_drg
        on dim_hospital_drg.drg_id = stg_capacity_expected_los.drg_id
    left join {{ref('stg_treatment_teams')}} as stg_treatment_teams
        on stg_treatment_teams.visit_key = encounter_inpatient.visit_key
where
    coalesce(encounter_inpatient.hospital_discharge_date, current_date) >= '2017-07-01'
