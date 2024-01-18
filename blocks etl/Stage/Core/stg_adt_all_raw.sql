{{ config(meta = {
    'critical': true
}) }}

with dept_groups as ( --noqa: PRS
    select
        stg_department_all.dept_key,
        stg_department_all.department_name,
        fact_department_rollup_summary.mstr_dept_grp_unit_key,
        fact_department_rollup_summary.unit_dept_grp_abbr,
        fact_department_rollup_summary.bed_care_dept_grp_abbr,
        fact_department_rollup_summary.intended_use_dept_grp_abbr,
        coalesce(fact_department_rollup_summary.min_dept_align_dt, '1900-01-01') as min_date,
        coalesce(fact_department_rollup_summary.max_dept_align_dt, current_date) as max_date,
        row_number() over (partition by fact_department_rollup_summary.dept_key
            order by min_dept_align_dt) as depts_seq_num,
        case
            when fact_department_rollup_summary.always_count_for_census_ind = 1
            then 1
            when stg_department_all.department_name in (-- pre 2014 ip department names
            '3 CENTER', '7 EAST', '7 NORTH', '7 WEST', 'NICU EAST', 'NICU WEST')
            then 1
            else 0
        end as always_count_for_census_ind
    from
        {{ref('stg_department_all')}} as stg_department_all
        left join {{source('cdw_analytics', 'fact_department_rollup_summary')}} as fact_department_rollup_summary
            on fact_department_rollup_summary.dept_key = stg_department_all.dept_key
),

adt as (
    select
        visit_event.visit_event_key,
        visit_event.visit_key,
        visit_event.adt_event_id,
        visit_event.bed_key,
        master_bed.bed_id,
        master_bed.bed_nm as bed_name,
        visit_event.room_key,
        master_room.room_nm as room_name,
        visit_event.dept_key,
        dict_pat_svc.dict_nm as initial_service,
        visit_event.eff_event_dt as enter_date,
        coalesce(
            dept_groups.unit_dept_grp_abbr,
            dept_groups_imputation.unit_dept_grp_abbr,
            dept_groups.department_name,
            dept_groups_imputation.department_name
        ) as department_group_name,
        coalesce(
            dept_groups.mstr_dept_grp_unit_key,
            dept_groups_imputation.mstr_dept_grp_unit_key
        ) as department_group_key,
        coalesce(
            dept_groups.always_count_for_census_ind,
            dept_groups_imputation.always_count_for_census_ind,
            0
        ) as always_count_for_census_ind_new,
        coalesce(
            dept_groups.bed_care_dept_grp_abbr,
            dept_groups_imputation.bed_care_dept_grp_abbr)
        as bed_care_group,
        coalesce(
            dept_groups.intended_use_dept_grp_abbr,
            dept_groups_imputation.intended_use_dept_grp_abbr)
        as intended_use_group,
        lag(visit_event.bed_key) over (
            partition by visit_event.visit_key order by visit_event.eff_event_dt, visit_event.adt_event_id desc
        ) as prev_bed_key,
        lag(visit_event.dept_key) over (
            partition by visit_event.visit_key order by visit_event.eff_event_dt, visit_event.adt_event_id desc
        ) as prev_dept_key,
        lag(department_group_key) over (
            partition by visit_event.visit_key order by visit_event.eff_event_dt, visit_event.adt_event_id desc
        ) as prev_dept_grp_key,
        case
            when (master_bed.bed_key != prev_bed_key or prev_bed_key is null)
                or (visit_event.dept_key != prev_dept_key or prev_dept_key is null)
                or (department_group_key != prev_dept_grp_key or prev_dept_grp_key is null)
            then 1
            else 0
        end as new_bed_ind,
        case
            when (visit_event.dept_key != prev_dept_key or prev_dept_key is null)
                or (department_group_key != prev_dept_grp_key or prev_dept_grp_key is null)
            then 1
            else 0
        end as new_dept_ind,
        case
            when coalesce(department_group_key, visit_event.dept_key)
                != coalesce(prev_dept_grp_key, prev_dept_key, 0)
            then 1
            else 0
        end as new_dept_grp_ind
    from
        {{source('cdw', 'visit_event')}} as visit_event
        inner join {{source('cdw', 'master_bed')}} as master_bed
            on master_bed.bed_key = visit_event.bed_key
        inner join {{source('cdw', 'master_room')}} as master_room
            on master_room.room_key = visit_event.room_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_pat_svc
            on dict_pat_svc.dict_key = visit_event.dict_pat_svc_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_adt_event
            on dict_adt_event.dict_key = visit_event.dict_adt_event_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_event_subtype
            on dict_event_subtype.dict_key = visit_event.dict_event_subtype_key
        left join dept_groups
            on dept_groups.dept_key = visit_event.dept_key
            and date(visit_event.eff_event_dt) between dept_groups.min_date and dept_groups.max_date
        -- pre 2014 ip department names
        left join dept_groups as dept_groups_imputation
            on dept_groups_imputation.dept_key = visit_event.dept_key
            and dept_groups_imputation.depts_seq_num = 1
    where
        dict_adt_event.src_id in (1, 3) -- (Admission, Transfer In)
        and dict_event_subtype.src_id != 2 -- Canceled
),

bed_type as (
    select
        bed_metric_grouper.bed_id,
        group_concat(zc_cm_metric_grouper.name) as bed_type
    from
        {{source('clarity_ods','bed_metric_grouper')}} as bed_metric_grouper
        inner join {{source('clarity_ods','zc_cm_metric_grouper')}} as zc_cm_metric_grouper
            on zc_cm_metric_grouper.cm_metric_grouper_c = bed_metric_grouper.cm_metric_grouper_c
    group by
        bed_metric_grouper.bed_id
)

select
    adt.visit_event_key,
    adt.adt_event_id,
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.dob,
    visit.enc_id as csn,
    cast(
        coalesce(visit.eff_dt, visit.hosp_admit_dt, visit.appt_dt, master_date.full_dt) as date
    ) as encounter_date,
    visit.hosp_admit_dt as hospital_admit_date,
    visit.hosp_dischrg_dt as hospital_discharge_date,
    adt.bed_name,
    adt.room_name,
    stg_department_all.department_name,
    adt.department_group_name,
    adt.intended_use_group,
    adt.bed_care_group,
    adt.bed_key,
    bed_type.bed_type,
    adt.room_key,
    stg_department_all.dept_key,
    stg_department_all.department_id,
    stg_department_all.department_center,
    stg_department_all.department_center_id,
    stg_department_all.department_center_abbr,
    adt.department_group_key,
    adt.initial_service,
    adt.enter_date,
    adt.always_count_for_census_ind_new as ip_unit_ind,
    adt.prev_bed_key,
    adt.prev_dept_key,
    adt.prev_dept_grp_key,
    adt.new_bed_ind,
    adt.new_dept_ind,
    adt.new_dept_grp_ind,
    visit.create_by as visit_source_system,
    {{
        dbt_utils.surrogate_key([
            'floor(visit.enc_id)',
            'visit.pat_id',
            'visit.create_by'
        ])
    }} as encounter_key,
    adt.visit_key,
    visit.pat_key,
    stg_patient.patient_key,
    max(
        case when department_group_name = 'ED' then 1 else 0 end
    ) over (partition by visit.visit_key) as ed_ind,
    case
        when department_group_name in ('PICU', 'CICU', 'NICU')
        then 1
        when stg_department_all.department_name in ('NICU EAST', 'NICU WEST')
        then 1
        else 0
    end as icu_ind
from
    adt
    inner join {{ref('stg_department_all')}} as stg_department_all
        on stg_department_all.dept_key = adt.dept_key
    inner join {{source('cdw', 'visit')}} as visit
        on visit.visit_key = adt.visit_key
    left join {{source('cdw', 'master_date')}} as master_date
        on master_date.dt_key = visit.contact_dt_key
    inner join {{ref('stg_patient_ods')}} as stg_patient
        on stg_patient.pat_id = visit.pat_id
    left join bed_type
        on bed_type.bed_id = adt.bed_id
where
    visit.visit_key > 0
