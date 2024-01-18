{{ config(meta = {
    'critical': true
}) }}

with ccc_dx_keys as (
    select
        epic_grouper_item.epic_grouper_id,
        epic_grouper_item.epic_grouper_disp_nm,
        epic_grouper_diagnosis.dx_key,
        master_diagnosis.dx_id,
        case
            when epic_grouper_item.epic_grouper_id = 101291 then 'HEMATU'
            when epic_grouper_item.epic_grouper_id = 101295 then 'RENAL'
            when epic_grouper_item.epic_grouper_id = 101290 then 'GI'
            when epic_grouper_item.epic_grouper_id = 101297 then 'MALIGNANCY'
            when epic_grouper_item.epic_grouper_id = 101292 then 'METABOLIC'
            when epic_grouper_item.epic_grouper_id = 101293 then 'NEONATAL'
            when epic_grouper_item.epic_grouper_id = 101289 then 'CONGENI GENETIC'
            when epic_grouper_item.epic_grouper_id = 101296 then 'RESP'
            when epic_grouper_item.epic_grouper_id = 101288 then 'CVD'
            when epic_grouper_item.epic_grouper_id = 101294 then 'NEUROMUSC'
        end as dx_ccc_group
    from
        {{source('cdw', 'epic_grouper_item')}} as epic_grouper_item
        inner join {{source('cdw', 'epic_grouper_diagnosis')}} as epic_grouper_diagnosis
            on epic_grouper_diagnosis.epic_grouper_key = epic_grouper_item.epic_grouper_key
        inner join {{source('cdw','master_diagnosis')}} as master_diagnosis
            on master_diagnosis.dx_key = epic_grouper_diagnosis.dx_key
    where
        epic_grouper_item.epic_grouper_id in (
            101292, --'complex metabolic/endocrine disease'
            101291, --'complex hematologic/immunologic disease'
            101289, --'complex congental anomalies'
            101297, --'complex malignancy'
            101290, --'complex chronic gi illness'
            101293, --'complex illness related to prematurity'
            101296, --'complex chronic respiratory disease'
            101288, --'complex chronic cardiovascular disease'
            101295, --'complex chronic renal disease'
            101294  --'complex chronic neuromuscular disease'
        )
),

tech_dx_keys as (
    select
        epic_grouper_diagnosis.dx_key,
        master_diagnosis.dx_id
    from
        {{source('cdw', 'epic_grouper_item')}} as epic_grouper_item
        inner join {{source('cdw', 'epic_grouper_diagnosis')}} as epic_grouper_diagnosis
            on epic_grouper_diagnosis.epic_grouper_key = epic_grouper_item.epic_grouper_key
        inner join {{source('cdw','master_diagnosis')}} as master_diagnosis
            on master_diagnosis.dx_key = epic_grouper_diagnosis.dx_key
    where
        epic_grouper_item.epic_grouper_id = 103892 --'technology dependent grouper' 
),

tech_med_keys as (
    select
        epic_grouper_medication.med_key
    from
        {{source('cdw', 'epic_grouper_item')}} as epic_grouper_item
        inner join {{source('cdw', 'epic_grouper_medication')}} as epic_grouper_medication
            on epic_grouper_medication.epic_grouper_key = epic_grouper_item.epic_grouper_key
    where
        epic_grouper_item.epic_grouper_id = 103893 --'chop erx tech dependence'
),

tech_proc_keys as (
    select
        epic_grouper_procedure.proc_key
    from
        {{source('cdw', 'epic_grouper_item')}} as epic_grouper_item
        inner join {{source('cdw', 'epic_grouper_procedure')}} as epic_grouper_procedure
            on epic_grouper_procedure.epic_grouper_key = epic_grouper_item.epic_grouper_key
    where
        epic_grouper_item.epic_grouper_id = 103895  --'chop eap tech dependence'
),

ccc_tech_timeframes_union as (
    select
        stg_encounter.pat_key,
        ccc_dx_keys.dx_ccc_group as ccc_group,
        'VISIT DIAGNOSIS' as ccc_source,
        ccc_dx_keys.dx_key,
        null as med_key,
        null as proc_key,
        stg_encounter.encounter_date as start_dt,
        stg_encounter.encounter_date + cast('12 months' as interval) - cast('1 day' as interval) as end_dt
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{ref('stg_dx_visit_diagnosis_long')}} as stg_dx_visit_diagnosis_long
            on stg_dx_visit_diagnosis_long.pat_enc_csn_id = stg_encounter.csn
        inner join ccc_dx_keys
            on ccc_dx_keys.dx_id = stg_dx_visit_diagnosis_long.dx_id
    group by
        stg_encounter.pat_key,
        ccc_dx_keys.dx_ccc_group,
        ccc_dx_keys.dx_key,
        stg_encounter.encounter_date
    union all
    select
        patient_problem_list.pat_key,
        ccc_dx_keys.dx_ccc_group as ccc_group,
        'PROBLEM DIAGNOSIS' as ccc_source,
        ccc_dx_keys.dx_key,
        null as med_key,
        null as proc_key,
        coalesce(patient_problem_list.noted_dt, stg_encounter.encounter_date, stg_patient.dob) as start_dt,
        coalesce(patient_problem_list.rslvd_dt, current_date) as end_dt
    from
        {{source('cdw', 'patient_problem_list')}} as patient_problem_list
        inner join {{ref('stg_patient')}} as stg_patient
            on stg_patient.pat_key = patient_problem_list.pat_key
        left join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = patient_problem_list.visit_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_prob_stat
            on dict_prob_stat.dict_key = patient_problem_list.dict_prob_stat_key
        inner join ccc_dx_keys
            on ccc_dx_keys.dx_key = patient_problem_list.dx_key
    where
        dict_prob_stat.src_id != 3 --'Deleted'
    group by
        patient_problem_list.pat_key,
        ccc_dx_keys.dx_ccc_group,
        ccc_dx_keys.dx_key,
        patient_problem_list.noted_dt,
        stg_encounter.encounter_date,
        patient_problem_list.rslvd_dt,
        stg_patient.dob
    union all
    select
        stg_encounter.pat_key,
        'TECH DEPENDENT' as ccc_group,
        'VISIT DIAGNOSIS' as ccc_source,
        tech_dx_keys.dx_key,
        null as med_key,
        null as proc_key,
        stg_encounter.encounter_date as start_dt,
        stg_encounter.encounter_date + cast('12 months' as interval) - cast('1 day' as interval) as end_dt
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{ref('stg_dx_visit_diagnosis_long')}} as stg_dx_visit_diagnosis_long
            on stg_dx_visit_diagnosis_long.pat_enc_csn_id = stg_encounter.csn
        inner join tech_dx_keys on tech_dx_keys.dx_id = stg_dx_visit_diagnosis_long.dx_id
    group by
        stg_encounter.pat_key,
        tech_dx_keys.dx_key,
        stg_encounter.encounter_date
    union all
    select
        patient_problem_list.pat_key,
        'TECH DEPENDENT' as ccc_group,
        'PROBLEM DIAGNOSIS' as ccc_source,
        tech_dx_keys.dx_key,
        null as med_key,
        null as proc_key,
        coalesce(patient_problem_list.noted_dt, stg_encounter.encounter_date, stg_patient.dob) as start_dt,
        coalesce(patient_problem_list.rslvd_dt, current_date) as end_dt
    from
        {{source('cdw', 'patient_problem_list')}} as patient_problem_list
        inner join {{ref('stg_patient')}} as stg_patient
            on stg_patient.pat_key = patient_problem_list.pat_key
        left join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = patient_problem_list.visit_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_prob_stat
            on dict_prob_stat.dict_key = patient_problem_list.dict_prob_stat_key
        inner join tech_dx_keys
            on tech_dx_keys.dx_key = patient_problem_list.dx_key
    where
        dict_prob_stat.src_id != 3 --'Deleted'
    group by
        patient_problem_list.pat_key,
        tech_dx_keys.dx_key,
        patient_problem_list.noted_dt,
        patient_problem_list.rslvd_dt,
        stg_encounter.encounter_date,
        stg_patient.dob
    union all
    select
        stg_encounter.pat_key,
        'TECH DEPENDENT' as ccc_group,
        'MEDICATION ORDER' as ccc_source,
        null as dx_key,
        tech_med_keys.med_key as med_key,
        null as proc_key,
        medication_order.med_ord_create_dt as start_dt,
        medication_order.med_ord_create_dt + cast('12 months' as interval) as end_dt
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{source('cdw', 'medication_order')}} as medication_order
            on medication_order.visit_key = stg_encounter.visit_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_ord_mode
            on dict_ord_mode.dict_key = medication_order.dict_ord_mode_key
        inner join tech_med_keys
            on tech_med_keys.med_key = medication_order.med_key
    where
        dict_ord_mode.src_id = 1 --ordered in outpatient setting
        and medication_order.med_ord_create_dt is not null
    group by
        stg_encounter.pat_key,
        tech_med_keys.med_key,
        medication_order.med_ord_create_dt
    union all
    select
        stg_encounter.pat_key,
        'TECH DEPENDENT' as ccc_group,
        'PROCEDURE ORDER' as ccc_source,
        null as dx_key,
        null as med_key,
        tech_proc_keys.proc_key,
        procedure_order.proc_ord_create_dt as start_dt,
        procedure_order.proc_ord_create_dt + cast('12 months' as interval) as end_dt
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{source('cdw', 'procedure_order')}} as procedure_order
            on procedure_order.visit_key = stg_encounter.visit_key
        inner join tech_proc_keys
            on tech_proc_keys.proc_key = procedure_order.proc_key
    where
        procedure_order.proc_ord_create_dt is not null
    group by
        stg_encounter.pat_key,
        tech_proc_keys.proc_key,
        procedure_order.proc_ord_create_dt
),

next_dates as (
    select
        *,
        lead(start_dt) over (
            partition by pat_key, dx_key, med_key, proc_key, ccc_group, ccc_source order by start_dt
        ) as next_start_date,
        lead(end_dt) over (
            partition by pat_key, dx_key, med_key, proc_key, ccc_group, ccc_source order by start_dt
        ) as next_end_date,
        case when next_start_date between start_dt and end_dt then 0 else 1 end as end_of_run
    from
        ccc_tech_timeframes_union
),

start_of_run as (
    select
        *,
        case
            when lag(end_of_run) over (
                partition by pat_key, dx_key, med_key, proc_key, ccc_group, ccc_source order by start_dt
            ) is null then 1
            else lag(end_of_run) over (
                partition by pat_key, dx_key, med_key, proc_key, ccc_group, ccc_source order by start_dt
            )
        end as start_of_run
    from
        next_dates
),

cume_sum as (
    select
        *,
        sum(start_of_run) over (
            partition by pat_key, dx_key, med_key, proc_key, ccc_group, ccc_source
            order by start_dt asc rows unbounded preceding
        ) as cume_sum
    from
        start_of_run
)

select
    pat_key,
    ccc_group,
    ccc_source,
    dx_key,
    med_key,
    proc_key,
    min(start_dt) as start_date,
    max(end_dt) as end_date
from
    cume_sum
group by
    pat_key,
    ccc_group,
    ccc_source,
    dx_key,
    med_key,
    proc_key,
    cume_sum
