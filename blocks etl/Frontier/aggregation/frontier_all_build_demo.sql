select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_act_hf_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_airway_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_ccpm_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_ctis_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_cva_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_drof_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_engin_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_food_allergy_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_heart_valve_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_hyperinsulinism_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_id_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_lymphatics_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_minds_matter_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_motility_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_n_o_t_bleeding_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_rare_lung_base') }}
union all
select
    program_name,
    sub_cohort,
    metric_level as metric_level_fy,
    mrn,
    visit_key,
    pat_key
from
    {{ ref('stg_thyroid_base') }}
