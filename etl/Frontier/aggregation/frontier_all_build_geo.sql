select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_act_hf_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_airway_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_ccpm_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_ctis_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_cva_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_drof_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_engin_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_food_allergy_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_heart_valve_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_hyperinsulinism_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_id_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_lymphatics_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_minds_matter_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_n_o_t_bleeding_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_motility_base') }}
where
    metric_level is not null
union all
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_rare_lung_base') }}
where
    metric_level is not null
union
select
    program_name,
    mrn,
    pat_key,
    metric_name,
    metric_level,
    visit_cat,
    mailing_state,
    mailing_city,
    num
from {{ ref('stg_thyroid_base') }}
where
    metric_level is not null
