with active_registry as (--region active in GLOMERULAR REGISTRY
    select
        registry_data_membership.pat_key
    from
        {{source('cdw', 'registry_data_membership')}} as registry_data_membership
        inner join {{source('cdw', 'dim_registry_status')}} as dim_registry_status
            on registry_data_membership.dim_registry_status_key = dim_registry_status.dim_registry_status_key
        inner join {{source('cdw', 'registry_configuration')}} as registry_configuration
            on registry_data_membership.registry_config_key = registry_configuration.registry_config_key
    where
        registry_id = '100136'  --enter your registrty id
        and lower(registry_status_nm) = 'active'
--end region
),
reg_metrics as (--region metrics history. Need Glomerular registry + registry metric logic to combine 
                --for GLEAN cohort
    select
        registry_data_info.pat_key,
        rule_id,
        rule_nm,
        metric_string_value,
        metric_last_upd_dt,
        row_number() over (partition by registry_data_info.pat_key, rule_id
                            order by metric_last_upd_dt desc) as seq
    from
        {{source('cdw', 'registry_data_info')}} as registry_data_info
        inner join {{source('cdw', 'registry_metric_history')}} as registry_metric_history
            on registry_data_info.record_key = registry_metric_history.record_key
        inner join {{source('cdw', 'master_charge_edit_rule')}} as master_charge_edit_rule
            on registry_metric_history.mstr_chrg_edit_rule_key = master_charge_edit_rule.mstr_chrg_edit_rule_key
),
metrics_current as (--region most recent metric per patient
    select
        pat_key,
        max(case when rule_id = '1380979' then metric_string_value else null end) as neph_count_ind,
        max(case when rule_id = '1382495' then metric_string_value else null end) as transfer_ind,
        max(case when rule_id = '1386336' then metric_string_value else null end) as dialysis_ind
    from
        reg_metrics
    where
        seq = 1
    group by
        pat_key
--end region
),
phenotype_sde as (--region phenotype sde used
    select
        pat_key,
        1 as phenotype_ind
    from
        {{ref('smart_data_element_all')}}
    where
        concept_id = 'CHOPNEPHRO#004' --phenotype sde completed
    group by
        pat_key
--end region
),
monthly_cohort as (--region the cohort at the current time
    select
        active_registry.pat_key,
        stg_patient.mrn,
        neph_count_ind,
        phenotype_ind,
        transfer_ind,
        dialysis_ind
    from
        active_registry as active_registry
        left join metrics_current as metrics_current
            on active_registry.pat_key = metrics_current.pat_key
        left join phenotype_sde as phenotype_sde
            on active_registry.pat_key = phenotype_sde.pat_key
        inner join {{ref('stg_patient') }} as stg_patient
            on active_registry.pat_key = stg_patient.pat_key
    where
        (neph_count_ind = 1 or phenotype_ind = 1) -- need to add or phenotype exists (TBD)
        and dialysis_ind is null --exclude dialysis past 30 days
        and transfer_ind is null
--end region
),
cohort_metrics as (--region all reg metrics
    select
        monthly_cohort.*,
        row_number() over (partition by monthly_cohort.pat_key, rule_id
                            order by metric_last_upd_dt desc) as seq,
        rule_id,
        rule_nm,
        metric_last_upd_dt,
        metric_string_value as val
    from
        monthly_cohort
        inner join {{source('cdw', 'registry_data_info')}} as registry_data_info
            on monthly_cohort.pat_key = registry_data_info.pat_key
        inner join {{source('cdw', 'registry_metric_history')}} as registry_metric_history
            on registry_data_info.record_key = registry_metric_history.record_key
        inner join {{source('cdw', 'master_charge_edit_rule')}} as master_charge_edit_rule
            on registry_metric_history.mstr_chrg_edit_rule_key = master_charge_edit_rule.mstr_chrg_edit_rule_key
    where
        rule_id in (--region
        '1387066', -- chop dm glom last nephrology visit (3y)
        '1387067', -- chop dm glom last nephrology provider (3y)
        '1017191', -- chop dm ckd next nephrology appt
        '1386535', -- chop dm glom primary dx last neph visit
        '1386009', -- chop dm glom remission status
        '1386811', -- chop dm glom remission status date
        '1017522', -- chop dm last urinalysis protein
        '644363', -- chop dm last urinalysis date
        '1386358', -- chop dm glom number of hospital admissions (30 days)
        '1386363', -- chop dm glom recent admission length of stay (30 days)
        '1389514', -- chop dm last ed/hosp discharge date prior to readmission
        '1387274', -- chop dm covid19 most recent vaccine date
        '642861', -- chop dm imm last flu vaccine
        '1386337', -- chop dm glom most recent pneumovax
        '1386339', -- chop dm glom 2nd most recent pneumovax
        '1386340', -- chop dm glom most recent prevnar
        '1386341', -- chop dm glom 2nd most recent prevnar
        '1386343', -- chop dm glom 3rd most recent prevnar
        '1386345', -- chop dm glom 4th most recent prevnar
        '1387086' -- chop dm glom last nephrology department (3y)
    --end region
        )
--end region
)

select
    *
from
    cohort_metrics
