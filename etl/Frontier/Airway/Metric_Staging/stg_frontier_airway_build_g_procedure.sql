with allcpts as (
    select proc_cpt.mrn,
            proc_cpt.pat_key,
            proc_cpt.service_date,
            proc_cpt.cpt_code,
            proc_cpt.provider_id,
            proc_cpt.provider_name,
            proc_cpt.mlb_ind
    from {{ ref('stg_frontier_airway_enc_proc_cpt')}} as proc_cpt
    inner join {{ ref('frontier_airway_encounter_cohort')}}  as cohort
        on proc_cpt.visit_key = cohort.visit_key
        and cohort.procedure_ind = 1
    group by
        proc_cpt.mrn,
        proc_cpt.pat_key,
        proc_cpt.service_date,
        proc_cpt.cpt_code,
        proc_cpt.provider_id,
        proc_cpt.provider_name,
        proc_cpt.mlb_ind
)
select distinct
    'Program-Specific: Procedures' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'allcpts.pat_key',
            'allcpts.service_date',
            'allcpts.cpt_code'
        ])
    }} as primary_key,
    allcpts.mrn,
    allcpts.pat_key,
    allcpts.service_date,
    allcpts.cpt_code,
    allcpts.provider_id,
    case when allcpts.mlb_ind = 1 then 'MLB, '||initcap(lookup_fp_procedure.label)
         else initcap(lookup_fp_procedure.label) end as drill_down_one,
    initcap(allcpts.provider_name) as drill_down_two,
    allcpts.service_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_airway_procedure' as metric_id,
    primary_key as num
from allcpts as allcpts
inner join {{ref('lookup_frontier_program_procedures')}} as lookup_fp_procedure
	on lower(allcpts.cpt_code) = cast(lookup_fp_procedure.id as nvarchar(20))
	and lookup_fp_procedure.program = 'airway'
