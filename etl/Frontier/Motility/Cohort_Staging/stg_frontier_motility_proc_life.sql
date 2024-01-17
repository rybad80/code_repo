select
    procedure_order_all.pat_key,
    procedure_order_all.mrn,
    max(case when
        lookup_frontier_program_procedures.category like 'general-motility%'
        then 1 else 0 end) as gen_mot_cpt_ind,
    max(case when
        lookup_frontier_program_procedures.category like 'defecation-disorder%'
        then 1 else 0 end) as def_dis_cpt_ind,
    max(case when
        lookup_frontier_program_procedures.category like 'multi-disciplinary%'
        then 1 else 0 end) as multi_d_cpt_ind,
    max(case when
        lookup_frontier_program_procedures.category like 'neuromodulation%'
        then 1 else 0 end) as neuromod_cpt_ind
from
    {{ ref('procedure_order_all') }} as procedure_order_all
    inner join {{ ref('lookup_frontier_program_procedures') }} as lookup_frontier_program_procedures
        on lower(procedure_order_all.cpt_code) = cast(lookup_frontier_program_procedures.id as nvarchar(20))
            and lookup_frontier_program_procedures.program = 'motility'
group by
    procedure_order_all.pat_key,
    procedure_order_all.mrn
