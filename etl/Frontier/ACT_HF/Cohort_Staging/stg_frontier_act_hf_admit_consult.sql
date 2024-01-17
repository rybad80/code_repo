select ip_enc.mrn,
    ip_enc.visit_key
from {{ ref('stg_frontier_act_hf_admit_all') }} as ip_enc
inner join {{ ref('note_edit_metadata_history') }} as ne
    on ip_enc.visit_key = ne.visit_key
inner join {{source('cdw', 'employee')}} as emp
    on ne.final_author_emp_key = emp.emp_key
inner join {{source('cdw', 'provider')}} as p
    on emp.prov_key = p.prov_key
inner join {{ ref('lookup_frontier_program_providers_all') }} as lk_prov
	on p.prov_id = lk_prov.provider_id
	and lk_prov.program = 'act-hf' and lk_prov.provider_type = 'hf attending'
where
    ne.last_edit_ind = 1
    and note_type_id = '400005' --CONSULT_NOTE
    and ne.version_author_service_id = '5' --CARDIOLOGY
    and ne.note_status in ('SIGNED', 'ADDENDUM')
group by
    ip_enc.mrn,
    ip_enc.visit_key
