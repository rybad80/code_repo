select
	dx_hx.mrn,
	dx_hx.pat_key
from {{ ref('stg_frontier_airway_dx_hx') }} as dx_hx
left join {{ ref('stg_frontier_airway_enc_procedure') }} as enc_proc
	on enc_proc.pat_key = dx_hx.pat_key
left join {{ ref('stg_frontier_airway_enc_ov') }} as enc_ov
	on dx_hx.pat_key = enc_ov.pat_key
where
	enc_proc.pat_key is not null or enc_ov.pat_key is not null
group by
	dx_hx.mrn,
    dx_hx.pat_key
