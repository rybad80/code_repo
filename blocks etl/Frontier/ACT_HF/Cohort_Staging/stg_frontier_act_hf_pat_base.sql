select mrn,
	min(hospital_discharge_date) as min_hospital_discharge_date,
	max(sp_vad_ind) as vad_pat_ind
from {{ ref('stg_frontier_act_hf_inpat_enc') }}
group by mrn
