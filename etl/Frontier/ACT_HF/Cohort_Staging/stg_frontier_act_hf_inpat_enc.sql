select
	admit_all.mrn,
	admit_all.visit_key,
	admit_all.hospital_discharge_date,
	admit_all.fl6_enter_date,
	admit_all.currently_in_fl6_ind,
	admit_all.ccu_ind,
	admit_all.cicu_ind,
	dx_pat.sp_vad_ind,
	case when admit_consult.visit_key is not null then 1 else 0 end as consult_hf_prov_ind
from {{ ref('stg_frontier_act_hf_admit_all') }} as admit_all
inner join {{ ref('stg_frontier_act_hf_dx_pat') }} as dx_pat
	on admit_all.mrn = dx_pat.mrn
left join {{ ref('stg_frontier_act_hf_admit_consult') }} as admit_consult
	on admit_all.visit_key = admit_consult.visit_key
where admit_all.ccu_ind = 1
	or (admit_all.cicu_ind = 1 and dx_pat.sp_vad_ind = 1)
	or (admit_all.cicu_ind = 1 and admit_consult.visit_key is not null)
