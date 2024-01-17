{{ config(meta = {
    'critical': true
}) }}

select
	stg_encounter.visit_type,
	stg_encounter.visit_type_id,
	appt_block.name as appointment_block,
	appt_block.internal_id as appointment_block_id,
	max(stg_encounter.encounter_date) as max_visit_type_date,
	max(case when stg_scheduling_npv_visit_type.visit_type is not null
		then 1
		else 0 end) as new_patient_visit_block_ind,
	max(case when upper(appt_block.is_active_yn) = 'Y' then 1 else 0 end) as block_active_ind
from {{source('clarity_ods', 'pat_enc')}} as pat_enc
inner join {{source('clarity_ods', 'all_categories')}} as appt_block
	on pat_enc.appt_block_c = appt_block.value_c
inner join {{ ref('stg_encounter') }} as stg_encounter on pat_enc.pat_enc_csn_id = stg_encounter.csn
left join {{ ref('stg_scheduling_npv_visit_type') }} as stg_scheduling_npv_visit_type
	on stg_encounter.visit_type_id = stg_scheduling_npv_visit_type.visit_type_id
where
	upper(appt_block.ini) = 'EPT'
	and appt_block.item = 7060
group by
	stg_encounter.visit_type,
	stg_encounter.visit_type_id,
	appt_block.name,
	appt_block.internal_id
