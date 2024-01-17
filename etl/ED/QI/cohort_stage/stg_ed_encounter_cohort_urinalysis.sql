select
    stg_encounter_ed.visit_key,
    stg_encounter_ed.pat_key,
    'URINALYSIS' as cohort,
	max(case
		when procedure_order_clinical.procedure_id in ('5767', '13436', '15141' )then 'UA_IND'
		else 'POCT_IND'
	end) as subcohort
from
	{{ ref('stg_encounter_ed') }} as stg_encounter_ed
inner join {{ ref('procedure_order_clinical') }} as procedure_order_clinical  on
	stg_encounter_ed.visit_key = procedure_order_clinical.visit_key
	where
    year(stg_encounter_ed.encounter_date) >= year(current_date) - 5
	--last 5 years--
	and stg_encounter_ed.age_days between 57 and 6570
	and procedure_order_clinical.procedure_id in (
        '5767', --URINALYSIS BY DIP (OFFICE)--
		'13436', --URINALYSIS RFLX TO MICROSCOPIC--
		'15141', --URINALYSIS, ROUTINE   -LC--
		'16606') --POC 10 SG URINE DIPSTICK-- --USE PROCEDURE_IDS--
	and procedure_order_clinical.specimen_taken_date is not null
    and procedure_order_clinical.placed_date between
    stg_encounter_ed.ed_arrival_date and stg_encounter_ed.ed_discharge_date
group by
	stg_encounter_ed.visit_key,
	stg_encounter_ed.pat_key
