with
admit_to_6fl_tmp as (
    select i.pat_key,
		adt_department.mrn,
		i.visit_key,
		adt_department.hospital_discharge_date,
		min(adt_department.enter_date) as fl6_enter_date,
		min(case when adt_department.department_group_name = 'CCU' then adt_department.enter_date else null end)
			as fl6_ccu_enter_date,
		max(case when adt_department.department_group_name = 'CCU'
			then coalesce(adt_department.exit_date, current_timestamp)
			else null end) as fl6_ccu_exit_date,
		max(case when adt_department.exit_date is null then 1 else 0 end) as currently_in_fl6_ind,
		max(case when adt_department.department_group_name = 'CCU' then 1 else 0 end) as ccu_ind,
		max(case when adt_department.department_group_name = 'CICU' then 1 else 0 end) as cicu_ind
	from {{ ref('stg_encounter_inpatient') }}  as i
	inner join {{source('cdw', 'visit_addl_info')}} as visit_addl_info
        on visit_addl_info.visit_key = i.visit_key
	inner join {{ ref('adt_department') }} as adt_department
		on i.visit_key = adt_department.visit_key
	where
		adt_department.department_name in ('6 EAST', '6 SOUTH TOWER', '6 NORTHEAST')
		and adt_department.department_group_name in ('CCU', 'CICU')
	group by
		i.pat_key,
		adt_department.mrn,
		i.visit_key,
		adt_department.hospital_discharge_date
	having
		max(adt_department.exit_date) >= '11/28/2022' or currently_in_fl6_ind = 1
)

select
	ad.pat_key,
	ad.mrn,
	ad.visit_key,
	ad.hospital_discharge_date,
	ad.fl6_enter_date,
	ad.ccu_ind,
	ad.cicu_ind,
	ad.fl6_ccu_enter_date,
	ad.fl6_ccu_exit_date,
	ad.currently_in_fl6_ind
from admit_to_6fl_tmp as ad
inner join {{source('cdw','visit_provider_hist')}} as vph
	on ad.visit_key = vph.visit_key
inner join {{source('cdw','provider')}} as p
	on vph.prov_key = p.prov_key
inner join {{ ref('lookup_frontier_program_providers_all') }} as lk_prov
	on p.prov_id = lk_prov.provider_id
	and lk_prov.program = 'act-hf' and lk_prov.provider_type = 'hf attending'
where
	ad.ccu_ind = 1
group by
	ad.pat_key,
	ad.mrn,
	ad.visit_key,
	ad.hospital_discharge_date,
	ad.fl6_enter_date,
	ad.ccu_ind,
	ad.cicu_ind,
	ad.fl6_ccu_enter_date,
	ad.fl6_ccu_exit_date,
	ad.currently_in_fl6_ind
having
	(min(vph.attnd_from_dt) between ad.fl6_ccu_enter_date and ad.fl6_ccu_exit_date)
	or (max(vph.attnd_to_dt) between ad.fl6_ccu_enter_date and ad.fl6_ccu_exit_date)
	or (min(vph.attnd_from_dt) < ad.fl6_ccu_enter_date and max(vph.attnd_to_dt) > ad.fl6_ccu_exit_date)
union all
select
	ad.pat_key,
	ad.mrn,
	ad.visit_key,
	ad.hospital_discharge_date,
	ad.fl6_enter_date,
	ad.ccu_ind,
	ad.cicu_ind,
	ad.fl6_ccu_enter_date,
	ad.fl6_ccu_exit_date,
	ad.currently_in_fl6_ind
from admit_to_6fl_tmp as ad
where ad.ccu_ind = 0
