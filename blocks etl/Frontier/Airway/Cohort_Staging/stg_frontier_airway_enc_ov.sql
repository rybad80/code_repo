-- office visit (visit types provided by the team)
with enc_ov_raw as (
    select
        stg_encounter.mrn,
        stg_encounter.pat_key,
        stg_encounter.visit_key,
        stg_encounter.visit_type_id,
        stg_encounter.encounter_date,
        provider.prov_id as provider_id,
        max(case when lookup_frontier_program_visit.category like '%tel' then 1 else 0 end) as telvisit_ind,
        max(case when lookup_frontier_program_providers_all.provider_type = 'airway team ent tel' then 1
            else 0 end) as tel_ent_prov,
        max(case when lookup_frontier_program_providers_all.provider_type = 'airway team gi tel' then 1
            else 0 end) as tel_gi_prov,
        max(case when lookup_frontier_program_providers_all.provider_type = 'airway team pul tel' then 1
            else 0 end) as tel_pul_prov,
        max(case when lookup_frontier_program_providers_all.provider_type = 'airway team slp'
            and lookup_frontier_program_visit.id = '4812' -- "voice evaluation" visit type
            then 1 else 0 end) as ve_spl_ind
    from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    inner join {{ ref('lookup_frontier_program_visit') }} as lookup_frontier_program_visit
        on stg_encounter.visit_type_id = cast(lookup_frontier_program_visit.id as nvarchar(20))
        and lookup_frontier_program_visit.program = 'airway'
    inner join {{ ref('lookup_frontier_program_providers_all') }} as lookup_frontier_program_providers_all
        on provider.prov_id = cast(lookup_frontier_program_providers_all.provider_id as nvarchar(20))
        and lookup_frontier_program_providers_all.program = 'airway'
        and lookup_frontier_program_providers_all.provider_type like 'airway team%'
    inner join {{ ref('stg_frontier_airway_dx_hx') }} as dx_hx
        on stg_encounter.pat_key = dx_hx.pat_key
        and dx_hx.earliest_dx_date <= stg_encounter.encounter_date
    where stg_encounter.encounter_date between '2017-07-01' and current_date
        and lower(stg_encounter.department_name) like '%bgr%' -- airway center office visits occur in buerger
        and lower(stg_encounter.appointment_status) not in ('canceled', 'no show')
        and (lookup_frontier_program_visit.category not like '%tel'
            or (lookup_frontier_program_visit.category like '%tel'
                and lookup_frontier_program_providers_all.provider_type like '%tel'
                )
            )
    group by
        stg_encounter.mrn,
        stg_encounter.pat_key,
        stg_encounter.visit_key,
        stg_encounter.visit_type_id,
        stg_encounter.encounter_date,
        provider.prov_id
),
--Special rule for “telephone visit”, “video visit follow up” and “video visit new” visit types:
    --If the same patient had a visit with ENT and GI or Pulmonary, then include these visits. 
enc_ov_tel_pat as (
	select
		mrn,
		pat_key,
		max(tel_ent_prov) as tel_ent_prov,
		max(tel_gi_prov) as tel_gi_prov,
		max(tel_pul_prov) as tel_pul_prov
	from enc_ov_raw
	where telvisit_ind = 1
	group by
		mrn,
		pat_key
	having (max(tel_ent_prov) = 1 and max(tel_gi_prov) = 1) or (max(tel_ent_prov) = 1 and max(tel_pul_prov) = 1)
),
--Special rule for "voice evaluation" visit type:
    -- if the patient only seen for a speech therapist for a voice evaluation but no other visit, then exclude.
enc_ov_slp_only_pat as (
	select
		mrn,
		pat_key
	from enc_ov_raw
	group by
		mrn,
		pat_key
	having max(ve_spl_ind) = 1 and min(ve_spl_ind) = 1
)

select enc_ov_raw.mrn,
        enc_ov_raw.pat_key,
        enc_ov_raw.visit_key,
        enc_ov_raw.visit_type_id,
        enc_ov_raw.encounter_date
	from enc_ov_raw as enc_ov_raw
	left join enc_ov_tel_pat as enc_ov_tel_pat
		on enc_ov_raw.mrn = enc_ov_tel_pat.mrn
	left join enc_ov_slp_only_pat as enc_ov_slp_only_pat
		on enc_ov_raw.mrn = enc_ov_slp_only_pat.mrn
	where (enc_ov_raw.telvisit_ind = 0 or enc_ov_tel_pat.mrn is not null)
		and enc_ov_slp_only_pat.mrn is null
