with
minds_matter_first as (--region
    select
        mrn,
        min(encounter_date) as first_mm_date
    from
        {{ ref('stg_minds_matter_all') }}
    where
        minds_matter_patient_ind = 1
    group by
        mrn
    --end region
),
minds_matter_occt_n_pt as (--region
    select
        stg_encounter.visit_key,
        stg_encounter.mrn,
        '1' as minds_matter_pt_occt_ind
    from
        minds_matter_first
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on minds_matter_first.mrn = stg_encounter.mrn
                and minds_matter_first.first_mm_date <= stg_encounter.encounter_date
    where
        regexp_like(lower(stg_encounter.department_name),
                'occ ther|
                |phy.*ther')
            and lower(stg_encounter.visit_type) like '%conc%'
    --end region
),
minds_matter_build as (
    select
        stg_encounter.visit_key,
        stg_encounter.mrn,
        stg_encounter.csn,
        stg_encounter.patient_name,
        stg_encounter.encounter_date,
        minds_matter_all.fiscal_year,
        case
            when encounter_primary_care.visit_key is not null
            and stg_encounter.appointment_status_id in (
                '-2',   --not applicable
                '6',    --arrived
                '2',    --completed
                '1'     --scheduled
                )
            then '1'
            else '0'
        end as primary_care_visit_ind,
        case
            when encounter_ed.visit_key is not null
            or lower(stg_encounter.department_name) like '%urgent care%'
            then '1'
            else '0'
        end as ed_encounter_ind,
        minds_matter_all.minds_matter_dx_ind,
        minds_matter_all.minds_matter_sde_ind,
        minds_matter_all.minds_matter_visit_type_ind,
        minds_matter_all.minds_matter_reason_visit_ind,
        initcap(provider.full_nm) as provider_name,
        provider.prov_id as provider_id,
        stg_encounter.department_name,
        stg_encounter.department_id,
        stg_encounter.visit_type,
        stg_encounter.visit_type_id,
        stg_encounter.encounter_type,
        stg_encounter.encounter_type_id,
        case when minds_matter_all.minds_matter_patient_ind = '1' then '1'
            else '0' end as minds_matter_patient_ind,
        case when minds_matter_occt_n_pt.minds_matter_pt_occt_ind = '1' then '1'
            else '0' end as minds_matter_pt_occt_ind,
        case when minds_matter_all.minds_matter_patient_ind = 1 then 'Specialty Care'
            when ed_encounter_ind = 1 then 'ED/UC'
            when primary_care_visit_ind = 1 then 'Primary Care'
            else 'Other' end as encounter_sub_group,
        stg_encounter.appointment_status,
        stg_encounter.pat_key,
        coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
    from
        {{ ref('stg_encounter') }} as stg_encounter
        left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
            on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
        left join {{ref('stg_minds_matter_all')}} as minds_matter_all
            on stg_encounter.visit_key = minds_matter_all.visit_key
        left join minds_matter_occt_n_pt
            on stg_encounter.visit_key = minds_matter_occt_n_pt.visit_key
        left join {{source('cdw','provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key
        left join {{ ref('encounter_ed') }} as encounter_ed
            on stg_encounter.visit_key = encounter_ed.visit_key
        left join {{ ref('encounter_primary_care') }} as encounter_primary_care
            on stg_encounter.visit_key = encounter_primary_care.visit_key
    where
        (minds_matter_all.visit_key is not null or minds_matter_occt_n_pt.visit_key is not null)
        and stg_encounter.encounter_date <= current_date
        and year(stg_encounter.encounter_date) >= '2017'
),
pat_sub_group as (
    select
        fiscal_year,
        mrn,
		case when max(case when encounter_sub_group = 'Specialty Care' then 1 else 0 end) = 1
            then 'Specialty Care Patient'
			when max(case when encounter_sub_group = 'ED/UC' then 1 else 0 end) = 1 then 'ED/UC Patient'
			when max(case when encounter_sub_group = 'Primary Care' then 1 else 0 end) = 1 then 'Primary Care Patient'
			else 'Other Patient' end as patient_sub_group
	from minds_matter_build
	where appointment_status not in ('CANCELED', 'NO SHOW', 'LEFT WITHOUT SEEN', 'NO')
	group by
		fiscal_year,
		mrn
)
select
    minds_matter_build.visit_key,
    minds_matter_build.mrn,
    minds_matter_build.csn,
    minds_matter_build.patient_name,
    minds_matter_build.encounter_date,
    minds_matter_build.fiscal_year,
    minds_matter_build.primary_care_visit_ind,
    minds_matter_build.ed_encounter_ind,
    minds_matter_build.minds_matter_dx_ind,
    minds_matter_build.minds_matter_sde_ind,
    minds_matter_build.minds_matter_visit_type_ind,
    minds_matter_build.minds_matter_reason_visit_ind,
    minds_matter_build.provider_name,
    minds_matter_build.provider_id,
    minds_matter_build.department_name,
    minds_matter_build.department_id,
    minds_matter_build.visit_type,
    minds_matter_build.visit_type_id,
    minds_matter_build.encounter_type,
    minds_matter_build.encounter_type_id,
    minds_matter_build.minds_matter_patient_ind,
    minds_matter_build.minds_matter_pt_occt_ind,
    minds_matter_build.encounter_sub_group,
    minds_matter_build.appointment_status,
    minds_matter_build.pat_key,
    minds_matter_build.hsp_acct_key,
	coalesce(pat_sub_group.patient_sub_group, 'n/a') as patient_sub_group
from minds_matter_build
left join pat_sub_group
    on minds_matter_build.fiscal_year = pat_sub_group.fiscal_year
		and minds_matter_build.mrn = pat_sub_group.mrn
