select
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.encounter_date,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    date_trunc('month', stg_encounter.encounter_date) as visual_month,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    stg_encounter.visit_key,
    stg_encounter.csn,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    stg_encounter.department_name,
    stg_encounter.department_id,
    encounter_specialty_care.specialty_name,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    max(
        case when
        stg_encounter_inpatient.visit_key is not null
        then 1 else 0 end) as inpatient_ind,
    max(case when
        lookup_frontier_program_visit.category like 'center for gi motility %'
        then 1 else 0 end) as general_motility_visit_ind,
    max(case when
        lookup_frontier_program_visit.category like 'defecation disorders clinic %'
        then 1 else 0 end) as defecation_disorder_visit_ind,
    max(case when
        lookup_frontier_program_visit.category like 'ibd/icaps %'
        then 1 else 0 end) as multi_disciplinary_visit_ind,
    max(case when
        lookup_frontier_program_visit.category like 'surgical intestinal motility service %'
        then 1 else 0 end) as life_bowel_dysmotility_visit_ind,
    max(case when
        lookup_frontier_program_visit.category like 'acquired autonomic dysfunction program %'
        then 1 else 0 end) as neuromodulation_visit_ind,
    max(case when lower(stg_encounter.department_name) like '%adolescent%med'
            or lower(stg_encounter.department_name) like '%gastroenterology%'
            or lower(encounter_specialty_care.specialty_name) like '%gastroenterology%'
            or lower(stg_encounter.department_name) like '%genetic%'
            or lower(stg_encounter.department_name) like '%mito%'
            or lower(stg_encounter.department_name) like '%nutrition%'
            or lower(encounter_specialty_care.specialty_name) like '%ped%surgery%'
            or lower(stg_encounter.department_name) like '%pathology%'
            or lower(stg_encounter.department_name) like '%psychiatry%'
            or lower(encounter_specialty_care.specialty_name) like '%psychiatry%'
            or lower(stg_encounter.department_name) like '% psychology%'
            or lower(stg_encounter.department_name) like '%radiology%'
        then 1 else 0 end) as specialty_deptartment_ind,
    max(case when
            lookup_frontier_program_providers_all.program = 'motility'
            and lookup_frontier_program_providers_all.provider_type = 'motility provider'
        then 1 else 0 end) as physician_ind,
    max(case when
            lookup_frontier_program_providers_all.program = 'motility'
            and lookup_frontier_program_providers_all.provider_type = 'other motility provider'
        then 1 else 0 end) as other_provider_ind,
    max(case when
            lookup_frontier_program_departments.program = 'motility'
        then 1 else 0 end) as gi_department_ind,
    max(case when
            stg_encounter.department_id = '101012165' --'bgr aadp multi d clnc'
        then 1 else 0 end) as aadp_multi_d_ind
from
    {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
        on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
    left join {{ ref('encounter_specialty_care') }} as encounter_specialty_care
        on stg_encounter.visit_key = encounter_specialty_care.visit_key
    left join {{ ref('lookup_frontier_program_visit') }} as lookup_frontier_program_visit
        on cast(lookup_frontier_program_visit.id as nvarchar(20)) = stg_encounter.visit_type_id
            and lookup_frontier_program_visit.program = 'motility'
    left join {{ ref('lookup_frontier_program_providers_all') }} as lookup_frontier_program_providers_all
        on cast(lookup_frontier_program_providers_all.provider_id as nvarchar(20)) = provider.prov_id
            and lookup_frontier_program_providers_all.program = 'motility'
    left join {{ ref('lookup_frontier_program_departments') }} as lookup_frontier_program_departments
        on cast(lookup_frontier_program_departments.department_id as nvarchar(20)) = stg_encounter.department_id
            and lookup_frontier_program_departments.program = 'motility'
group by
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.encounter_date,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0),
    stg_encounter.visit_key,
    stg_encounter.csn,
    initcap(provider.full_nm),
    provider.prov_id,
    stg_encounter.department_name,
    stg_encounter.department_id,
    encounter_specialty_care.specialty_name,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id
