with
preterm_dx as (
    select
        mrn,
        max(case when lower(icd10_code) in ('z93.0', 'z99.11')
            then 1 else 0 end) as bpd_complex_ind,
        max(case when
            lower(diagnosis_name) like '%broncho%pulm%dys%'
                or lower(diagnosis_name) = 'chronic lung disease of prematurity'
            then 1 else 0 end) as bpd_ind,
        max(case when
                icd10_code is not null
                    and (
                        (lower(diagnosis_name) like '%pulm%hyper%'
                        or lower(diagnosis_name) like '%hyperten%pulm%')
                        and lower(icd10_code) = 'z99.81' --dependence on supplemental oxygen // --oxygen dependent
                        )
            then 1 else 0 end) as ph_ind,
        max(case when
            lower(icd10_code) = ('p07.30')
            or lower(diagnosis_name) like '%premature%gestation'
            then 1 else 0 end) as preterm_dx_ind    --gestation_less_37weeks_ind
    from
        {{ ref('diagnosis_encounter_all') }}
    where
        lower(diagnosis_name) like '%broncho%pulm%dys%'
        or lower(diagnosis_name) = 'chronic lung disease of prematurity'
        or lower(diagnosis_name) like '%pulm%hyper%'
        or lower(diagnosis_name) like '%hyperten%pulm%'
        or lower(icd10_code) in ('z93.0', 'z99.11')
        or lower(icd10_code) = 'z99.81' --dependence on supplemental oxygen // --oxygen dependent
        or lower(icd10_code) = 'p07.30'
        or lower(diagnosis_name) like '%premature%gestation'
    group by
        mrn
),
preterm_pat as (
    select
        mrn,
        1 as preterm_pat_ind
    from
        {{ ref('stg_patient') }}
    where
        current_age >= 1
        and (
            gestational_age_complete_weeks < 37
            and gestational_age_complete_weeks >= 26
            )
    group by
        mrn
),
build_pat_list as (
    select
        preterm_dx.mrn,
        preterm_dx.bpd_complex_ind,
        preterm_dx.bpd_ind,
        preterm_dx.ph_ind,
        preterm_dx.preterm_dx_ind,
        coalesce(preterm_pat.preterm_pat_ind, 0) as preterm_pat_ind
    from
        preterm_dx
        left join preterm_pat on preterm_dx.mrn = preterm_pat.mrn
),
build_pat_list_b as (
    select
        build_pat_list.mrn,
        build_pat_list.bpd_complex_ind,
        build_pat_list.bpd_ind,
        build_pat_list.ph_ind,
        build_pat_list.preterm_dx_ind,
        build_pat_list.preterm_pat_ind
    from
        build_pat_list
        inner join {{ ref('stg_patient') }} as stg_patient
        on build_pat_list.mrn = stg_patient.mrn
    where
        stg_patient.current_age >= 1
        and (bpd_complex_ind = 1
            or bpd_ind + ph_ind = 2)
        and preterm_dx_ind + preterm_pat_ind > 0
)
select
    encounter_all.visit_key,
    build_pat_list_b.mrn,
    1 as bpd_complex_ind
from
    build_pat_list_b
    left join {{ ref('encounter_all') }} as encounter_all
        on build_pat_list_b.mrn = encounter_all.mrn
    inner join {{ ref('lookup_frontier_program_providers_all') }} as lookup_frontier_program_providers_all
        on encounter_all.provider_id = cast(
            lookup_frontier_program_providers_all.provider_id as nvarchar(20))
        and lookup_frontier_program_providers_all.program = 'rare-lung'
where
    lower(department_name) like '%pulm%'
    and visit_type_id not in (
            '0',    -- default
            '4151', --  follow up cystic fibrosis
            '4158', --  follow up pcd
            '4107', --  new sleep visit
            '4132', --  php fol up
            '2755'  --  video visit sleep fol up
        )
    and year(add_months(encounter_all.encounter_date, 6)) >= 2022
    and encounter_all.encounter_date < current_date
group by build_pat_list_b.mrn, encounter_all.visit_key
