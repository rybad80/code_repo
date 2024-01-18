with
cohort_enc_base as (--region
    select
        diagnosis_encounter_all.pat_key,
        diagnosis_encounter_all.mrn,
        diagnosis_encounter_all.visit_key,
        diagnosis_encounter_all.csn,
        diagnosis_encounter_all.patient_name,
        diagnosis_encounter_all.encounter_date,
        diagnosis_encounter_all.hsp_acct_key,
        max(case when
                lower(diagnosis_encounter_all.icd10_code) in( 'e16.1') --hyperinsulinism
                and lower(diagnosis_encounter_all.diagnosis_name) like '%hyperinsulinism%'
            then 1 else 0 end) as hi_ind,
        max(case when
                lower(diagnosis_encounter_all.icd10_code) in( 'e16.1') --ketotic hypoglycemia
                and lower(diagnosis_encounter_all.diagnosis_name) like '%ketotic%'
              then 1 else 0 end) as ketotic_ind,
        max(case when
                lower(diagnosis_encounter_all.icd10_code) in(
                    'e74.00', 'e74.01', 'e74.09', 'e74.04', 'e74.03') -- gsd
                and (lower(diagnosis_encounter_all.diagnosis_name) like '%glycogen storage disease%'
                    or lower(diagnosis_encounter_all.diagnosis_name) like '%gsd%')
              then 1 else 0 end) as gsd_ind,
        max(case when lower(diagnosis_encounter_all.diagnosis_name) like '%pancreatectomy%'
                then 1 else 0 end) as panc_dx_ind,
        max(case when lower(diagnosis_encounter_all.icd10_code) = 'e16.2'  -- hypoglycemia
              then 1 else 0 end) as hypo_ind,
        max(case when lower(diagnosis_encounter_all.icd10_code) = 'd13.7' -- insulinoma
              then 1 else 0 end) as insl_ind,
        max(case when lower(diagnosis_encounter_all.icd10_code) = 'q93.81' -- 22q
              then 1 else 0 end) as dx_22q_ind,
        max(case when
                lower(diagnosis_encounter_all.diagnosis_name) like '%acanthosis nigricans%'
              then 1 else 0 end) as acanthos_ind,
        max(case when
                lower(diagnosis_encounter_all.diagnosis_name) like '%hyperlipidemia%'
              then 1 else 0 end) as hyperlip_ind,
        max(case when
                lower(diagnosis_encounter_all.diagnosis_name) like 'morbid%obesity%'
              then 1 else 0 end) as morbid_ob_ind
    from {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
    group by
        diagnosis_encounter_all.pat_key,
        diagnosis_encounter_all.mrn,
        diagnosis_encounter_all.visit_key,
        diagnosis_encounter_all.csn,
        diagnosis_encounter_all.patient_name,
        diagnosis_encounter_all.encounter_date,
        diagnosis_encounter_all.hsp_acct_key
        --end region
),
cohort_base as (--region
    select
        stg_encounter.pat_key,
        stg_encounter.mrn,
        max(case when
                cohort_enc_base.hi_ind = 1
              then 1 else 0 end) as hi_ind,
        max(case when
                cohort_enc_base.ketotic_ind = 1
              then 1 else 0 end) as ketotic_ind,
        max(case when
                cohort_enc_base.gsd_ind = 1
              then 1 else 0 end) as gsd_ind,
        max(case when
                cohort_enc_base.panc_dx_ind = 1
              then 1 else 0 end) as panc_dx_ind,
        max(case when
                cohort_enc_base.hypo_ind = 1
              then 1 else 0 end) as hypo_ind,
        max(case when
                cohort_enc_base.insl_ind = 1
              then 1 else 0 end) as insl_ind,
        max(case when
                cohort_enc_base.dx_22q_ind = 1
              then 1 else 0 end) as dx_22q_ind,
        max(case when
                cohort_enc_base.acanthos_ind = 1
              then 1 else 0 end) as acanthos_ind,
        max(case when
                cohort_enc_base.hyperlip_ind = 1
              then 1 else 0 end) as hyperlip_ind,
        max(case when
                cohort_enc_base.morbid_ob_ind = 1
              then 1 else 0 end) as morbid_ob_ind
    from
        cohort_enc_base
        inner join {{ref('stg_encounter')}} as stg_encounter
            on cohort_enc_base.visit_key = stg_encounter.visit_key
    group by
        stg_encounter.pat_key,
        stg_encounter.mrn
    --end region
),
or_hx as (--region
    select
        surgery_procedure.pat_key,
        surgery_procedure.mrn,
        1 as surgery_ind
    from {{ ref('surgery_procedure') }} as surgery_procedure
    where
        lower(surgery_procedure.or_procedure_name) like '%pancreatectomy%'
        and lower(surgery_procedure.primary_surgeon) like '%adzick,%scott%'
    group by
        surgery_procedure.pat_key,
        surgery_procedure.mrn
    --end region
),
diabetes_hx as (--region
    select
        encounter_specialty_care.pat_key,
        encounter_specialty_care.mrn,
        max(case when
                --'new diabetes patient', 'follow up diabetes'
                encounter_specialty_care.visit_type_id in ('2202', '2203')
              then 1 else 0 end) as diab_visit_ind
    from {{ ref('encounter_specialty_care') }} as encounter_specialty_care
    group by
        encounter_specialty_care.pat_key,
        encounter_specialty_care.mrn
    --end region
),
visit_hx as (--region
    select
        stg_encounter.visit_key,
        initcap(provider.full_nm) as provider_name,
        provider.prov_id as provider_id,
        stg_encounter.department_name,
        stg_encounter.department_id,
        stg_encounter.visit_type,
        stg_encounter.visit_type_id,
        stg_encounter.encounter_type,
        stg_encounter.encounter_type_id,
        case when
                lower(encounter_specialty_care.department_name) = 'bgr hifp multid cln'
                 -- HI Multi-D Clinic Only
            then 1 else 0 end as hifp_ind,
        case when --general HI providers = providers with no restrictions for visit types
                lookup_fp_hi_providers.provider_type in ('general')
            then 1 else 0 end as gen_visit_ind,
        case when -- IP visits only w/ IP only providers + general HI providers
                stg_encounter.encounter_type_id = 3
                and lookup_fp_hi_providers.provider_type in ('general', 'inpatient only')
            then 1 else 0 end as inpatient_ind,
        case when --specialty care (appt, office visit, op, sunday visit, telemedicine) in endo
                stg_encounter.encounter_type_id in(50, 101, 152, 204, 76)
                and (lower(encounter_specialty_care.specialty_name) = 'endocrinology'
                     or lower(encounter_specialty_care.department_name) = 'bgr hifp multid cln')
                and lookup_fp_hi_providers.provider_type in ('general', 'hifp only')
            then 1 else 0 end as specialty_care_ind
    from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ ref('encounter_specialty_care') }} as encounter_specialty_care
        on stg_encounter.visit_key = encounter_specialty_care.visit_key
    inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_fp_hi_providers
        on provider.prov_id = cast(lookup_fp_hi_providers.provider_id as nvarchar(20))
        and lookup_fp_hi_providers.program = 'hi'
        and lookup_fp_hi_providers.active_ind = 1
    where
                lookup_fp_hi_providers.provider_type = 'general'
                or (
                    stg_encounter.encounter_type_id = 3 --inpatient (hosp enc)
                    and lookup_fp_hi_providers.provider_type in ('general', 'inpatient only')
                    )
        --specialty care
                or (stg_encounter.encounter_type_id in( 50,     -- appt
                                                        101,    -- office visit
                                                        152,    -- op
                                                        204,    -- sunday visit
                                                        76      -- telemedicine
                                                        )
                        and (
                            lower(encounter_specialty_care.specialty_name) = 'endocrinology'
                            or lower(encounter_specialty_care.department_name) = 'bgr hifp multid cln'
                            )
                        and lookup_fp_hi_providers.provider_type in ('general', 'hifp only')
                    )
    --end region
),
flowsheet_hx as (--region
    select
        flowsheet_all.visit_key,
        1 as flowsheet_ind
    from {{ ref('flowsheet_all') }} as flowsheet_all
    where
        lower(flowsheet_all.flowsheet_name) in (
                                'hi phenotype',
                                'hi genetic mutation',
                                'hi management on admission',
                                'hi management on discharge',
                                'hi phenotype other comments',
                                'other non-hi medications',
                                'age when hi treatment started (days)'
                                )

    group by flowsheet_all.visit_key
    --end region
),
cohort_build as (--region:
    select
        cohort_enc_base.visit_key,
        cohort_enc_base.csn,
        cohort_enc_base.patient_name,
        cohort_base.mrn,
        cohort_enc_base.encounter_date,
        visit_hx.provider_name,
        visit_hx.provider_id,
        visit_hx.department_name,
        visit_hx.department_id,
        visit_hx.visit_type,
        visit_hx.visit_type_id,
        visit_hx.encounter_type,
        visit_hx.encounter_type_id,
        visit_hx.hifp_ind,
        visit_hx.gen_visit_ind,
        visit_hx.inpatient_ind,
        visit_hx.specialty_care_ind,
        visit_hx.specialty_care_ind as specialty_ind,
         --above: antiquated but kept in-case called by existing code
        diabetes_hx.diab_visit_ind,
        cohort_base.panc_dx_ind,
        cohort_base.hi_ind,
        cohort_base.ketotic_ind,
        cohort_base.gsd_ind,
        cohort_base.hypo_ind,
        cohort_base.insl_ind,
        cohort_base.dx_22q_ind,
        cohort_base.acanthos_ind,
        cohort_base.hyperlip_ind,
        cohort_base.morbid_ob_ind,
        flowsheet_hx.flowsheet_ind,
        cohort_base.pat_key,
        cohort_enc_base.hsp_acct_key,
        row_number()over (
            partition by cohort_base.mrn
            order by cohort_enc_base.encounter_date) as pat_visit_seq_num,
        row_number()over (
            partition by cohort_base.mrn, visit_hx.visit_type, year(add_months(cohort_enc_base.encounter_date, 6))
            order by cohort_enc_base.encounter_date) as pat_per_fy_seq_num,
        year(add_months(encounter_date, 6)) as fiscal_year,
        date_trunc('month', encounter_date) as visual_month,
        coalesce(surgery_ind, 0) as panc_ind,
        coalesce(surgery_ind, 0) as surgery_ind
         --above 2 fields: antiquated but kept in-case called by existing code
    from cohort_base
        inner join cohort_enc_base on cohort_base.pat_key = cohort_enc_base.pat_key
        left join visit_hx on cohort_enc_base.visit_key = visit_hx.visit_key
        left join diabetes_hx on cohort_base.pat_key = diabetes_hx.pat_key
        left join or_hx on cohort_base.pat_key = or_hx.pat_key
        left join flowsheet_hx on cohort_enc_base.visit_key = flowsheet_hx.visit_key
    where
        visit_hx.gen_visit_ind + visit_hx.inpatient_ind + visit_hx.specialty_care_ind > 0
        and lower(visit_hx.department_name) != 'bgr bone health'
        and lower(visit_hx.visit_type) not like '%bone health%'
        and (
            visit_hx.hifp_ind = 1
            or flowsheet_hx.flowsheet_ind = 1
            or (
                ((cohort_base.hi_ind + cohort_base.ketotic_ind + cohort_base.gsd_ind) > 0
                or (cohort_base.hypo_ind + cohort_base.insl_ind > 0
                    and cohort_base.dx_22q_ind + cohort_base.acanthos_ind
                    + cohort_base.hyperlip_ind + cohort_base.morbid_ob_ind = 0)
                        )
                and (coalesce(diabetes_hx.diab_visit_ind, 0) = 0
                    or (diabetes_hx.diab_visit_ind = 1
                        and (cohort_base.panc_dx_ind + surgery_ind > 0
                            or (cohort_base.hi_ind + cohort_base.gsd_ind > 0
                                --and cohort_base.dx_22q_ind + cohort_base.acanthos_ind = 0
                                --dropping this for now: clinical team might reconsider this exclusion
                                ))))
                )
            )
    --end region
)
select * from cohort_build
