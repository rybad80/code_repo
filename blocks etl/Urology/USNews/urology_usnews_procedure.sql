with procedure_name_distinct as (
    select distinct
        pat_key,
        service_date,
        cpt_code,
        source,
        procedure_name
    from
        {{ref('stage_urology_usnews_procedure')}}
),
procedure_name_concat as (
    select
        pat_key,
        service_date,
        cpt_code,
        source,
        group_concat(procedure_name, '|') as procedure_name
    from
        procedure_name_distinct
    group by
        pat_key,
        service_date,
        cpt_code,
        source
),
provider_name_distinct as (
    select distinct
        pat_key,
        service_date,
        cpt_code,
        source,
        initcap(provider_name) as provider_name
    from
        {{ref('stage_urology_usnews_procedure')}}
),
provider_name_concat as (
    select
        pat_key,
        service_date,
        cpt_code,
        source,
        group_concat(provider_name, '|') as provider_name
    from
        provider_name_distinct
    group by
        pat_key,
        service_date,
        cpt_code,
        source
),
diagnosis_distinct as (
    select distinct
        pat_key,
        service_date,
        cpt_code,
        source,
        icd10_code || ' ' || diagnosis_name as diagnosis
    from
        {{ref('stage_urology_usnews_procedure')}}
),
diagnosis_concat as (
    select
        pat_key,
        service_date,
        cpt_code,
        source,
        group_concat(diagnosis, '|') as diagnosis
    from diagnosis_distinct
    group by
        pat_key,
        service_date,
        cpt_code,
        source
),
dx_ind_max as (
    select
        pat_key,
        service_date,
        cpt_code,
        source,
        dx_question,
        dx_submission_year,
        max(dx_inclusion_ind) as dx_inclusion_ind,
        max(dx_exclusion_ind) as dx_exclusion_ind
    from
        {{ref('stage_urology_usnews_procedure')}}
    where
        dx_question is not null
    group by
        pat_key,
        service_date,
        cpt_code,
        source,
        dx_question,
        dx_submission_year
),
dx_exclusion as (
    select
        pat_key,
        service_date,
        cpt_code,
        source,
        -- k12a inclusion dx = k12b exclusion dx
        max(case when dx_question = 'k12a1' then 1 else 0 end) as k12b1_dx_exclusion_ind,
        max(case when dx_question = 'k12a2' then 1 else 0 end) as k12b2_dx_exclusion_ind,
        max(case when dx_question = 'k15c1' then 1 else 0 end) as k15c1_dx_exclusion_ind
    from
        dx_ind_max
    group by
        pat_key,
        service_date,
        cpt_code,
        source
),
chart_review as (
    select
        submission_year,
        lower(question) as question,
        mrn,
        to_date(service_date, 'YYYY-MM-DD') as service_date,
        cpt_code,
        max(case when include = 'Yes' then 1 else 0 end) as include_ind,
        group_concat(notes, '|') as notes
    from
        {{ source('manual_ods', 'urology_usnews_chart_review') }}
    group by
        submission_year,
        question,
        mrn,
        service_date,
        cpt_code
),
join_all_data as (
    select distinct
        stage_urology_usnews_procedure.pat_key,
        stage_urology_usnews_procedure.mrn,
        stage_urology_usnews_procedure.patient_name,
        stage_urology_usnews_procedure.dob,
        stage_urology_usnews_procedure.age_years,
        stage_urology_usnews_procedure.under_age_21_ind,
        stage_urology_usnews_procedure.service_date,
        provider_name_concat.provider_name,
        stage_urology_usnews_procedure.cpt_code,
        procedure_name_concat.procedure_name,
        diagnosis_concat.diagnosis,
        stage_urology_usnews_procedure.source,
        stage_urology_usnews_procedure.cpt_question,
        stage_urology_usnews_procedure.cpt_submission_year,
        stage_urology_usnews_procedure.cpt_inclusion_ind,
        stage_urology_usnews_procedure.cpt_exclusion_ind,
        stage_urology_usnews_procedure.cpt_chart_review_ind,
        coalesce(dx_exclusion.k12b2_dx_exclusion_ind, 0) as k12b2_dx_exclusion_ind,
        dx_ind_max.dx_question,
        dx_ind_max.dx_submission_year,
        dx_ind_max.dx_inclusion_ind,
        dx_ind_max.dx_exclusion_ind,
        nvl2(chart_review.mrn, 1, 0) as review_complete_ind,
        chart_review.notes,
        coalesce(
            chart_review.include_ind,
            case
                when
                    stage_urology_usnews_procedure.cpt_question = 'k12b1'
                    and dx_exclusion.k12b1_dx_exclusion_ind = 1
                    and stage_urology_usnews_procedure.cpt_submission_year = dx_ind_max.dx_submission_year
                    then 0
                when
                    stage_urology_usnews_procedure.cpt_question = 'k12b2'
                    and dx_exclusion.k12b2_dx_exclusion_ind = 1
                    and stage_urology_usnews_procedure.cpt_submission_year = dx_ind_max.dx_submission_year
                    then 0
                when
                    stage_urology_usnews_procedure.cpt_question = 'k15c1'
                    and dx_exclusion.k15c1_dx_exclusion_ind = 1
                    and stage_urology_usnews_procedure.cpt_submission_year = dx_ind_max.dx_submission_year
                    then 0
                when stage_urology_usnews_procedure.source = 'Billing' then 1
                else 0
            end
        ) as include_ind
    from
        {{ref('stage_urology_usnews_procedure')}} as stage_urology_usnews_procedure
        left join procedure_name_concat
            on stage_urology_usnews_procedure.pat_key = procedure_name_concat.pat_key
            and stage_urology_usnews_procedure.service_date = procedure_name_concat.service_date
            and stage_urology_usnews_procedure.cpt_code = procedure_name_concat.cpt_code
            and stage_urology_usnews_procedure.source = procedure_name_concat.source
        left join provider_name_concat
            on stage_urology_usnews_procedure.pat_key = provider_name_concat.pat_key
            and stage_urology_usnews_procedure.service_date = provider_name_concat.service_date
            and stage_urology_usnews_procedure.cpt_code = provider_name_concat.cpt_code
            and stage_urology_usnews_procedure.source = provider_name_concat.source
        left join diagnosis_concat
            on stage_urology_usnews_procedure.pat_key = diagnosis_concat.pat_key
            and stage_urology_usnews_procedure.service_date = diagnosis_concat.service_date
            and stage_urology_usnews_procedure.cpt_code = diagnosis_concat.cpt_code
            and stage_urology_usnews_procedure.source = diagnosis_concat.source
        left join dx_ind_max
            on stage_urology_usnews_procedure.pat_key = dx_ind_max.pat_key
            and stage_urology_usnews_procedure.service_date = dx_ind_max.service_date
            and stage_urology_usnews_procedure.cpt_code = dx_ind_max.cpt_code
            and stage_urology_usnews_procedure.source = dx_ind_max.source
        left join dx_exclusion
            on stage_urology_usnews_procedure.pat_key = dx_exclusion.pat_key
            and stage_urology_usnews_procedure.service_date = dx_exclusion.service_date
            and stage_urology_usnews_procedure.cpt_code = dx_exclusion.cpt_code
            and stage_urology_usnews_procedure.source = dx_exclusion.source
        left join chart_review
            on stage_urology_usnews_procedure.cpt_question = chart_review.question
            and stage_urology_usnews_procedure.cpt_submission_year = chart_review.submission_year
            and stage_urology_usnews_procedure.mrn = chart_review.mrn
            and stage_urology_usnews_procedure.service_date = chart_review.service_date
            and stage_urology_usnews_procedure.cpt_code = chart_review.cpt_code
),
prep_surrogate_key as (
    select distinct
        pat_key,
        service_date,
        cpt_code,
        cpt_question,
        cpt_submission_year,
        dx_question,
        dx_submission_year
    from
        join_all_data
),
surrogate_key as (
    select
        {{
            dbt_utils.surrogate_key([
                'pat_key',
                'coalesce(service_date, \'1111-11-11\')',
                'coalesce(cpt_code, \'x\')',
                'coalesce(cpt_question, \'x\')',
                'coalesce(cpt_submission_year, 0)',
                'coalesce(dx_question, \'x\')',
                'coalesce(dx_submission_year, 0)'
            ])
        }} as urology_usnews_procedure_question_key,
        prep_surrogate_key.pat_key,
        prep_surrogate_key.service_date,
        prep_surrogate_key.cpt_code,
        prep_surrogate_key.cpt_question,
        prep_surrogate_key.cpt_submission_year,
        prep_surrogate_key.dx_question,
        prep_surrogate_key.dx_submission_year
    from
        prep_surrogate_key
)
select
    join_all_data.pat_key,
    join_all_data.mrn,
    join_all_data.patient_name,
    join_all_data.dob,
    join_all_data.age_years,
    join_all_data.under_age_21_ind,
    join_all_data.service_date,
    join_all_data.provider_name,
    join_all_data.cpt_code,
    join_all_data.procedure_name,
    join_all_data.diagnosis,
    join_all_data.source,
    join_all_data.cpt_question,
    join_all_data.cpt_submission_year,
    join_all_data.cpt_inclusion_ind,
    join_all_data.cpt_exclusion_ind,
    join_all_data.cpt_chart_review_ind,
    join_all_data.k12b2_dx_exclusion_ind,
    join_all_data.dx_question,
    join_all_data.dx_submission_year,
    join_all_data.dx_inclusion_ind,
    join_all_data.dx_exclusion_ind,
    join_all_data.review_complete_ind,
    join_all_data.notes,
    join_all_data.include_ind,
    surrogate_key.urology_usnews_procedure_question_key
from
    join_all_data
    inner join surrogate_key
        on join_all_data.pat_key = surrogate_key.pat_key
        and coalesce(join_all_data.service_date, '1111-11-11') = coalesce(surrogate_key.service_date, '1111-11-11')
        and coalesce(join_all_data.cpt_code, 'x') = coalesce(surrogate_key.cpt_code, 'x')
        and coalesce(join_all_data.cpt_question, 'x') = coalesce(surrogate_key.cpt_question, 'x')
        and coalesce(join_all_data.cpt_submission_year, 0) = coalesce(surrogate_key.cpt_submission_year, 0)
        and coalesce(join_all_data.dx_question, 'x') = coalesce(surrogate_key.dx_question, 'x')
        and coalesce(join_all_data.dx_submission_year, 0) = coalesce(surrogate_key.dx_submission_year, 0)
