with
op_enc_generic_tmp as (--region: all potential generic encounters during earlier pandemic window
    select
        stg_encounter.visit_key,
        stg_encounter.visit_type,
        stg_encounter.encounter_date,
        stg_encounter.pat_key,
        provider.prov_key as provider_id,
        stg_encounter.appointment_status_id,
        case when (stg_encounter.encounter_date between lookup_fp_visit.start_date and lookup_fp_visit.end_date)
            and lookup_fp_visit.id is not null then 1 else 0 end as generic_early_pandemic
    from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_fp_providers
        on provider.prov_id = cast(lookup_fp_providers.provider_id as nvarchar(20))
        and lookup_fp_providers.program = 'engin'
        and lookup_fp_providers.provider_type = 'active during pandemic' -- engin providers active in pandemic
        and lookup_fp_providers.active_ind = 1
    inner join {{ ref('lookup_frontier_program_departments')}} as lookup_fp_departments
        on stg_encounter.department_id = cast(lookup_fp_departments.department_id as nvarchar(20))
        and lookup_fp_departments.program = 'engin'
        and lookup_fp_departments.active_ind = 1
    inner join {{ ref('lookup_frontier_program_visit')}} as lookup_fp_visit
        on stg_encounter.visit_type_id = cast(lookup_fp_visit.id as nvarchar(20))
        and lookup_fp_visit.program = 'engin'
        and lookup_fp_visit.category = 'generic visit'
        and lookup_fp_visit.active_ind = 1
    where
        stg_encounter.appointment_status_id in (1, -- scheduled
                                                2, -- completed
                                                6, --arrived
                                                4 -- no show (for calculating 'no show' metric)
                                                )
        and (stg_encounter.visit_type_id not in ('3218', --NEW NEUROGENTICS PATIENT
                                                '3219') --FOL NEUROGENETICS PATIENT
            or provider.prov_id in ('8373', -- goldberg, ethan m
                                            '9228')-- helbig, ingo
        )
    --end region
),
op_enc_generic_tmp2 as (--region: firstly incluse these three cinarios:
    --cinario 1. in window, include non-new generic visits for patients who had engin visit prior to the
    --generic visit (they were engin patient already)
    select
        op_enc_generic_tmp.visit_key,
        op_enc_generic_tmp.encounter_date,
        op_enc_generic_tmp.pat_key,
        op_enc_generic_tmp.provider_id,
        op_enc_generic_tmp.appointment_status_id
    from op_enc_generic_tmp
    inner join  {{ ref('stg_frontier_engin_op_enc_engin') }} as op_enc_engin
        on op_enc_generic_tmp.pat_key = op_enc_engin.pat_key
        and op_enc_engin.appointment_status_id != 4 -- exclude 'no show'
    where generic_early_pandemic = 1
        and op_enc_generic_tmp.encounter_date > op_enc_engin.encounter_date
        and not regexp_like(lower(op_enc_generic_tmp.visit_type), '\bnew\b')
    union
    --cinario 2. in window, generic visits that happened after an engin consult order placed
    --(new patient needs a referal)
    select
        op_enc_generic_tmp.visit_key,
        op_enc_generic_tmp.encounter_date,
        op_enc_generic_tmp.pat_key,
        op_enc_generic_tmp.provider_id,
        op_enc_generic_tmp.appointment_status_id
    from op_enc_generic_tmp
    inner join {{ ref('stg_frontier_engin_consult_order') }} as consult_order
        on op_enc_generic_tmp.pat_key = consult_order.pat_key
    where generic_early_pandemic = 1
        and op_enc_generic_tmp.encounter_date > to_date(consult_order.placed_date, 'yyyy-mm-dd hh24:mi:ss')
        and regexp_like(lower(op_enc_generic_tmp.visit_type), '\bnew\b')
    union
    --cinario 3. any generic visits meet note search requirment below
    select
        op_enc_generic_tmp.visit_key,
        op_enc_generic_tmp.encounter_date,
        op_enc_generic_tmp.pat_key,
        op_enc_generic_tmp.provider_id,
        op_enc_generic_tmp.appointment_status_id
    from op_enc_generic_tmp
    inner join {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
        on op_enc_generic_tmp.visit_key = note_edit_metadata_history.visit_key
        and note_edit_metadata_history.note_type_id = 1 --progress notes
        and note_edit_metadata_history.last_edit_ind = 1
    inner join {{source('cdw', 'note_text')}} as note_text
        on note_edit_metadata_history.note_visit_key = note_text.note_visit_key
    where -- regardless of window, if any generic visits that have these language in progress note
        (((op_enc_generic_tmp.appointment_status_id != 4 and note_edit_metadata_history.note_deleted_ind = 0)
            or op_enc_generic_tmp.appointment_status_id = 4)
        and lower(note_text) like '% had the pleasure of %ing % epilepsy neurogenetics initiative%')
    --if the year = FY2020 and providers was Goldberg/Helbig/Fitzgerald/Massey, then search "Neurogenetics Clinic"
        or (year(add_months(op_enc_generic_tmp.encounter_date, 6)) = 2020
            and op_enc_generic_tmp.provider_id in ('11811', -- fitzgerald, mark
                                                    '8373', -- goldberg, ethan m
                                                    '9228', -- helbig, ingo
                                                    '9423' -- massey, shavonne l
                                                    )
            and lower(note_text) like '%neurogenetics clinic%'
        )
    --end region
)
--secondly exclude generic visit with an engin consult order placed
--(the provider placed an engin consult in the visit)
select distinct
    op_enc_generic_tmp2.visit_key,
    op_enc_generic_tmp2.encounter_date,
    op_enc_generic_tmp2.pat_key,
    op_enc_generic_tmp2.appointment_status_id
from op_enc_generic_tmp2
left join {{ ref('stg_frontier_engin_consult_order') }} as consult_order
    on op_enc_generic_tmp2.provider_id = consult_order.referring_prov_id
    and op_enc_generic_tmp2.pat_key = consult_order.pat_key
    and op_enc_generic_tmp2.visit_key = consult_order.visit_key
--also exclude encounters opened in error
left join {{ ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on op_enc_generic_tmp2.visit_key = diagnosis_encounter_all.visit_key
    and diagnosis_encounter_all.visit_diagnosis_ind = 1
    and lower(diagnosis_encounter_all.icd10_code) = 'xxxxbc' -- erroneous encounter--disregard
where
    consult_order.placed_date is null
    and diagnosis_encounter_all.visit_key is null
