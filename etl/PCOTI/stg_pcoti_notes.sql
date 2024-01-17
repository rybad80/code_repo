-- get all relevant smart-text elements by note_key
with note_relevant_smart_text as (
    select
        note_smart_text_id.note_key,
        max(
            case
                when upper(smart_text.smart_text_name) like 'IP CAT INITIATION NOTE%'
                    then 'IP CAT INITIATION NOTE'
                when upper(smart_text.smart_text_name) like 'IP CAT EVALUATION NOTE%'
                    then 'IP CAT EVALUATION NOTE'
                when upper(smart_text.smart_text_name) like 'IP CCOT NOTE%'
                    then 'IP CCOT NOTE'
                when upper(smart_text.smart_text_name) like 'IP CCOT RN/RT NOTE%'
                    then 'IP CCOT RN/RT NOTE'
                when upper(smart_text.smart_text_name) like 'IP CAT FOLLOWUP ASSESSMENT NOTE'
                    then 'IP CAT FOLLOWUP ASSESSMENT NOTE'
            end
        ) as note_type
    from
        {{ source('cdw', 'note_smart_text_id') }} as note_smart_text_id
        inner join {{ source('cdw', 'smart_text') }} as smart_text
            on note_smart_text_id.smart_text_key = smart_text.smart_text_key
    where
        upper(smart_text.smart_text_name) like 'IP CAT INITIATION NOTE%'
        or upper(smart_text.smart_text_name) like 'IP CAT EVALUATION NOTE%'
        or upper(smart_text.smart_text_name) like 'IP CCOT NOTE%'
        or upper(smart_text.smart_text_name) like 'IP CCOT RN/RT NOTE%'
        or upper(smart_text.smart_text_name) like 'IP CAT FOLLOWUP ASSESSMENT NOTE'
    group by
        note_smart_text_id.note_key
),

-- attending eval - get notes with relevant smart-phrase
attending_eval_smart_phrase as (
    select
        note_smart_phrase.note_key
    from
        {{ source('cdw', 'note_smart_phrase') }} as note_smart_phrase
        inner join {{ source('cdw', 'smart_phrase') }} as smart_phrase
            on note_smart_phrase.smart_phrase_key = smart_phrase.smart_phrase_key
    where
        smart_phrase.smart_phrase_nm = 'ATTESTPRECEPT'
    group by
        note_smart_phrase.note_key
),

-- attending eval - get notes with relevant smart-data-element
attending_eval_sde as (
    select
        smart_data_element_info.note_key
    from
        {{ source('cdw', 'smart_data_element_info') }} as smart_data_element_info
        inner join {{ source('cdw', 'clinical_concept') }} as clinical_concept
            on smart_data_element_info.concept_key = clinical_concept.concept_key
    where
        smart_data_element_info.context_nm = 'NOTE'
        and clinical_concept.concept_id = 'CHOPIP#244'
    group by
        smart_data_element_info.note_key
),

-- attending eval - get notes with relevant smart-text
attending_eval_smart_text as (
    select
        note_smart_text_id.note_key
    from
        {{ source('cdw', 'note_smart_text_id') }} as note_smart_text_id
        inner join {{ source('cdw', 'smart_text') }} as smart_text
            on note_smart_text_id.smart_text_key = smart_text.smart_text_key
    where
        upper(smart_text.smart_text_name) like '%ATTESTPRECEPT%'
    group by
        note_smart_text_id.note_key
),

-- attending eval - union all to get single list of note_keys
attending_eval as (
    select attending_eval_smart_phrase.note_key from attending_eval_smart_phrase
    union
    select attending_eval_sde.note_key from attending_eval_sde
    union
    select attending_eval_smart_text.note_key from attending_eval_smart_text
),

-- pull distinct list of active notes
active_notes as (
    select
        subqry.note_key
    from (
        select
            note_visit_info.note_key,
            max(
                case
                    when dim_note_status.note_stat_id in (1, 4) then 1 else 0
                end
            ) as deleted_note_ind
        from
            {{ source('cdw', 'note_visit_info') }} as note_visit_info
            inner join {{ source('cdw', 'dim_note_status') }} as dim_note_status
                on note_visit_info.dim_note_stat_key = dim_note_status.dim_note_stat_key
        group by
            note_visit_info.note_key
    ) as subqry
    where
        subqry.deleted_note_ind = 0
),

-- get all relevant history records
note_history_details as (
    select
        note_history.note_key,
        note_history.seq_num,
        note_history.note_act_local_dt,
        dim_note_action.note_act_id,
        note_history.act_emp_key as emp_key,
        provider.prov_key as prov_key,
        coalesce(employee.last_nm, provider.last_nm) as last_name,
        coalesce(employee.first_nm, provider.first_nm) as first_name,
        upper(coalesce(employee.title, provider.title)) as title
    from
        {{ source('cdw', 'note_history') }} as note_history
        inner join {{ source('cdw', 'dim_note_action') }} as dim_note_action
            on note_history.dim_note_act_key = dim_note_action.dim_note_act_key
        inner join {{ source('cdw', 'employee') }} as employee
            on note_history.act_emp_key = employee.emp_key
        inner join {{ source('cdw', 'provider') }} as provider
            on employee.prov_key = provider.prov_key
    where
        note_history.seq_num = 1 -- initial note records
        or dim_note_action.note_act_id = 2 -- sign actions
        or dim_note_action.note_act_id = 7 -- cosign actions
),

-- get first history record for a note, use as proxy for original note author
-- using this approach because clarity.hno_info.entry_emp_key is not populated
note_author_details as (
    select
        note_history_details.note_key,
        note_history_details.emp_key as orig_author_emp_key,
        note_history_details.prov_key as orig_author_prov_key,
        note_history_details.last_name as orig_author_last_name,
        note_history_details.first_name as orig_author_first_name,
        note_history_details.title as orig_author_title,
        regexp_extract(note_history_details.title, '^[^;]+') as orig_author_type
    from
        note_history_details
    where
        note_history_details.seq_num = 1
),

-- get details on signed notes
-- take only the first sign in the event there are multiple
note_sign_details as (
    select
        subqry.*
    from (
        select
            note_history_details.note_key,
            1 as note_signed_ind,
            note_history_details.note_act_local_dt as note_signed_date,
            note_history_details.emp_key as signing_emp_key,
            note_history_details.prov_key as signing_prov_key,
            note_history_details.last_name as signing_provider_last_name,
            note_history_details.first_name as signing_provider_first_name,
            note_history_details.title as signing_provider_title,
            regexp_extract(note_history_details.title, '^[^;]+') as signing_provider_type,
            row_number() over (
                partition by note_history_details.note_key
                order by note_history_details.note_act_local_dt
            ) as sign_order
        from
            note_history_details
        where
            note_history_details.note_act_id = 2
    ) as subqry
    where
        subqry.sign_order = 1
),

-- get details on cosigned notes
-- take only the first co-sign in the event there are multiple
note_cosign_details as (
    select
        subqry.*
    from (
        select
            note_history_details.note_key,
            1 as note_cosigned_ind,
            note_history_details.note_act_local_dt as note_cosigned_date,
            note_history_details.emp_key as cosigning_emp_key,
            note_history_details.prov_key as cosigning_prov_key,
            note_history_details.last_name as cosigning_provider_last_name,
            note_history_details.first_name as cosigning_provider_first_name,
            note_history_details.title as cosigning_provider_title,
            regexp_extract(note_history_details.title, '^[^;]+') as cosigning_provider_type,
            row_number() over (
                partition by note_history_details.note_key
                order by note_history_details.note_act_local_dt
            ) as cosign_order
        from
            note_history_details
        where
            note_history_details.note_act_id = 7
    ) as subqry
    where
        subqry.cosign_order = 1
)

-- assemble
select
    note_info.pat_key,
    note_info.visit_key,
    note_info.note_key,
    note_info.note_id,
    coalesce(
        note_sign_details.note_signed_date,
        (note_info.note_svc_dt at time zone 'est')::timestamp
    ) as note_join_date,
    (note_info.note_svc_dt at time zone 'est')::timestamp as note_create_date,
    note_relevant_smart_text.note_type,
    note_author_details.orig_author_emp_key,
    note_author_details.orig_author_prov_key,
    note_author_details.orig_author_last_name,
    note_author_details.orig_author_first_name,
    note_author_details.orig_author_type,
    coalesce(note_sign_details.note_signed_ind, 0) as note_signed_ind,
    note_sign_details.note_signed_date,
    note_sign_details.signing_emp_key,
    note_sign_details.signing_prov_key,
    note_sign_details.signing_provider_last_name,
    note_sign_details.signing_provider_first_name,
    note_sign_details.signing_provider_type,
    coalesce(note_cosign_details.note_cosigned_ind, 0) as note_cosigned_ind,
    note_cosign_details.note_cosigned_date,
    note_cosign_details.cosigning_emp_key,
    note_cosign_details.cosigning_prov_key,
    note_cosign_details.cosigning_provider_last_name,
    note_cosign_details.cosigning_provider_first_name,
    note_cosign_details.cosigning_provider_type,
    case
        when attending_eval.note_key is null then 0
        else 1
    end as attending_eval_ind
from
    {{ source('cdw', 'note_info') }} as note_info
    inner join active_notes
        on note_info.note_key = active_notes.note_key
    inner join note_relevant_smart_text
        on note_info.note_key = note_relevant_smart_text.note_key
    left join attending_eval
        on note_info.note_key = attending_eval.note_key
    left join note_author_details
        on note_info.note_key = note_author_details.note_key
    left join note_sign_details
        on note_info.note_key = note_sign_details.note_key
    left join note_cosign_details
        on note_info.note_key = note_cosign_details.note_key
