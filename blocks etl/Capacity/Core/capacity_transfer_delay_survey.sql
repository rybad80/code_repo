with numbers as (
        select
            row_number() over(order by full_date)::int as search_id
        from
            {{ref('dim_date')}}
),

questions as (
    select
        project_id,
        form_name,
        field_order,
        field_name,
        element_type,
        element_label,
        element_enum,
        search_id,
        regexp_extract(
            regexp_replace(element_enum, '(?<=\\n)( )+', ''),  -- remove spaces after /n
            '(^|(?<=\\n))[\d]+',  -- starting digits or first digits after '\n '
            1,                    -- start at the beginning
            search_id             -- which instance to grab
        ) as element_id,
        regexp_extract(
            element_enum,
            '(?<=, )[^\\]+',     -- text between 1st comma and '\'
            1,
            search_id
        ) as element_text
    from
        {{source('ods_redcap_porter','redcap_metadata')}}
        cross join numbers
    where
        project_id in (873, 891)
        and element_text is not null
),

answers as (
    select
        redcap_data.project_id,
        questions.form_name,
        redcap_data.record,
        (redcap_data.field_name)::varchar(250) as field_name,
        questions.element_type as question_type,
        questions.element_label as question,
        questions.element_id as answer_id,
        substr(
        case
            when questions.element_type is null
            then redcap_data.value
            else questions.element_text end,
        1, 250)  as answer,
        row_number() over (partition by redcap_data.project_id, record, redcap_data.field_name
                            order by search_id) as row_num
    from
        {{ ref('stg_redcap_all')}} as redcap_data
        left join questions
            on questions.field_name = redcap_data.field_name
            and redcap_data.value = questions.element_id
            and redcap_data.project_id = questions.project_id
     where
        redcap_data.project_id in (873, 891)
),

collapse as (
    select
        project_id,
        record,
        max(form_name) as form_name,
        max(
            case
                when field_name = 'csn'
                then answer
            end
        ) as entered_csn,
        max(
            case
                when field_name = 'ed_arrival_date'
                then answer
            end
        ) as transfer_dt,
        max(
            case
                when field_name = 'role'
                then answer
            end
        ) as user_role,
        max(
            case
                when field_name = 'what_is_the_most_significa'
                then regexp_replace(answer, 'Delay in ', '')
            end
        ) as primary_delay,
        max(
            case
                when question_type = 'checkbox'
                  and row_num = 1
                then answer
            end
         )as secondary_delay_1,
        max(
            case
                when question_type = 'checkbox'
                and row_num = 2
                then answer
            end
        ) as secondary_delay_2,
        max(
            case
                when question_type = 'checkbox'
                and row_num = 3
                then answer
            end
        ) as secondary_delay_3,
        max(
            case
                when field_name = 'please_explain_the_perceiv'
                then answer
            end
        ) as other_delay_detail,
        max(
            case
                when field_name = 'please_provide_any_additio'
                then answer
            end
        ) as other_info
    from
        answers
    group by
        record,
        project_id
),

final_details as (
    select
        project_id,
        form_name,
        record::int as record_id,
        entered_csn::numeric as entered_csn,
        transfer_dt::date as transfer_date,
        user_role,
        primary_delay,
        case
            when secondary_delay_1 in ('Unsure', 'Other')
            then 'Unsure/Other'
            else coalesce(secondary_delay_1, primary_delay)
        end as secondary_delay_1,
        secondary_delay_2,
        secondary_delay_3,
        other_delay_detail,
        other_info,
        row_number() over (partition by entered_csn, project_id, primary_delay
                            order by record_id) as enc_reason_num,
        row_number() over (partition by entered_csn
                    order by record_id) as enc_entry_num
    from
        collapse
)

select
    project_id,
    form_name,
    record_id,
    stg_encounter.visit_key,
    entered_csn,
    transfer_date,
    user_role,
    primary_delay,
    secondary_delay_1,
    secondary_delay_2,
    secondary_delay_3,
    other_delay_detail,
    other_info,
    enc_reason_num,
    enc_entry_num,
    case
        when stg_encounter.visit_key is null
        then 1
        else 0
    end as invalid_csn_ind,
    case
        when enc_reason_num > 1
        then 1
        else 0
    end as duplicate_ind
from
    final_details
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = entered_csn
