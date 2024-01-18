with rcap as (
    select
        redcap_detail.record,
        master_redcap_question.field_nm,
        master_redcap_question.element_label,
        master_redcap_question.element_type,
        -- shorten field values for next pivot step
        -- failure to complete this step causes buffer issues when pivoting
        lower(
            substr(
                coalesce(
                    master_redcap_element_answr.element_desc,
                    redcap_detail.value
                ),
                1,
                200
            )
        )::varchar(200) as response
    from
        {{ source('cdw', 'redcap_detail') }} as redcap_detail
        left join {{ source('cdw', 'master_redcap_project') }} as master_redcap_project
            on redcap_detail.mstr_project_key = master_redcap_project.mstr_project_key
        left join {{ source('cdw', 'master_redcap_question') }} as master_redcap_question
            on redcap_detail.mstr_redcap_quest_key = master_redcap_question.mstr_redcap_quest_key
        left join {{ source('cdw', 'master_redcap_element_answr') }} as master_redcap_element_answr
            on redcap_detail.mstr_redcap_quest_key = master_redcap_element_answr.mstr_redcap_quest_key
            and redcap_detail.value = master_redcap_element_answr.element_id
    where
        redcap_detail.cur_rec_ind = 1
        and master_redcap_project.project_id = 859
),

rcap_pivot as (
    select
        rcap.record,
        max(
            case when rcap.field_nm = 'mrn' then rcap.response end
        ) as mrn,
        max(
            case when rcap.field_nm = 'csn' then rcap.response end
        ) as csn,
        max(
            case when rcap.field_nm = 'unit' then rcap.response end
        ) as unit,
        max(
            case when rcap.field_nm = 'event_date' then rcap.response end
        ) as event_date,
        max(
            case when rcap.field_nm = 'time_of_huddle' then rcap.response end
        ) as time_of_huddle,
        max(
            case when rcap.field_nm = 'datetime_of_huddle' then rcap.response end
        ) as huddle_date,
        max(
            case when rcap.field_nm = 'huddle_participants' then rcap.response end
        ) as huddle_participants,
        max(
            case when rcap.field_nm = 'tier_ii_request' then rcap.response end
        ) as tier_ii_request_ind,
        max(
            case when rcap.field_nm = 'event_review_date' then rcap.response end
        ) as tier_ii_review_date,
        max(
            case when rcap.field_nm = 'was_the_code_team_activate' then rcap.response end
        ) as code_team_activated_ind,
        max(
            case when rcap.field_nm = 'code_category' then rcap.response end
        ) as code_category
    from
        rcap
    group by
        rcap.record
)

select
    coalesce(stg_encounter.pat_key, stg_patient.pat_key) as pat_key,
    stg_encounter.visit_key,
    rcap_pivot.record,
    regexp_replace(rcap_pivot.mrn, '[^0-9\.]', '') as mrn,
    -- remove non-numeric chars from csn, limit to 11 chars not including
    -- leading and trailing zeroes
    regexp_extract(
        regexp_replace(rcap_pivot.csn, '[^0-9\.]', ''),
        '^(?:0*)([0-9\.]{1,11}?)(?:0*)$'
    )::numeric(14, 3) as csn,
    upper(rcap_pivot.unit) as unit,
    to_timestamp(rcap_pivot.event_date, 'YYYY-MM-DD HH24:MI') as event_date,
    -- older records have only time_of_huddle, not huddle_date
    -- if we have only time_of_huddle, complete with YYYYMMDD from event_date
    case
        when rcap_pivot.huddle_date is not null
        then to_timestamp(rcap_pivot.huddle_date, 'YYYY-MM-DD HH24:MI')
        else to_timestamp(
            regexp_extract(rcap_pivot.event_date, '[0-9]{4}-[0-9]{2}-[0-9]{2} ')
            || rcap_pivot.time_of_huddle,
            'YYYY-MM-DD HH24:MI'
        )
    end as huddle_date,
    case
        when rcap_pivot.huddle_date is null and rcap_pivot.time_of_huddle is null then 'MISSING'
        when rcap_pivot.huddle_date is not null then 'HUDDLE DATE/TIME'
        else 'EVENT DATE + HUDDLE TIME'
    end as huddle_date_src,
    rcap_pivot.huddle_participants,
    case
        when rcap_pivot.tier_ii_request_ind = 'yes' then 1
        else 0
    end as tier_ii_request_ind,
    to_timestamp(rcap_pivot.tier_ii_review_date, 'YYYY-MM-DD') as tier_ii_review_date,
    case
        when rcap_pivot.code_team_activated_ind = '1' then 1
        else 0
    end as code_team_activated_ind,
    upper(code_category) as code_category
from
    rcap_pivot
    left join {{ ref('stg_patient') }} as stg_patient
        on rcap_pivot.mrn = stg_patient.mrn
    left join {{ ref('stg_encounter') }} as stg_encounter
        on rcap_pivot.csn = stg_encounter.csn
