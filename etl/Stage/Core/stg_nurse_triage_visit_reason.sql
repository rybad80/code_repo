{{ config(meta = {
    'critical': true
}) }}

with encounter_reasons as (
select
    stg_encounter_nurse_triage.encounter_key,
    pat_enc_rsn_visit.line as seq_num,
    last_value(pat_enc_rsn_visit.line) over(
        partition by stg_encounter_nurse_triage.encounter_key
        order by pat_enc_rsn_visit.line
        range between unbounded preceding and unbounded following
    ) as num_rsns,
    cl_rsn_for_visit.reason_visit_id as reason_id,
    cl_rsn_for_visit.reason_visit_name,
    cl_rsn_for_visit.display_text as visit_reason,
    pat_enc_rsn_visit.comments as reason_comment
from {{ ref('stg_encounter_nurse_triage') }} as stg_encounter_nurse_triage
    inner join {{ source('clarity_ods', 'pat_enc_rsn_visit') }} as pat_enc_rsn_visit
        on stg_encounter_nurse_triage.csn = pat_enc_rsn_visit.pat_enc_csn_id
    inner join
        {{ source('clarity_ods', 'cl_rsn_for_visit') }}
            as cl_rsn_for_visit
        on pat_enc_rsn_visit.enc_reason_id = cl_rsn_for_visit.reason_visit_id
)
select
    stg_encounter_nurse_triage.encounter_key,
    stg_encounter_nurse_triage.visit_key,
    coalesce(encounter_reasons.seq_num, 1) as seq_num,
    coalesce(encounter_reasons.num_rsns, 0) as num_visit_reasons,
    encounter_reasons.reason_id,
    case when encounter_reasons.encounter_key is null
        then 'No reason selected'
        else encounter_reasons.visit_reason
    end as visit_reason,
    encounter_reasons.reason_comment
from {{ ref('stg_encounter_nurse_triage') }} as stg_encounter_nurse_triage
    left join encounter_reasons
        on stg_encounter_nurse_triage.encounter_key = encounter_reasons.encounter_key
group by
    encounter_reasons.encounter_key,
    stg_encounter_nurse_triage.encounter_key,
    stg_encounter_nurse_triage.visit_key,
    encounter_reasons.seq_num,
    num_visit_reasons,
    reason_id,
    visit_reason,
    reason_comment
