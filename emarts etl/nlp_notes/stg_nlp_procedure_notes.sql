{{
    config(
        materialized = 'view',        
        meta = {
            'targets': ['snowflake'],
            'critical': false
        }
    ) 
}}
with today_notes as (
    select
        order_proc_id
    from
        {{source('clarity_ods', 'order_narrative')}}
    where
        line = 1
        and date(_update_date) > current_date - 14
    union
    select
        order_proc_id
    from
        {{source('clarity_ods', 'order_impression')}}
    where
        line = 1
        and date(_update_date) > current_date - 14
),
proc_ids as (
    select
        order_proc.order_proc_id,
        patient.pat_mrn_id,
        order_proc._update_date as upd_dt,
        order_proc.pat_enc_csn_id
    from
        {{source('clarity_ods', 'order_proc')}} as order_proc
        inner join {{source('clarity_ods', 'patient')}} as patient
            on patient.pat_id = order_proc.pat_id
        inner join {{source('clarity_ods', 'zc_order_status')}} as zc_order_status
            on zc_order_status.order_status_c = order_proc.order_status_c
        inner join today_notes
            on today_notes.order_proc_id = order_proc.order_proc_id
    where
        (
            lower(order_proc.display_name) like '%doppler%'
            or lower(order_proc.display_name) like '%brain%'
            or lower(order_proc.display_name) like '%venogram%'
            or lower(order_proc.display_name) like '%angio%'
            or lower(order_proc.display_name) like '%us abd%'
            or lower(order_proc.display_name) like '%patency%'
        )
        and lower(zc_order_status.name) = 'completed'
        and date(order_proc._update_date) > current_date - 14
),
lines as (
    select distinct
        proc_ids.pat_mrn_id,
        proc_ids.order_proc_id,
        proc_ids.pat_enc_csn_id,
        order_narrative.line as seq_num,
        order_narrative.narrative as note,
        proc_ids.upd_dt
    from
        proc_ids
        inner join {{source('clarity_ods', 'order_narrative')}} as order_narrative
            on order_narrative.order_proc_id = proc_ids.order_proc_id
    union all
    select distinct
        proc_ids.pat_mrn_id,
        proc_ids.order_proc_id,
        proc_ids.pat_enc_csn_id,
        /* Hack: impression notes need to go after the narrative, so we add large number
         that sorts impression notes at the end */
        order_impression.line + 1000 as seq_num,
        case when order_impression.line = 1 then '\n\nIMPRESSION\n\n' else '' end
            || coalesce(order_impression.impression, '') as note,
        proc_ids.upd_dt
    from
        proc_ids
        inner join {{source('clarity_ods', 'order_impression')}} as order_impression
            on order_impression.order_proc_id = proc_ids.order_proc_id
)
select
    lines.order_proc_id,
    lines.pat_mrn_id,
    lines.pat_enc_csn_id,
    'PROC_ORDER_ID: ' || lines.order_proc_id || '\n'
        || 'PAT_MRN_ID: ' || lines.pat_mrn_id || '\n'
        || listagg(lines.note, ' ') within group(order by lines.seq_num asc) as full_note
from
    lines
    inner join {{source('lake_netezza', 'encounter_all')}} as encounter_all
        on encounter_all.csn = lines.pat_enc_csn_id
    left join {{source('lake_comprehend_medical', 'nlp_procedure_notes_results')}} as nlp_results
        on nlp_results.order_proc_id = lines.order_proc_id
where
    nlp_results.order_proc_id is null
    and (encounter_all.ed_ind = 1 or encounter_all.inpatient_ind = 1)
group by all
