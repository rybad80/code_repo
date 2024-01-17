{{ config(meta = {
    'critical': true
}) }}

with anesthesia_records as (
    select
        coalesce(surgery_log.or_key, surgery_case.or_key) as or_key,
        surgery_case.or_key as case_key,
        surgery_log.or_key as log_key,
        f_an_record_summary.case_id,
        f_an_record_summary.log_id,
        an_hsb_link_info.summary_block_id as anesthesia_id,
        an_hsb_link_info.an_52_enc_csn_id::numeric(14, 3) as anesthesia_csn,
        an_hsb_link_info.an_proc_name as anesthesia_proc_name,
        an_hsb_link_info.an_start_datetime as anesthesia_start,
        to_char(an_hsb_link_info.an_date, 'YYYY-MM-DD') as anesthesia_start_date,
        to_char(an_hsb_link_info.an_time, 'HH24:MI:SS') as anesthesia_start_time,
        an_hsb_link_info.an_stop_datetime as anesthesia_stop,
        an_hsb_link_info.an_resp_prov_id as anesthesia_prov_id,
        an_hsb_link_info.an_doc_comp_instant as anesthesia_doc_comp,
        case when an_hsb_link_info.anes_doc_comp_yn = 'Y' then 1 end as anesthesia_doc_comp_ind,
        case when an_hsb_link_info.anes_intraop_com_yn = 'Y' then 1 end as anesthesia_intraop_comp_ind,
        case
            when an_hsb_link_info.anes_preop_comp_yn = 'Y' then 1
            when an_hsb_link_info.anes_preop_comp_yn = 'N' then 0
        end as anesthesia_preop_comp_ind,
        case when an_hsb_link_info.an_unlinked_flag_yn = 'Y' then 1 end as anesthesia_unlinked_flag_ind
    from
        {{ source('clarity_ods', 'an_hsb_link_info') }} as an_hsb_link_info
        left join {{ source('clarity_ods', 'f_an_record_summary') }} as f_an_record_summary
            on an_hsb_link_info.summary_block_id = f_an_record_summary.an_episode_id
        left join {{ ref('stg_surgery_keys_xref') }} as surgery_log
            on f_an_record_summary.log_id = surgery_log.or_id
            and surgery_log.src_id = 1
            and surgery_log.source_system = 'CLARITY'
        left join {{ ref('stg_surgery_keys_xref') }} as surgery_case
            on f_an_record_summary.case_id = surgery_case.or_id
            and surgery_case.src_id = 2
            and surgery_case.source_system = 'CLARITY'
),

anesthesia_link as (
    select
        case when or_log_key = 0 then null else or_log_key end as log_key,
        case when or_case_key = 0 then null else or_case_key end as case_key,
        coalesce(log_key, case_key) as or_key,
        anes_key,
        anes_visit_key,
        anes_id,
        prov_key
    from
        {{ source('cdw', 'anesthesia_encounter_link') }}
    where
        or_key is not null
        and anes_key not in (0, -1)
)

select
    anesthesia_records.or_key,
    anesthesia_records.case_key,
    anesthesia_records.log_key,
    anesthesia_link.anes_key,
    anesthesia_link.anes_visit_key,
    anesthesia_link.prov_key,
    anesthesia_records.case_id,
    anesthesia_records.log_id,
    anesthesia_records.anesthesia_id,
    anesthesia_records.anesthesia_csn,
    anesthesia_records.anesthesia_proc_name,
    anesthesia_records.anesthesia_start,
    anesthesia_records.anesthesia_start_date,
    anesthesia_records.anesthesia_start_time,
    anesthesia_records.anesthesia_stop,
    anesthesia_records.anesthesia_prov_id,
    anesthesia_records.anesthesia_doc_comp,
    anesthesia_records.anesthesia_doc_comp_ind,
    anesthesia_records.anesthesia_intraop_comp_ind,
    anesthesia_records.anesthesia_preop_comp_ind,
    anesthesia_records.anesthesia_unlinked_flag_ind
from
    anesthesia_records
    left join anesthesia_link
        on anesthesia_records.or_key = anesthesia_link.or_key
