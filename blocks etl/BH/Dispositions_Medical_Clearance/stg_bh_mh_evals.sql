with mh_note_eval_dispos as (
select
  element_value as disposition,
  smart_data_element_all.entered_date,
  smart_data_element_all.visit_key
from
    {{ ref('smart_data_element_all') }} as smart_data_element_all
    inner join {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
        on note_edit_metadata_history.note_key = smart_data_element_all.note_key
where
   concept_id = 'CHOP#6173'  -- ED Evaluation Note Disposition
   and note_status_id in (2, 3)
   and last_edit_ind = 1
),

mh_note_eval_dispos_concatenated as (
select
  cast(group_concat(mh_note_eval_dispos.disposition) as varchar(200)) as disposition,
  row_number() over
    (partition by mh_note_eval_dispos.visit_key order by mh_note_eval_dispos.entered_date)
        as disposition_seq,
  row_number() over
    (partition by mh_note_eval_dispos.visit_key order by mh_note_eval_dispos.entered_date desc)
    as disposition_seq_desc,
  mh_note_eval_dispos.entered_date,
  mh_note_eval_dispos.visit_key
from
  mh_note_eval_dispos
group by
  mh_note_eval_dispos.entered_date,
  mh_note_eval_dispos.visit_key
),

mh_note_eval_dispos_summary as (
select
    mh_note_eval_dispos_concatenated.visit_key,
    min(case
      when mh_note_eval_dispos_concatenated.disposition_seq = 1
      then mh_note_eval_dispos_concatenated.disposition end) as ed_dispo_first,
    min(case
      when mh_note_eval_dispos_concatenated.disposition_seq = 1
      then mh_note_eval_dispos_concatenated.entered_date end) as ed_dispo_first_time,
    min(case
      when mh_note_eval_dispos_concatenated.disposition_seq_desc = 1
      then mh_note_eval_dispos_concatenated.disposition end) as ed_dispo_last,
    min(case
      when mh_note_eval_dispos_concatenated.disposition_seq_desc = 1
      then mh_note_eval_dispos_concatenated.entered_date end) as ed_dispo_last_time
from
    mh_note_eval_dispos_concatenated
group by
    mh_note_eval_dispos_concatenated.visit_key
),

eating_disorder_dispos as (
select
    smart_data_element_all.visit_key,
    cast(smart_data_element_all.element_value as varchar(100)) as element_value,
    smart_data_element_all.entered_date,
    row_number() over
        (partition by smart_data_element_all.visit_key order by smart_data_element_all.entered_date)
        as dispo_seq_num,
    row_number() over
        (partition by smart_data_element_all.visit_key order by smart_data_element_all.entered_date desc)
        as dispo_seq_num_desc
from
    {{ ref('smart_data_element_all') }} as smart_data_element_all
    inner join {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
        on note_edit_metadata_history.note_key = smart_data_element_all.note_key
where
   concept_id = 'CHOP#6663'  -- Eating Disorder Disposition
   and note_status_id in (2, 3)
   and last_edit_ind = 1
),

eating_disorders_dispo_summary as (
select
    eating_disorder_dispos.visit_key,
    min(case
        when eating_disorder_dispos.dispo_seq_num = 1
        then eating_disorder_dispos.element_value end)
        as eating_disorder_dispo_first,
    min(case
        when eating_disorder_dispos.dispo_seq_num = 1
        then eating_disorder_dispos.entered_date end)
        as eating_disorder_dispo_first_time,
    max(case
        when eating_disorder_dispos.dispo_seq_num_desc = 1
        then eating_disorder_dispos.element_value end)
        as eating_disorder_dispo_last,
    max(case
        when eating_disorder_dispos.dispo_seq_num_desc = 1
        then eating_disorder_dispos.entered_date end)
        as eating_disorder_dispo_last_time
from
    eating_disorder_dispos
group by
    eating_disorder_dispos.visit_key
)

select
    mh_note_eval_dispos_summary.visit_key,
    mh_note_eval_dispos_summary.ed_dispo_first,
    mh_note_eval_dispos_summary.ed_dispo_first_time,
    mh_note_eval_dispos_summary.ed_dispo_last,
    mh_note_eval_dispos_summary.ed_dispo_last_time,
    case when mh_note_eval_dispos_summary.ed_dispo_first like '%Inpatient%'
        then 1 else 0 end as ed_dispo_first_ip_ind,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Inpatient%'
        then 1 else 0 end as ed_dispo_last_ip_ind,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Eating Disorder%'
        then 1 else 0 end as ed_dispo_last_eating_ind,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Substance%'
        then 1 else 0 end as ed_dispo_last_substance_ind,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Wraparound%'
        then 1 else 0 end as ed_dispo_last_wrap_ind,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Partial hospitalization%'
        then 1 else 0 end as ed_dispo_last_php_ind,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Outpatient therapy%'
        then 1 else 0 end as ed_dispo_last_op_psychotherapy_ind,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Outpatient psychiatry%'
        then 1 else 0 end as ed_dispo_last_op_psychiatry_ind,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Children''s Movile%' --noqa: PRS
      then 1 else 0 end as ed_dispo_last_cmis_ind, --noqa: L019
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%None%'
        then 1 else 0 end as ed_dispo_last_none,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Deferred%'
        then 1 else 0 end as ed_dispo_last_deferred,
    case when mh_note_eval_dispos_summary.ed_dispo_last like '%Other'
        then 1 else 0 end as ed_dispo_last_other,
    eating_disorders_dispo_summary.eating_disorder_dispo_first,
    eating_disorders_dispo_summary.eating_disorder_dispo_first_time,
    eating_disorders_dispo_summary.eating_disorder_dispo_last,
    eating_disorders_dispo_summary.eating_disorder_dispo_last_time
from
  mh_note_eval_dispos_summary
  left join eating_disorders_dispo_summary
    on eating_disorders_dispo_summary.visit_key = mh_note_eval_dispos_summary.visit_key
