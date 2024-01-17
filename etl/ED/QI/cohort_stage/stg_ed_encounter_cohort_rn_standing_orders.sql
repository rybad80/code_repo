with standing_ordersets as (
    select
      fact_edqi.visit_key,
      fact_edqi.pat_key,
      ept_sel_smartsets.pat_enc_csn_id,
      max(case
              when ept_sel_smartsets.selected_sset_id = 746
                then 1
              else 0
          end) as pathway_ind,
      max(case
              when ept_sel_smartsets.selected_sset_id = 300093
                then 1
              else 0
          end) as standard_ind,
      max(case
              when ept_sel_smartsets.selected_sset_id = 800408
                then 1
              else 0
          end) as triage_ind
    from
      {{ source('cdw_analytics', 'fact_edqi') }} as fact_edqi
      inner join {{ ref('stg_encounter') }} as stg_encounter on fact_edqi.visit_key = stg_encounter.visit_key
      inner join {{ source('clarity_ods', 'ept_sel_smartsets') }} as ept_sel_smartsets
        on fact_edqi.enc_id = ept_sel_smartsets.pat_enc_csn_id
    where
      admin.year(stg_encounter.encounter_date) >= year(current_date) - 5
      and ept_sel_smartsets.selected_sset_id in (
                                                 746,     -- ED Nursing Pathway Standing Orders
                                                 300093,  -- ED Nursing Standing Orders
                                                 800408   -- ED Triage Order Set
                                                )
    group by
      fact_edqi.visit_key,
      fact_edqi.pat_key,
      ept_sel_smartsets.pat_enc_csn_id
)

select
  standing_ordersets.visit_key,
  standing_ordersets.pat_key,
  'RN_STANDING_ORDERS' as cohort,
  case
      when standing_ordersets.pathway_ind = 0
           and standing_ordersets.standard_ind = 0
           and standing_ordersets.triage_ind = 1
        then 'Triage'
      when standing_ordersets.pathway_ind = 0
           and standing_ordersets.standard_ind = 1
           and standing_ordersets.triage_ind = 0
        then 'Standard'
      when standing_ordersets.pathway_ind = 0
           and standing_ordersets.standard_ind = 1
           and standing_ordersets.triage_ind = 1
        then 'Standard & Triage'
      when standing_ordersets.pathway_ind = 1
           and standing_ordersets.standard_ind = 0
           and standing_ordersets.triage_ind = 0
        then 'Pathway'
      when standing_ordersets.pathway_ind = 1
           and standing_ordersets.standard_ind = 0
           and standing_ordersets.triage_ind = 1
        then 'Pathway & Triage'
      when standing_ordersets.pathway_ind = 1
           and standing_ordersets.standard_ind = 1
           and standing_ordersets.triage_ind = 0
        then 'Pathway & Standard'
      when standing_ordersets.pathway_ind = 1
           and standing_ordersets.standard_ind = 1
           and standing_ordersets.triage_ind = 1
        then 'Pathway, Standard, & Triage'
  end as subcohort
from
  standing_ordersets
