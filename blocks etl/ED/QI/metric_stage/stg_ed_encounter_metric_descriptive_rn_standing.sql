with smartsets as (
    select
      cl_prl_ss.protocol_id,
      cl_prl_ss.protocol_name
    from
      {{source('clarity_ods', 'cl_prl_ss')}} as cl_prl_ss
    where
      cl_prl_ss.protocol_id in (
                               746,     -- ED Nursing Pathway Standing Orders
                               300093,  -- ED Nursing Standing Orders
                               800408   -- ED Triage Order Set
                               )
),

smartset_ordered_meds as (
    select
      smartsets.protocol_id,
      medication_order_administration.visit_key,
      medication_order_administration.med_ord_key,
      max(case
              when medication_order_signed.verb_sig_emp_key != -1
                then 1
              else 0
          end) as verbal_order_signed_ind
    from
      smartsets
      inner join {{ref('medication_order_administration')}} as medication_order_administration
        on lower(smartsets.protocol_name) = lower(medication_order_administration.orderset_name)
      left join {{source('cdw', 'medication_order_signed')}} as medication_order_signed
        on medication_order_administration.med_ord_key = medication_order_signed.med_ord_key
    group by
      smartsets.protocol_id,
      medication_order_administration.visit_key,
      medication_order_administration.med_ord_key
),

smartset_ordered_procs as (
    select
      smartsets.protocol_id,
      procedure_order_clinical.visit_key,
      procedure_order_clinical.proc_ord_key,
      max(case
              when procedure_order_signed.verb_sig_emp_key != -1
                then 1
              else 0
          end) as verbal_order_signed_ind
    from
      smartsets
      inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
        on lower(smartsets.protocol_name) = lower(procedure_order_clinical.orderset_name)
      left join {{ source('cdw', 'procedure_order_signed') }} as procedure_order_signed
        on procedure_order_clinical.proc_ord_key = procedure_order_signed.proc_ord_key
    where
      lower(procedure_order_clinical.procedure_order_type) = 'parent order'
    group by
      smartsets.protocol_id,
      procedure_order_clinical.visit_key,
      procedure_order_clinical.proc_ord_key
),

smartset_orders as (
    select
      'medications' as order_source,
      smartset_ordered_meds.protocol_id,
      smartset_ordered_meds.visit_key,
      smartset_ordered_meds.med_ord_key as order_key,
      smartset_ordered_meds.verbal_order_signed_ind
    from
      smartset_ordered_meds

    union

    select
      'procedures' as order_source,
      smartset_ordered_procs.protocol_id,
      smartset_ordered_procs.visit_key,
      smartset_ordered_procs.proc_ord_key,
      smartset_ordered_procs.verbal_order_signed_ind
    from
      smartset_ordered_procs
)

select
  smartset_orders.visit_key,
  count(distinct
        case
            when smartset_orders.protocol_id = 300093  -- ED Nursing Standing Orders
              then smartset_orders.order_key
        end) as rn_standing_standard_orders_placed,
  count(distinct
        case
            when smartset_orders.protocol_id = 300093  -- ED Nursing Standing Orders
                  and smartset_orders.verbal_order_signed_ind = 1
              then smartset_orders.order_key
        end) as rn_standing_standard_orders_signed,
  count(distinct
        case
            when smartset_orders.protocol_id = 800408   -- ED Triage Order Set
              then smartset_orders.order_key
        end) as rn_standing_triage_orders_placed,
  count(distinct
        case
            when smartset_orders.protocol_id = 800408   -- ED Triage Order Set
                 and smartset_orders.verbal_order_signed_ind = 1
              then smartset_orders.order_key
        end) as rn_standing_triage_orders_signed,
  count(distinct
        case
            when smartset_orders.protocol_id = 746     -- ED Nursing Pathway Standing Orders
              then smartset_orders.order_key
        end) as rn_standing_pathway_orders_placed,
  count(distinct
        case
            when smartset_orders.protocol_id = 746     -- ED Nursing Pathway Standing Orders
                 and smartset_orders.verbal_order_signed_ind = 1
              then smartset_orders.order_key
        end) as rn_standing_pathway_orders_signed
from
  smartset_orders
  inner join {{ref('stg_ed_encounter_cohort_all')}} as stg_ed_encounter_cohort_all
    on smartset_orders.visit_key = stg_ed_encounter_cohort_all.visit_key
group by
  smartset_orders.visit_key
