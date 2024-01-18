{{ config(meta = {
    'critical': true
}) }}

with
bh_dispo_orders as (
select
    procedure_order_clinical.visit_key,
    procedure_order_clinical.placed_date,
    procedure_order_clinical.proc_ord_key,
    procedure_order_clinical.procedure_order_id,
    max(case when ord_spec_quest.ord_quest_id = '130376' then ord_spec_quest.ord_quest_resp end)
        as mc_yes_no,
    max(case when ord_spec_quest.ord_quest_id = '130377' then ord_spec_quest.ord_quest_resp end)
        as mc_expected_time_frame,
    case when row_number() over
        (partition by procedure_order_clinical.visit_key order by procedure_order_clinical.placed_date) = 1
        then 1
        else 0
        end as first_dispo_order_ind,
    case when row_number() over
        (partition by procedure_order_clinical.visit_key order by procedure_order_clinical.placed_date desc) = 1
        then 1
        else 0
        end as last_dispo_order_ind
from
    {{ ref('procedure_order_clinical') }} as procedure_order_clinical
    inner join {{source('clarity_ods', 'ord_spec_quest')}} as ord_spec_quest
        on ord_spec_quest.order_id = procedure_order_clinical.procedure_order_id
where ord_spec_quest.ord_quest_id in ('130377', '130376')
    and procedure_order_clinical.proc_ord_parent_key > 0
group by procedure_order_clinical.visit_key,
    procedure_order_clinical.proc_ord_key,
    procedure_order_clinical.procedure_order_id,
    procedure_order_clinical.placed_date
)

select
    bh_dispo_orders.visit_key,
    min(case when bh_dispo_orders.mc_yes_no = 'Yes' then bh_dispo_orders.placed_date end)
        as first_mc_yes_order_date,
    max(case when bh_dispo_orders.mc_yes_no = 'Yes' then bh_dispo_orders.placed_date end)
        as last_mc_yes_order_date,
    cast(max(case when bh_dispo_orders.last_dispo_order_ind = 1 then bh_dispo_orders.mc_yes_no end)
        as varchar(30))
        as last_mc_status,
    max(bh_dispo_orders.placed_date) as last_mc_status_date,
    cast(max(case when bh_dispo_orders.last_dispo_order_ind = 1
        and bh_dispo_orders.mc_yes_no = 'No' then bh_dispo_orders.mc_expected_time_frame end)
        as varchar(30))
        as mc_expected_time_frame
from
    bh_dispo_orders
group by
    bh_dispo_orders.visit_key
