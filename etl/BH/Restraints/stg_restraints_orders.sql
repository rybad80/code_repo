with restraint_details as (
    --region details about each restraint
    select
        restraint_episode_key,
        visit_key,
        device_type, --manual hold or device restraint?
        restraint_start,
        restraint_removal,
        --identify prior restraint with same device and restraint types
        lag(restraint_start) over (
            partition by
                visit_key,
                device_type
            order by
                restraint_start
        ) as prior_restraint_start,
        lag(restraint_removal) over (
            partition by
                visit_key,
                device_type
            order by
                restraint_start
        ) as prior_restraint_removal
    from
        {{ ref('stg_restraints') }}
    where
        violent_restraint_ind = 1
--end region
),

violent_manual_order as (
    --region identify if the violent restraint order is for a manual hold
    --non-violent restraints always use devices
    select distinct
        order_id,
        'Manual' as order_type
    from
      {{source('clarity_ods', 'ord_spec_quest')}}
    where
        ord_quest_id = '500200301' --restraint method (violent)
        and ord_quest_resp like 'Manual%'
),

restraint_orders as (
    --region identify restraint orders and whether a manual hold or device was ordered
    select distinct
        procedure_order_clinical.visit_key,
        procedure_order_clinical.proc_ord_key,
        procedure_order_clinical.procedure_order_id,
        procedure_order_clinical.placed_date,
        --any order that isn't a manual hold is for a device restraint
        coalesce(
            violent_manual_order.order_type,
            'Device'
        ) as order_type
    from
        {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        left join violent_manual_order
            on procedure_order_clinical.procedure_order_id = violent_manual_order.order_id
    where
        procedure_order_clinical.procedure_order_type = 'Child Order'
        and procedure_order_clinical.procedure_id in (
            80910, --Restraints Behavior Management PT < 9 yrs
            80918 --Restraints Behavior Management PT > 9yrs
        )
),

orders_per_restraint_right_limit as (
    --region begin relating orders to restraints
    /*Right Limit*/
    --for restraints > 15 minutes, list of potential orders is right-bounded by the restraint removal date
    --for restraints <= 15, consider all orders up to 15 minutes after initiation (based on compliance)
    /*Left Limit*/
    --consider all orders placed after the last restraint of the same group and type's removal date
    select
        restraint_details.restraint_episode_key,
        restraint_details.device_type,
        restraint_orders.proc_ord_key,
        restraint_orders.procedure_order_id,
        restraint_details.restraint_start,
        restraint_details.restraint_removal,
        restraint_orders.placed_date,
        --was the order placed before this restraint started?
        --if it is the nearest order to the current restraint's initiation, include it
        --otherwise, assume the current restraint's first order was placed after initiation
        case when restraint_orders.placed_date < restraint_details.restraint_start
                then 1 else 0 end as order_before_initiation_ind,
        --if there are orders placed before restraint initiation, returns the one with the latest date
        first_value(restraint_orders.placed_date) over(
            partition by
                restraint_details.restraint_episode_key
            order by
                order_before_initiation_ind desc,
                restraint_orders.placed_date desc
        ) as latest_order_before_initiation,
        --if there are orders placed after restraint initiation, returns the one with the earliest date
        first_value(restraint_orders.placed_date) over(
            partition by
                restraint_details.restraint_episode_key
            order by
                order_before_initiation_ind,
                restraint_orders.placed_date
        ) as earliest_order_after_initiation
    from
        restraint_details
        --join to orders, ensuring the restraint group and device types align
        inner join restraint_orders
            on restraint_details.visit_key = restraint_orders.visit_key
    where
        restraint_details.device_type = restraint_orders.order_type
        /*Right Limit*/
        and (
            --ongoing restraint or no discontinue time; consider all orders
            restraint_details.restraint_removal is null
            --order placed prior to removal
            or restraint_orders.placed_date <= restraint_details.restraint_removal
            --or order placed within 15 minutes of start, for super short durations
            or minutes_between(restraint_orders.placed_date, restraint_details.restraint_start) <= 15
        )
        /*Left Limit*/
        and (
            --no prior restraint; consider all orders before restraint initiation
            restraint_details.prior_restraint_removal is null
            --prior restraint exists; consider all orders after it's removal
            or restraint_orders.placed_date > restraint_details.prior_restraint_removal
        )
)

--region occasionally, a restraint isn't documented in flowsheets but has an order
--left-limit the orders using the order closest to restraint initiation
select
    restraint_episode_key,
    restraint_start,
    restraint_removal,
    proc_ord_key,
    procedure_order_id,
    placed_date,
    --use rank when orders placed at the same time
    rank() over(
        partition by
            restraint_episode_key
        order by
            placed_date
    ) as order_number
from
    orders_per_restraint_right_limit
where
    --order placed before initiation, but closest order to start time
    (
        order_before_initiation_ind = 1
        and placed_date = latest_order_before_initiation
        and (
            minutes_between(restraint_start, latest_order_before_initiation)
            <= minutes_between(earliest_order_after_initiation, restraint_start)
            or earliest_order_after_initiation is null
        )
    )
    --order placed after initiation and is therefore related to the current restraint
    or order_before_initiation_ind = 0
