select
    stg_restraints_orders.restraint_episode_key,
    max(case --Alternatives used prior to restraint
        when ord_spec_quest.ord_quest_id = '500200299'
            and ord_spec_quest.ord_quest_resp is not null
            then 1 else 0 end) as order_alternatives_ind,
    max(case --Rationale for use
        when ord_spec_quest.ord_quest_id = '500201313'
            and ord_spec_quest.ord_quest_resp is not null
            then 1 else 0 end) as order_rationale_ind,
    max(case --Restraint Method
        when ord_spec_quest.ord_quest_id = '500200301'
            and ord_spec_quest.ord_quest_resp is not null
            then 1 else 0 end) as order_restraint_method_ind,
    max(case --Device
        when ord_spec_quest.ord_quest_id in (
            '500200300',
            '500200421'
            ) and ord_spec_quest.ord_quest_resp is not null
            then 1 else 0 end) as order_device_ind,
    max(case --Device Location
        when ord_spec_quest.ord_quest_id in(
            '130217', --Limb Holders
            '130218', --No Nos
            '130219', --Peek a Boo Mitts
            '130220', --TATs
            '130221', --Other
            '500200312' --Device Location
            ) and ord_spec_quest.ord_quest_resp is not null
            then 1 else 0 end) as order_device_location_ind,
    max(case --Manual Physical Hold
        when ord_spec_quest.ord_quest_id = '500200301'
            and lower(ord_spec_quest.ord_quest_resp) like '%manual%'
            then 1 else 0 end) as order_manual_hold_ind,
    case --if a device is used, limbs also needs to be documented
        when order_device_ind + order_device_location_ind = 2
        --otherwise, manual hold should be documented
            or order_manual_hold_ind = 1
            then 1 else 0 end as device_ind,
    (order_alternatives_ind
        + order_rationale_ind
        + order_restraint_method_ind
        + device_ind) / 4 as order_complete_ind
from
    {{ ref('stg_restraints_orders') }} as stg_restraints_orders
    inner join {{ source('clarity_ods', 'ord_spec_quest')}} as ord_spec_quest
    on stg_restraints_orders.procedure_order_id = ord_spec_quest.order_id
where
    stg_restraints_orders.order_number = 1
    and ord_spec_quest.ord_quest_id in (
        '500200299', --Alternatives used prior to restraint
        '500201313', --Rationale for use
        '500200301', --Restraint Method
        '500200300', --Device (< 9 Yrs)
        '500200421', --Device (>9 Yrs)
        '130217', --Device Location (Limb Holders)
        '130218', --Device Location (No Nos)
        '130219', --Device Location (Peek a Boo Mitts)
        '130220', --Device Location (TATs)
        '130221', --Device Location (Other)
        '500200312' --Device Location
    )
group by
    stg_restraints_orders.restraint_episode_key
