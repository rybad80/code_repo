with picklist as (--for each preference card, instrument details like name, id and required flags are listed
    select or_pklst.pick_list_id,
        or_pklst.pick_list_name,
        or_sply.supply_id,
        or_sply.supply_name,
        or_sply.active_yn,
        or_pklst_sup_list.num_needed_open as supply_num_needed_open,
        or_pklst_sup_list.num_supplies_prn as supply_num_supplies_prn,
        or_sply.primary_ext_id as supply_pri_ext_id,
        zc_or_type_of_item.name as surgical_supply_tag
    from
        {{source('clarity_ods', 'or_pklst')}} as or_pklst
    inner join
        {{source('clarity_ods', 'or_pklst_sup_list')}} as or_pklst_sup_list on
            or_pklst_sup_list.pick_list_id = or_pklst.pick_list_id
    inner join
        {{source('clarity_ods', 'or_sply')}} as or_sply on
            or_sply.supply_id = or_pklst_sup_list.supply_id
    left join
        {{source('clarity_ods', 'zc_or_type_of_item')}} as zc_or_type_of_item on
            zc_or_type_of_item.type_of_item_c = or_sply.type_of_item_c
    where
        pick_list_type_c != 2 --exclude picklists from actual cases 
),

supply_price as (--get the latest price for a supply
    select
        or_sply_ovtm.item_id,
        or_sply_ovtm.cost_per_unit_ot,
        row_number() over (
            partition by or_sply_ovtm.item_id order by or_sply_ovtm.effective_date desc
        ) as supply_price_latest_row_number
    from
        {{source('clarity_ods', 'or_sply_ovtm')}} as or_sply_ovtm
),

--all instruments are not used in every surgery location, this cte lists every single instrument and the location availability -- noqa: L016
--this cte is joined twice, once for supply_active_ind and once for supply_location_active_ind -- noqa: L016
--when this cte is used for the supply_active_ind: a row_number function is used to remove duplicates caused by multiple locations -- noqa: L016
supply_location as (
    select
        or_sply_loc_info.item_id as supply_id,
        or_sply.active_yn as supply_active_ind,
        or_sply_loc_info.active_or_loc_yn as supply_location_active_ind,
        clarity_loc.loc_id as supply_loc_id,
        clarity_loc.loc_name as supply_location_name,
        --remove duplicates caused by location, this table has one:many relationship at the supply_id: supply_location_name level -- noqa: L016
        row_number() over (
            partition by
                or_sply_loc_info.item_id
            order by or_sply.active_yn desc
        ) as supply_location_distinct_row_number
    from
        {{source('clarity_ods', 'or_sply')}} as or_sply
    inner join
        {{source('clarity_ods', 'or_sply_loc_info')}} as or_sply_loc_info on
            or_sply_loc_info.item_id = or_sply.supply_id
    inner join {{source('clarity_ods', 'clarity_loc')}} as clarity_loc on
            clarity_loc.loc_id = or_sply_loc_info.or_loc_id
)
select
      {{
        dbt_utils.surrogate_key([
            'tdl_preference_cards.or_proc_id',
            'tdl_preference_cards.preference_card_id',
            'tdl_preference_cards.location_id',
            'picklist.supply_id'
        ])
    }} as preference_card_supply_key,
    tdl_preference_cards.or_procedure_name,
    tdl_preference_cards.or_proc_id,
    tdl_preference_cards.or_procedure_active_ind,
    tdl_preference_cards.preference_card_name,
    tdl_preference_cards.preference_card_id,
    tdl_preference_cards.preference_card_active_ind,
    tdl_preference_cards.default_or_modified_preference_card,
    tdl_preference_cards.provider_name,
    tdl_preference_cards.provider_id,
    tdl_preference_cards.location_name,
    tdl_preference_cards.location_id,
    tdl_preference_cards.last_reviewed_date as preference_card_last_reviewed_date,
    tdl_preference_cards.picklist_id,
    tdl_preference_cards.preference_card_key,
    picklist.supply_name,
    picklist.supply_id,
    picklist.supply_pri_ext_id as supply_mfg_part_num,
    picklist.surgical_supply_tag as supply_category,
    supply_price.cost_per_unit_ot as supply_cost_per_unit,
    case picklist.active_yn
        when 'Y' then 1 else 0 end as picklist_active_ind,
    case supply_active.supply_active_ind
        when 'Y' then 1 else 0
    end as supply_active_ind,
    case supply_location.supply_location_active_ind
        when 'Y' then 1 else 0
    end as supply_location_active_ind,
    coalesce(
        picklist.supply_num_needed_open, 0
    ) as supply_needed_open_quantity,
    coalesce(
        picklist.supply_num_supplies_prn, 0
    ) as supply_use_as_needed_quantity,
    coalesce(
        case
            when
                picklist.supply_num_needed_open > 0 then picklist.supply_num_needed_open
            else picklist.supply_num_supplies_prn
        end,
        0
    ) as supply_needed_open_or_use_as_needed_quantity
from
    {{ref('stg_preference_cards')}} as tdl_preference_cards
left join
    picklist on
        picklist.pick_list_id = tdl_preference_cards.picklist_id
left join
    supply_location on
        supply_location.supply_id = picklist.supply_id
        and supply_location.supply_loc_id = tdl_preference_cards.location_id
left join
    supply_location as supply_active on
        supply_active.supply_id = picklist.supply_id and supply_active.supply_location_distinct_row_number = 1
left join
    supply_price on
        supply_price.item_id = picklist.supply_id and supply_price.supply_price_latest_row_number = 1
