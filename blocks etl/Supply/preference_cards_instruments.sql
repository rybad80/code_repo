with instrument as (--for each preference card, instrument details like name, id and required flags are listed-- noqa: L016  
    select
        or_proc_instr.or_proc_id,
        or_proc_instr.line,
        or_proc_instr.instr_req_c,
        or_proc_instr.instr_req_yn,
        or_proc_instr.num_instr_req,
        zc_or_instr_type.name
    from
        {{source('clarity_ods', 'or_proc_instr')}} as or_proc_instr
    inner join
        {{source('clarity_ods', 'zc_or_instr_type')}} as zc_or_instr_type on
            zc_or_instr_type.instrument_type_c = or_proc_instr.instr_req_c
),
--all instruments are not used in every surgery location, the next two cte's lists every single instrument and the location availability-- noqa: L016
or_tank_table as (--master list of instruments
    select
        or_tank.instrument_type_c,
        or_tank.status_yn,
        or_tank.tank_id,
        or_tank.avail_cases_yn,
        --remove duplicates caused by test instrument ids
        case
            when
                (or_tank.tank_name like 'EAF%' and substr(or_tank.tank_name, 14, 1) = 7) then 0
            else 1
        end as eaf_instruments_exclude_ind
    from
        {{source('clarity_ods', 'or_tank')}} as or_tank
    where
        or_tank.surg_rec_type_c = 7 --instruments only 
),
--this cte is joined twice, once for instrument_active_ind and once for instrument_loc_id-- noqa: L016
--when this cte is used for the instrument_active_ind: a row_number function is used to remove duplicates caused by multiple locations-- noqa: L016 
instrument_location as (
    select
        or_tank_table.instrument_type_c as instrument_id,
        or_tank_table.status_yn,
        or_tank_auth_loc.location_id as instrument_loc_id,
        or_tank_table.tank_id,
        or_tank_table.avail_cases_yn,
        --remove duplicates caused by location, this table has one:many relationship at the instrument_id: instrument_loc_id level-- noqa: L016  
        row_number() over (
            partition by
                or_tank_table.instrument_type_c
            order by or_tank_table.status_yn desc
        ) as instrument_location_distinct_row_number
    from
        or_tank_table
    left join
        {{source('clarity_ods', 'or_tank_auth_loc')}} as or_tank_auth_loc on
            or_tank_auth_loc.tank_id = or_tank_table.tank_id
    where
        eaf_instruments_exclude_ind = 1 --exclude test instrument ids 
)

select --procedure id and name 
    {{
        dbt_utils.surrogate_key([
            'tdl_preference_cards.or_proc_id',
            'tdl_preference_cards.preference_card_id',
            'tdl_preference_cards.location_id',
            'instrument.instr_req_c',
            'instrument.line'
        ])
    }} as preference_card_instrument_key,
    tdl_preference_cards.or_procedure_name,
    tdl_preference_cards.or_proc_id,
    tdl_preference_cards.or_procedure_active_ind,
    --preference card details 
    tdl_preference_cards.preference_card_name,
    tdl_preference_cards.preference_card_id,
    tdl_preference_cards.preference_card_active_ind,
    tdl_preference_cards.default_or_modified_preference_card,
    tdl_preference_cards.provider_name,
    tdl_preference_cards.provider_id,
    tdl_preference_cards.location_name,
    tdl_preference_cards.location_id,
    tdl_preference_cards.last_reviewed_date as preference_card_last_reviewed_date,
    --instrument details
    tdl_preference_cards.preference_card_key,
    instrument.line as instrument_list_line,
    instrument.name as instrument_name,
    instrument.instr_req_c as instrument_id,
    case
        when instrument_active.status_yn = 'Y' then 1 else 0
    end as instrument_active_ind,
    case
        when instrument_location.instrument_loc_id is not null then 1 else 0
    end as instrument_location_authorization_ind,
    case
        when instrument.instr_req_yn = 'Y' then 1 else 0
    end as instrument_required_indicator,
    case
        when instrument_active.avail_cases_yn = 'Y' then 1 else 0
    end as instrument_available_for_cases_ind,
    coalesce(instrument.num_instr_req, 0) as number_of_instruments_required
from
    {{ref('stg_preference_cards')}} as tdl_preference_cards
left join
    instrument on
        instrument.or_proc_id = tdl_preference_cards.preference_card_id
left join
    instrument_location on
        instrument_location.instrument_id = instrument.instr_req_c
        and instrument_location.instrument_loc_id = tdl_preference_cards.location_id
left join
    instrument_location as instrument_active on
        instrument_active.instrument_id = instrument.instr_req_c
        and instrument_active.instrument_location_distinct_row_number = 1
