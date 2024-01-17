--get the latest record for any address id, removing duplicates
with address_location as (
    select
        location_address.address_id as ship_to_address_id,
        location_address.address_line_1 as ship_to_address_line1,
        location_address.location_id as ship_to_address_location,
        location.location_name as ship_to_address_name,
        row_number() over (partition by location_address.address_id
        order by location_address.upd_dt desc) as address_id_latest_row_number
    from
        {{source('workday_ods', 'location_address')}} as location_address
        left join {{source('workday_ods', 'location')}} as location -- noqa: L029
            on location.location_id = location_address.location_id
)

select
    *
from
    address_location
where
    address_id_latest_row_number = 1
