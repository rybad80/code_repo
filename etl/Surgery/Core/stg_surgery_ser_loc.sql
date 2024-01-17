select
    clarity_ser.prov_id,
    clarity_ser.prov_name,
    or_ser_surg_srvc.line,
    clarity_loc.loc_id as location_id,
    clarity_loc.loc_name as location,
    clarity_ser.staff_resource,
    clarity_ser.staff_resource_c
from
    {{ source('clarity_ods', 'or_ser_surg_srvc') }} as or_ser_surg_srvc
    left join {{ source('clarity_ods', 'clarity_ser') }} as clarity_ser
        on or_ser_surg_srvc.prov_id = clarity_ser.prov_id
    left join {{ source('clarity_ods', 'clarity_loc') }} as clarity_loc
        on or_ser_surg_srvc.loc_id = clarity_loc.loc_id
