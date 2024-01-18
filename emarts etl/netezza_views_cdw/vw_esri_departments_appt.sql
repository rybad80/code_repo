select
    distinct dg.dept_key,
    dg.x,
    dg.y
from
    (
        {{source('cdw', 'visit')}} as vi
        join (
            select
                dg.dept_key,
                dg.street_long_deg_x as x,
                dg.street_lat_deg_y as y
            from
                {{source('cdw', 'department_geographical_spatial_info')}} as dg
        ) dg on ((vi.dept_key = dg.dept_key))
    )
where
    (
        vi.appt_made_dt = "TIMESTAMP"((date('now(0)' :: "VARCHAR") - 1))
    )
