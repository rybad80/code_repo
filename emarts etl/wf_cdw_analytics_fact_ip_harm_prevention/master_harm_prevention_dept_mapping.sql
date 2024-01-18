select
    cast(a.harm_type as varchar(30)) as harm_type,
    d_current.dept_key as current_dept_key,
    d_hist.dept_key as historical_dept_key,
    cast(a.current_cost_cntr_cd as varchar(30)) as current_cost_cntr_cd,
    cast(a.current_dept_id as bigint) as current_dept_id,
    cast(a.historical_dept_id as bigint) as historical_dept_id,
    cast(d_current.dept_nm as varchar(300)) as current_dept_nm,
    cast(d_hist.dept_nm as varchar(300)) as historical_dept_nm,
    cast(d_current.dept_abbr as varchar(20)) as current_dept_abbr,
    cast(d_hist.dept_abbr as varchar(20)) as historical_dept_abbr,
    cast(a.start_dt as timestamp) as start_dt,
    cast(a.end_dt as timestamp) as end_dt,
    cast(a.denominator_only_ind as byteint) as denominator_only_ind,
    cast(a.unit_move_ind as byteint) as unit_move_ind,
    current_timestamp as create_dt,
    'DBT' as create_by,
    current_timestamp as upd_dt
from
    {{ref('stg_harm_dept_mapping')}} as a
    left join {{source('cdw', 'department')}} as d_current on d_current.dept_id = a.current_dept_id
    left join {{source('cdw', 'department')}} as d_hist on d_hist.dept_id = a.historical_dept_id
