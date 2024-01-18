with stage as (
    select
        full_dt,
        --need to wrap this in date() or else it returns a timestamp field
        date(date_trunc('month', full_dt)) as visual_month,
        --visual month is inclusive, so this will be used to pull in utilization through the visual month
        add_months(date_trunc('month', full_dt), 1) - 1 as visual_month_end,
        c_mm,
        c_yy || '-Q' || c_qtr as cy_qtr,
        c_yyyy,
        c_yy as cy,
        to_date(c_yyyy || '-01-01', 'yyyy-mm-dd') as cy_year_start,
        to_date(c_yyyy || '-12-31', 'yyyy-mm-dd') as cy_year_end,
        f_mm,
        case
            when c_mm in (7, 8, 9) then f_yy || '-Q1'
            when c_mm in (10, 11, 12) then f_yy || '-Q2'
            when c_mm in (1, 2, 3) then f_yy || '-Q3'
            when c_mm in (4, 5, 6) then f_yy || '-Q4'
            else 'CHECK'
        end as fy_qtr,
        f_yyyy,
        f_yy as fy,
        to_date(f_yyyy || '-07-01', 'yyyy-mm-dd') as fy_year_start,
        to_date(f_yyyy + 1 || '-06-30', 'yyyy-mm-dd') as fy_year_end,
        f_day,
        c_day,
        day_of_mm
    from
        {{ source('cdw', 'master_date') }}
    where
        full_dt >= to_date('2016-01-01', 'yyyy-mm-dd')
        and full_dt < current_date
),

fy_row as (
    select
        fy,
        lag(fy) over(order by fy asc) as prev_fy,
        lag(f_yyyy) over(order by f_yyyy) as prev_f_yyyy
    from
        stage
    group by
        fy,
        f_yyyy
),

cy_row as (
    select
        cy,
        lag(cy) over(order by cy asc) as prev_cy
    from
        stage
    group by
        cy
),

leap_year_cy as (
    select
        cy,
        case when max(c_day) > 365 then 1 else 0 end as leap_year_cy_ind
    from
        stage
    group by
        cy
),

leap_year_fy as (
    select
        fy,
        case when max(f_day) > 365 then 1 else 0 end as leap_year_fy_ind
    from
        stage
    group by
        fy
)

select
    stage.full_dt,
    stage.visual_month,
    stage.visual_month_end,
    stage.c_mm,
    stage.cy_qtr,
    stage.c_yyyy,
    stage.cy,
    stage.cy_year_start,
    stage.cy_year_end,
    stage.f_mm,
    stage.fy_qtr,
    stage.f_yyyy,
    stage.fy,
    stage.fy_year_start,
    stage.fy_year_end,
    stage.f_day as unadjusted_f_day,
    stage.c_day as unadjusted_c_day,
    stage.day_of_mm,
    case when stage.f_day >= 244 and leap_year_fy_ind = 1 then stage.f_day - 1 else stage.f_day end as f_day,
    case when stage.c_day >= 60 and leap_year_cy_ind = 1 then stage.c_day - 1 else stage.c_day end as c_day,
    fy_row.prev_fy,
    fy_row.prev_f_yyyy,
    cy_row.prev_cy,
    leap_year_fy.leap_year_fy_ind,
    leap_year_cy.leap_year_cy_ind
from
    stage
    inner join fy_row
        on stage.fy = fy_row.fy
    inner join cy_row
        on stage.cy = cy_row.cy
    inner join leap_year_cy
        on stage.cy = leap_year_cy.cy
    inner join leap_year_fy
        on stage.fy = leap_year_fy.fy
