with
    stage as (--region
    select
        c_mm,
        c_yyyy,
        c_yy as cy,
        f_mm,
        f_yyyy,
        f_yy as fy,
        c_yy || '-Q' || c_qtr as cy_qtr,
        to_date(c_yyyy || '-01-01', 'yyyy-mm-dd') as cy_year_start,
        to_date(c_yyyy || '-12-31', 'yyyy-mm-dd') as cy_year_end,
        to_date(f_yyyy || '-07-01', 'yyyy-mm-dd') as fy_year_start,
        to_date(f_yyyy + 1 || '-06-30', 'yyyy-mm-dd') as fy_year_end,
        case --used to remove months that are not up to date
            when day(current_date) < 15 then -2 else -1 end as time_cap,
        date_trunc('month', full_dt) as visual_month,
        --visual month is inclusinve, so this will be used to pull in utilization through the visual month
        add_months(date_trunc('month', full_dt), 1) - 1 as visual_month_end,
        case
            when c_mm in (7, 8, 9)
            then f_yy || '-Q1'
            when c_mm in (10, 11, 12)
            then f_yy || '-Q2'
            when c_mm in (1, 2, 3)
            then f_yy || '-Q3'
            when c_mm in (4, 5, 6)
            then f_yy || '-Q4'
            else 'CHECK'
        end as fy_qtr

    from {{source('cdw', 'master_date')}}

    where
        full_dt >= to_date('2015-01-01', 'yyyy-mm-dd')
        and full_dt <= date_trunc('month', current_date)

    group by
        date_trunc('month', full_dt),
        c_mm,
        c_yyyy,
        c_yy,
        f_mm,
        f_yyyy,
        f_yy,
        date_trunc('year', full_dt),
        fy_qtr,
        cy_qtr

    order by
        date_trunc('month', full_dt)

    --end region
),

fy_qtr_row as (--region
    select
        fy,
        fy_qtr,
        lag(fy_qtr) over(order by fy_qtr asc) as prev_fy_qtr,
        row_number() over(partition by fy order by fy_qtr desc) as fy_qtr_seq_num

    from stage

    group by
        fy,
        fy_qtr

    --end region
),

cy_qtr_row as (--region
    select
        cy,
        cy_qtr,
        lag(cy_qtr) over(order by cy_qtr asc) as prev_cy_qtr,
        row_number() over(partition by cy order by cy_qtr desc) as cy_qtr_seq_num

    from stage

    group by
        cy,
        cy_qtr

    --end region
),

fy_row as (--region
    select
        fy,
        lag(fy) over(order by fy asc) as prev_fy,
        lag(f_yyyy) over(order by f_yyyy) as prev_f_yyyy,
        row_number() over(order by fy desc) as fy_seq_num

    from stage

    group by
        fy,
        f_yyyy

    --end region
),

cy_row as (--region
    select
        cy,
        lag(cy) over(order by cy asc) as prev_cy,
        row_number() over(order by cy desc) as cy_seq_num

    from stage

    group by cy

    --end region
),

combine as (--region
    select
        stage.*,
        --, sum((days_between(stage.visual_month_end, stage.visual_month)+1)) as day_count_sum
        qtr.prev_fy_qtr,
        fy.prev_fy,
        fy.prev_f_yyyy,
        qtr.fy_qtr_seq_num,
        fy.fy_seq_num,
        cqtr.prev_cy_qtr,
        cy.prev_cy,
        cqtr.cy_qtr_seq_num,
        cy.cy_seq_num,
        row_number() over(partition by stage.fy order by stage.visual_month desc) as fy_month_seq_num,
        row_number() over(partition by stage.cy order by stage.visual_month desc) as cy_month_seq_num,
        (days_between(stage.visual_month_end, stage.visual_month) + 1) as day_count,
        to_char(stage.visual_month, 'MM/DD/YYYY') as post_date_month_year

    from stage
        inner join fy_qtr_row as qtr on stage.fy = qtr.fy and stage.fy_qtr = qtr.fy_qtr
        inner join fy_row as fy on stage.fy = fy.fy
        inner join cy_qtr_row as cqtr on stage.cy = cqtr.cy and stage.cy_qtr = cqtr.cy_qtr
        inner join cy_row as cy on stage.cy = cy.cy
    --where stage.visual_month <= add_months(current_date, time_cap)
    --group by stage.fy
    --end region
),

day_count_cumulative as (--region
    select *,
        sum(day_count) over(partition by fy order by visual_month asc rows unbounded preceding) as cum_sum

    from combine
    --end region
),

day_count_max as (--region
    select *,
        max(cum_sum) over(partition by fy order by fy asc) as cum_sum_max

    from day_count_cumulative
    --end region
)
select * from day_count_max order by visual_month desc
--;
