with bed_charge_table as (
    /* table has one row per charge type but we only
    care about the alignment and date */
    select
        bed_charge_table.contact_date,
        bed_charge_table.department_id,
        bed_charge_table.cost_center_id
    from
        {{source('ods','bed_charge_table')}} as bed_charge_table
        inner join {{source('cdw','procedure')}} as procedure --noqa: L029
            on bed_charge_table.proc_id = procedure.proc_id
    where
        proc_nm not in
        (
           'SDU-POSTPARTUM (ROUTINE)',
           'RM CHG OBSERVATION PER HR'
        )
        and bill_cat = 'ROOM & BOARD'
        -- this may not be required but ensure other procedures do not cause ambiguity
        and proc_cat = 'HB ROOM & BOARD'
        and bed_charge_table.table_id = 100
    group by
        bed_charge_table.contact_date,
        bed_charge_table.department_id,
        bed_charge_table.cost_center_id
),

group_alignments as (
    select
        contact_date,
        department_id,
        cost_center_id,
        -- if department reverts to prevous alignment it will still create a new group
        row_number() over (partition by department_id order by contact_date)
        - row_number() over (partition by department_id, cost_center_id order by contact_date) as grp
    from
        bed_charge_table
),

get_date_range as (
    select
        min(contact_date) as effective_date,
        department_id,
        cost_center_id
    from
        group_alignments
    group by
        department_id,
        cost_center_id,
        grp
),

clarity_info as (
    select
        get_date_range.effective_date,
        lead(get_date_range.effective_date) over (
                partition by get_date_range.department_id
                order by get_date_range.effective_date
        ) as next_align_date,
        get_date_range.department_id,
        get_date_range.cost_center_id as clarity_cost_center_id,
        cost_center.cost_cntr_cd as cost_center_cd,
        cost_center.cost_cntr_key as clarity_cost_cntr_key,
        cost_center.gl_comp as epiccc_gl_comp -- will use to get to Workday cost center key
    from
        get_date_range
        inner join {{source('cdw','cost_center')}} as cost_center
            on get_date_range.cost_center_id = cost_center.cost_cntr_id
    where
        cost_center.gl_comp is not null
),

workday_info as (
    select
        department.dept_abbr,
        department.dept_nm,
        department.dept_key,
        case
            when clarity_info.effective_date < date('2020-01-01')
            then date('2020-01-01')
            else clarity_info.effective_date
        end as effective_date,
        coalesce(clarity_info.next_align_date,
        current_date + 1) - 1 as end_date,
        clarity_info.department_id,
        clarity_info.clarity_cost_center_id,
        clarity_info.clarity_cost_cntr_key,
        case
            -- parse out the Cost Center Site number as the digits after the first 
            -- decimal point of the Clarity cost center record
            when -- if there are two periods in the string, get the text in between them
                instr( clarity_info.cost_center_cd, '.', 1, 2) > 0
            then
                substring(clarity_info.cost_center_cd, instr( clarity_info.cost_center_cd, '.') + 1,
                instr( clarity_info.cost_center_cd, '.', 1, 2) - instr( clarity_info.cost_center_cd, '.') - 1)
            else
                substring(clarity_info.cost_center_cd, instr( clarity_info.cost_center_cd, '.') + 1 )
        end as cost_center_site_num,
        workday_cost_center.cost_cntr_key as workday_cost_cntr_key,
        workday_cost_center.cost_cntr_id as workday_cost_center_id,
        workday_cost_center.cost_cntr_nm
    from
        clarity_info
        left join {{source('cdw','department')}} as department
            on clarity_info.department_id = department.dept_id
        left join {{source('workday','workday_cost_center')}} as workday_cost_center
            on epiccc_gl_comp = workday_cost_center.cost_cntr_cd
)

select
    dept_key,
    master_date.dt_key as align_dt_key,
    master_date.full_dt as department_align_date,
    workday_cost_cntr_key,
    clarity_cost_cntr_key,
    department_id,
    workday_cost_center_id,
    clarity_cost_center_id,
    workday_cost_center_site.cost_cntr_site_id as cost_center_site_id,
    workday_cost_center_site.cost_cntr_site_key as cost_center_site_key
from
    workday_info
    inner join {{source('cdw','master_date')}} as master_date
        on master_date.full_dt
            between workday_info.effective_date and workday_info.end_date
    left join {{source('workday','workday_cost_center_site')}} as workday_cost_center_site
        on substring(workday_cost_center_site.cost_cntr_site_id,
            instr(workday_cost_center_site.cost_cntr_site_id, '_') + 1) = workday_info.cost_center_site_num
where
    -- workeday go live
    workday_info.end_date >= date('2020-01-01')
