{{
    config(materialized = 'view')
}}

with stage as (
    select
       fact_department_rollup.mstr_dept_grp_chop_key as mstr_dept_grp_key,
        capacity_licensed_bed_department.bed_count_date_key as bed_cnt_dt_key,
        sum(capacity_licensed_bed_department.licensed_bed_count) as dept_grp_tot_in_use_licensed_bed_cnt,
        min(fact_department_rollup.loc_acute_inpatient_ind) as acute_inpatient_ind,
        min(fact_department_rollup.loc_pediatric_ind) as pediatric_ind

    from
        {{source('chop_analytics','capacity_licensed_bed_department')}} as capacity_licensed_bed_department
        inner join {{ref('fact_department_rollup')}} as fact_department_rollup
            on fact_department_rollup.dept_key = capacity_licensed_bed_department.dept_key
            and fact_department_rollup.dept_align_dt_key = capacity_licensed_bed_department.bed_count_date_key
    group by
        fact_department_rollup.mstr_dept_grp_chop_key,
        capacity_licensed_bed_department.bed_count_date_key
),

cntsallscenarios as (
    select
        bed_cnt_dt_key,
        sum(dept_grp_tot_in_use_licensed_bed_cnt) as chop_tot_in_use_licensed_bed_cnt,
        acute_inpatient_ind,
        pediatric_ind
    from stage
    group by
            bed_cnt_dt_key,
            acute_inpatient_ind,
            pediatric_ind
)

select
    bed_cnt_dt_key,
    cast(chop_tot_in_use_licensed_bed_cnt as int) as chop_tot_in_use_licensed_bed_cnt,
    now() as create_dt,
    cast('FACTCENSUS' as varchar(20)) as create_by --noqa: 
from
    cntsallscenarios
where
    acute_inpatient_ind = 1
    and pediatric_ind = 1
    /*  for this table we want only the acute inpatieint pediatric total    */
