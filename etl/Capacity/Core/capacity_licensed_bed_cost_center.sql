select
    department_cost_center_xref.workday_cost_cntr_key,
    department_cost_center_xref.workday_cost_center_id,
    capacity_licensed_bed_department.bed_count_date_key,
    department_cost_center_xref.clarity_cost_cntr_key,
    department_cost_center_xref.clarity_cost_center_id,
    capacity_licensed_bed_department.bed_count_date,
    sum(capacity_licensed_bed_department.licensed_bed_count) as licensed_bed_count,
    cost_center.cost_cntr_cd || ' - ' || cost_center.cost_cntr_nm as cost_center_display
from
    {{ref('capacity_licensed_bed_department')}} as capacity_licensed_bed_department
inner join {{ref('department_cost_center_xref')}} as department_cost_center_xref
    on capacity_licensed_bed_department.department_id  = department_cost_center_xref.department_id
    and capacity_licensed_bed_department.bed_count_date_key = department_cost_center_xref.align_dt_key
inner join {{source('cdw','cost_center')}} as cost_center
    on department_cost_center_xref.workday_cost_cntr_key = cost_center.cost_cntr_key
    and cost_center.comp_key = 0
group by
    department_cost_center_xref.clarity_cost_cntr_key,
    department_cost_center_xref.workday_cost_cntr_key,
    department_cost_center_xref.workday_cost_center_id,
    department_cost_center_xref.clarity_cost_center_id,
    capacity_licensed_bed_department.bed_count_date_key,
    capacity_licensed_bed_department.bed_count_date,
    cost_center.cost_cntr_cd,
    cost_center.cost_cntr_nm
