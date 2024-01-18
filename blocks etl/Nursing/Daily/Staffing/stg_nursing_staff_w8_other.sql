/* stg_nursing_staff_w8_other
Aggregates other staffing metrics: LOA (Leave of Absence) FTE data from position control,
Open Requistition data from Workday, and Time to Fill (in days) from Workday
*/

select /* Leave of Absense (LOA) */
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from {{ ref('stg_nursing_staff_p8a_loa') }}

union all

select /* Open Requisitions */
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from {{ ref('stg_nursing_staff_p8b_open_requisition') }}

union all

select /* Time to Fill */
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from {{ ref('stg_nursing_staff_p8c_time_to_fill') }}
