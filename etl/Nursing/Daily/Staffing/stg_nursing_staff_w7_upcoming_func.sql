/* stg_nursing_staff_w7_upcoming_func
create functional FTE, vacancy FTE, and vacancy rate
for the third next upcoming pay period
*/

select
    'FunctionalFTEnext3PP' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    func_fte_next_3_pp as numerator,
    null::numeric as denominator,
    func_fte_next_3_pp as row_metric_calculation
from
    {{ ref('stg_nursing_staff_p6_vacancy_component') }}

union all

select
    'FuncVacancyFTEnext3PP' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    func_vacancy_fte_next_3_pp as numerator,
    null::numeric as denominator,
    func_vacancy_fte_next_3_pp as row_metric_calculation
from
    {{ ref('stg_nursing_staff_p6_vacancy_component') }}

union all

select
    'FuncVacancyRateNext3PP' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    func_vacancy_fte_next_3_pp as numerator,
    budget as denominator,
    func_vac_rate_next_3_pp as row_metric_calculation
from
    {{ ref('stg_nursing_staff_p6_vacancy_component') }}

union all

select
    'UpcomingFTEnext3PP' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    upcoming_fte_next_3_pp as numerator,
    null::numeric as denominator,
    upcoming_fte_next_3_pp as row_metric_calculation
from
    {{ ref('stg_nursing_staff_p6_vacancy_component') }}

union all

select
    'UpcomingVacancyFTEnext3PP' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    upcoming_vacancy_fte_next_3_pp as numerator,
    null::numeric as denominator,
    upcoming_vacancy_fte_next_3_pp as row_metric_calculation
from
    {{ ref('stg_nursing_staff_p6_vacancy_component') }}

union all

select
    'UpcomingVacancyRateNext3PP' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    upcoming_vacancy_fte_next_3_pp as numerator,
    budget as denominator,
    upcoming_vac_rate_next_3_pp as row_metric_calculation
from
    {{ ref('stg_nursing_staff_p6_vacancy_component') }}
