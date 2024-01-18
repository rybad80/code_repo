select
    'HI: Outpatients (Unique)' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_hi_op_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_hi_encounter_cohort')}}
where
    (visit_type_id in (
                            1301, -- 'follow up established',
                            1351, -- 'follow up estab',
                            2207, -- 'follow up hypoglycemia',
                            1978, -- 'hi fol',
                            1977, -- 'hi new',
                            2088, -- 'video visit new',
                            2124, -- 'video visit follow up',
                            2152, -- 'telephone visit',
                            2206, -- 'new hypoglycemia',
                            2553  -- 'new patient endo'
                            )
        or encounter_type_id in (76 --, --'telemedicine'
                                        )
    )
    and department_id in (
                        101012128, -- 'bgr endocrinology',
                        101012173, -- 'bgr hifp multid cln',
                        80245010,  -- 'ext endocrinology',
                        84248010,  -- 'kop endocrinology',
                        101022011, -- 'virtua endocrinology',
                        82253010   -- 'vnj endocrinology'
                        )
