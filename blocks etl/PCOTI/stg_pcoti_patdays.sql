with cost_center_pat_days as (
    select
        date_trunc('month', post_dt) as post_month,
        case
            -- non-ICU cost centers
            when cost_cntr_id = 10065 then -6877076916237443851 -- Adolescent and Gen Peds
            when cost_cntr_id = 10040 then -7420072304850955129 -- CSH Adolescent
            when cost_cntr_id = 10070 then -8378759699632897988 -- Complex Care Gen Peds
            when cost_cntr_id = 10030 then -5356685225857295528 -- GI - Endocrine
            when cost_cntr_id = 10050 then 7023405050661503395  -- General Pediatrics Med
            when cost_cntr_id = 10025 then -1039342137531550071 -- Infant Transitional Care Unit
            when cost_cntr_id = 10150 then 6578354093970604091  -- Inpatient Overflow
            when cost_cntr_id = 10525 then 2387921749509832077  -- Inpatient Overflow PACU
            when cost_cntr_id = 30190 then -6593177000888956837 -- KOP Medical Surgical Unit
            when cost_cntr_id = 10080 then 1608782767657931262  -- Medical Behavioral Unit
            when cost_cntr_id = 10005 then 759370677810954410   -- Medical Hospitalist Care Unit
            when cost_cntr_id = 10060 then 3260505321811293605  -- Neurology Gen Peds
            when cost_cntr_id = 10320 then 8407581346771630907  -- Observation Unit
            when cost_cntr_id = 30194 then 8407581346771630907  -- General Medical Unit
            when cost_cntr_id = 10181 then 8453519825503205068  -- Oncology BMT - East
            when cost_cntr_id = 10180 then 8453519825503205068  -- Oncology BMT - South
            when cost_cntr_id = 10010 then -775155838848206489  -- Pulmonary Gen Peds
            when cost_cntr_id = 10175 then -3078096573426016267 -- Rehab Nursing Unit
            when cost_cntr_id = 10140 then -941018618977266235  -- Rheumatology Gen Peds
            when cost_cntr_id = 10201 then 3003399749188172140  -- Surgery East
            when cost_cntr_id = 10200 then 3003399749188172140  -- Surgery South
            when cost_cntr_id = 30192 then 8453519825503205068  -- KOP PICU/Oncology Unit
            -- ICU cost centers
            when cost_cntr_id = 10252 then 8749486432628814115  -- NICU C
            when cost_cntr_id = 10250 then 8749486432628814115  -- NICU East
            when cost_cntr_id = 10251 then 8749486432628814115  -- NICU NE
            when cost_cntr_id = 10253 then 8749486432628814115  -- NICU West 1
            when cost_cntr_id = 10254 then 8749486432628814115  -- NICU West 2
            when cost_cntr_id = 10230 then -324401301796978398  -- PICU Main
            when cost_cntr_id = 10231 then -324401301796978398  -- PICU South
            when cost_cntr_id = 10233 then -324401301796978398  -- PICU West
            when cost_cntr_id = 10220 then 7632797884958572007  -- CICU
            when cost_cntr_id = 10020 then -2228486975980756796 -- Cardiac Care Unit
            when cost_cntr_id = 10090 then 5678845503956789073  -- Progressive Care Unit
            when cost_cntr_id = 10270 then -3423943552329491723 -- Special Delivery Unit
            when cost_cntr_id = 10565 then -139962965026118705  -- Cardiac Prep and Recovery Unit Overflow
        end as mstr_dept_grp_unit_key,
        case
            when cost_cntr_id in (
                10252,
                10250,
                10251,
                10253,
                10254,
                10230,
                10231,
                10233,
                10220,
                10020,
                10090,
                10270,
                10565
            ) then 1
            else 0
        end as icu_ind,
        case
            when cost_cntr_id in (
                30190,
                30192
            ) then 'KOPH'
            else 'PHL'
        end as campus_name,
        chrg_qty as patdays
    from
        {{ source('cdw', 'fact_financial_statistic_finance_pat_days') }}
    where
        post_dt >= '2017-01-01'
        and stats_cd in (32, 14) -- IP days and observation days
),

unit_rollup as (
    select
        mstr_dept_grp_unit_key,
        unit_dept_grp_abbr
    from
        {{ source('cdw_analytics', 'fact_department_rollup_summary')}}
    group by
        mstr_dept_grp_unit_key,
        unit_dept_grp_abbr
)

select
    cost_center_pat_days.post_month,
    cost_center_pat_days.icu_ind,
    cost_center_pat_days.mstr_dept_grp_unit_key,
    cost_center_pat_days.campus_name,
    coalesce(unit_rollup.unit_dept_grp_abbr, 'OTHER') as department_group_name,
    sum(cost_center_pat_days.patdays) as patdays
from
    cost_center_pat_days
    left join unit_rollup
        on cost_center_pat_days.mstr_dept_grp_unit_key = unit_rollup.mstr_dept_grp_unit_key
group by
    cost_center_pat_days.post_month,
    cost_center_pat_days.icu_ind,
    cost_center_pat_days.mstr_dept_grp_unit_key,
    cost_center_pat_days.campus_name,
    coalesce(unit_rollup.unit_dept_grp_abbr, 'OTHER')
