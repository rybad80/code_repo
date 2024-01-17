with t1_flow_3_years as ( --region 'T1DM': flowsheet report for type1 
    select
        flowsheet_all.patient_key,
        cast('1840-12-31' as date) + f2.meas_val_num as screening_date,
        max(case
            when (screening_date between (current_date - interval('2 year')) and current_date)
                and flowsheet_all.flowsheet_id = 10060262
            then 1 else 0
        end) as tsh_2_yr,
        max(case
            when (screening_date between (current_date - interval('2 year')) and current_date)
                and flowsheet_all.flowsheet_id = 10060262
            then screening_date
        end) as tsh_date,
        max(case
            when (screening_date between (current_date - interval('3 year')) and current_date)
                and flowsheet_all.flowsheet_id = 10678
            then 1 else 0
        end) as lipid_3_yr,
        max(case
            when (screening_date between (current_date - interval('3 year')) and current_date)
                and flowsheet_all.flowsheet_id = 10678
            then screening_date
        end) as lipid_date,
        max(case
            when (screening_date between (current_date - interval('1 year')) and current_date)
                and flowsheet_all.flowsheet_id = 10678
            then 1 else 0
        end) as lipid_1_yr,
        max(case
            when (screening_date between (current_date - interval('1 year')) and current_date)
                and ((flowsheet_all.meas_val in (
                    'Yes', 'No', 'Normal Screen', 'Abnormal Screen', 'Microalbuminuria Dx')
                    and flowsheet_all.flowsheet_id = 10060264) --Microalbuminuria Screening
                or flowsheet_all.flowsheet_id = 17305)	--Microalbuminuria Screen Comments
            then 1 else 0
        end) as microa_1_yr,
        max(case
            when (screening_date between (current_date - interval('1 year')) and current_date)
                and ((flowsheet_all.meas_val in (
                    'Yes', 'No', 'Normal Screen', 'Abnormal Screen', 'Microalbuminuria Dx')
                    and flowsheet_all.flowsheet_id = 10060264) --Microalbuminuria Screening
                or flowsheet_all.flowsheet_id = 17305)	--Microalbuminuria Screen Comments
            then screening_date
        end) as microa_date
    from
        {{ ref('flowsheet_all') }} as flowsheet_all
        inner join {{ ref('flowsheet_all') }} as f2
            on f2.patient_key = flowsheet_all.patient_key
                and f2.recorded_date = flowsheet_all.recorded_date
                and f2.flowsheet_id = 10060256 -- Last Screening Labs
                and f2.meas_val is not null
    where
        flowsheet_all.flowsheet_id in (
            '10060262', --'Thyroid Screen'
            '10678', --'Lipid Panel'
            '10060264', --Microalbuminuria Screening
            '17305' --Microalbuminuria Screen Comments
        )
        and flowsheet_all.meas_val is not null
        and lower(flowsheet_all.meas_val) not like '%deferred%'
        and lower(flowsheet_all.meas_val) not like '%not%done%'
        and lower(flowsheet_all.meas_val) not like '%no%baseline%'
    group by
        flowsheet_all.patient_key,
        f2.meas_val_num
),

type1_labs as (
    select
        t1_flow_3_years.patient_key,
        max(t1_flow_3_years.tsh_2_yr) as tsh_2_yr,
        max(t1_flow_3_years.tsh_date) as tsh_date,
        max(t1_flow_3_years.lipid_3_yr) as lipid_3_yr,
        max(t1_flow_3_years.lipid_1_yr) as lipid_1_yr,
        max(t1_flow_3_years.lipid_date) as lipid_date,
        max(t1_flow_3_years.microa_1_yr) as microa_1_yr,
        max(t1_flow_3_years.microa_date) as microa_date
    from
        t1_flow_3_years
    group by
        t1_flow_3_years.patient_key
--end region
),

t2_flow_3_years as ( --region 'T2-1DM': flowsheet report FOR type2
    select
        flowsheet_all.patient_key,
        cast('1840-12-31' as date) + f2.meas_val_num as screening_date,
        max(case
            when (screening_date between (current_date - interval('3 year')) and current_date)
                and flowsheet_all.flowsheet_id in (9416, 9422, 9426)
        then 1 else 0
        end) as lipid_3_yr,
        max(case
            when (screening_date between (current_date - interval('3 year')) and current_date)
                and flowsheet_all.flowsheet_id in (9416, 9422, 9426)
            then screening_date
        end) as lipid_date,
        max(case
            when (screening_date between (current_date - interval('1 year')) and current_date)
                and flowsheet_all.flowsheet_id in (9416, 9422, 9426)
            then 1 else 0
        end) as lipid_1_yr,
        max(case
            when (screening_date between (current_date - interval('1 year')) and current_date)
                and flowsheet_all.flowsheet_id in (
                    9427, --Microalbuminuria Screening
					9429 --Microalbuminuria Screen Comments
                )
			then 1 else 0
        end) as microa_1_yr,
        max(case
            when (screening_date between (current_date - interval('1 year')) and current_date)
                and flowsheet_all.flowsheet_id in (
                    9427, --Microalbuminuria Screening
                    9429 --Microalbuminuria Screen Comments
                )
			then screening_date
        end) as microa_date
    from
        {{ref('flowsheet_all') }} as flowsheet_all
        inner join {{ref('flowsheet_all') }} as f2
            on f2.patient_key = flowsheet_all.patient_key
                and f2.recorded_date = flowsheet_all.recorded_date
                and f2.flowsheet_id = 9391 --Last Screening Labs
                and f2.meas_val is not null
    where
        flowsheet_all.flowsheet_id in (
            '9426', --Lipid Panel Screen Comments T2DM
            '9416', --Lipid Panel Screening T2DM
            '9622', -- Current Treatment Lipid Panel Screening T2DM
            '9429', --Microalbuminuria Screen Comments
            '9427' --Microalbuminuria Screening
        )
        and flowsheet_all.meas_val is not null
        and flowsheet_all.meas_val not like '%no%baseline%'
group by
    flowsheet_all.patient_key,
    f2.meas_val_num
),

type2_labs as (
    select
        t2_flow_3_years.patient_key,
        max(t2_flow_3_years.lipid_3_yr) as lipid_3_yr,
        max(t2_flow_3_years.lipid_1_yr) as lipid_1_yr,
        max(t2_flow_3_years.lipid_date) as lipid_date,
        max(t2_flow_3_years.microa_1_yr) as microa_1_yr,
        max(t2_flow_3_years.microa_date) as microa_date
    from
        t2_flow_3_years
    group by
        t2_flow_3_years.patient_key
),

retinopathy_dates as (
    select
        flowsheet_all.patient_key,
        flowsheet_all.encounter_date,
        max(case
            when flowsheet_all.flowsheet_id in ('15792', '9442')
            then cast('1840-12-31' as date) + flowsheet_all.meas_val_num
        end) as retinopathy_screen_date,
        max(case
            when flowsheet_all.flowsheet_id in ('10060267', '9443')
            then flowsheet_all.meas_val
        end) as retinopathy_screen_value,
        coalesce(retinopathy_screen_date, flowsheet_all.encounter_date) as retinopathy_date, --noqa: L028
        case
            when (retinopathy_date between (current_date - interval('2 year')) and current_date) --noqa: L028
            then 1 else 0 end
        as retinopathy_screen_2_yr
    from
        {{ref('flowsheet_all') }} as flowsheet_all
    where
        flowsheet_all.flowsheet_id in (
            '15792', --'Last Retinopathy Screen' Date
            '10060267', --Retinopathy Screen
            '9443', --Diabetic --Retinopathy Screening
            '9442' --Last Retinopathy Screen
        )
        and flowsheet_all.meas_val is not null
        and flowsheet_all.meas_val != 'Deferred'
        and flowsheet_all.meas_val != 'No baseline'
        and flowsheet_all.meas_val != 'Screening Deferred Due to Age'
        and year(flowsheet_all.recorded_date) between 2021 and 2024 -- 2 years +3 months pre and Jan 2022
    group by
        flowsheet_all.patient_key,
        flowsheet_all.encounter_date
),

retinopathy as ( --region
    select
        retinopathy_dates.patient_key,
        max(retinopathy_dates.retinopathy_screen_2_yr) as retinopathy_screen_2_yr,
        max(retinopathy_dates.retinopathy_date) as retinopathy_date
    from
        retinopathy_dates
    group by
        retinopathy_dates.patient_key
),

flow_eye as (
    select
        flowsheet_all.patient_key,
        1 as flow_eye_ind,
        max(case
            when flowsheet_all.flowsheet_id = '10060569'
                then to_date(flowsheet_all.meas_val, 'MM/DD/YY')
        end) as real_flow_eye_date,
        max(case
            when flowsheet_all.flowsheet_id = '10060569'
                then flowsheet_all.meas_val
        end) as orig_real_flow_eye_date,
        max(case
            when flowsheet_all.flowsheet_id = '15791'
                then flowsheet_all.recorded_date
        end) as req_flow_eye_date
    from
        {{ref('flowsheet_all') }} as flowsheet_all
    where
        ((flowsheet_all.flowsheet_id = '10060569' -- Annual Dilated Eye Exam Due
            and (regexp_like(flowsheet_all.meas_val, '\d\d?/\d\d?/(2021|21)(\D|\Z)')
                or regexp_like(flowsheet_all.meas_val, '\d\d?/\d\d?/(2020|20)(\D|\Z)')
                or regexp_like(flowsheet_all.meas_val, '1/\d\d?/(2022|22)(\D|\Z)'))
            )
        or (flowsheet_all.flowsheet_id = '15791' --Dilated Eye Exam
            and flowsheet_all.meas_val is not null
            --include: 'Scheduled', 'Baseline screening requested',
            --'Up to date', 'Update screening requested', 'Requested'
            and flowsheet_all.meas_val != 'Deferred'
            and year(flowsheet_all.recorded_date)
                between year(current_date - interval('2 year')) and year(current_date)
            )
        )
 group by
   flowsheet_all.patient_key
)

select
    stg_usnwr_diabetes_primary_pop.primary_key as patient_key,
    type1_labs.tsh_2_yr as type1_tsh_2_yr,
    type1_labs.tsh_date as type1_tsh_date,
    type1_labs.lipid_3_yr as type1_lipid_3_yr,
    type1_labs.lipid_1_yr as type1_lipid_1_yr,
    type1_labs.lipid_date as type1_lipid_date,
    type1_labs.microa_1_yr as type1_microa_1_yr,
    type1_labs.microa_date as type1_microa_date,
    type2_labs.lipid_3_yr as type2_lipid_3_yr,
    type2_labs.lipid_1_yr as type2_lipid_1_yr,
    type2_labs.lipid_date as type2_lipid_date,
    type2_labs.microa_1_yr as type2_microa_1_yr,
    type2_labs.microa_date as type2_microa_date,
    retinopathy.retinopathy_screen_2_yr,
    retinopathy.retinopathy_date,
    flow_eye.flow_eye_ind,
    flow_eye.real_flow_eye_date,
    flow_eye.orig_real_flow_eye_date,
    flow_eye.req_flow_eye_date
from
    {{ref('stg_usnwr_diabetes_primary_pop') }} as stg_usnwr_diabetes_primary_pop
    left join type1_labs
        on stg_usnwr_diabetes_primary_pop.primary_key = type1_labs.patient_key
    left join type2_labs
        on stg_usnwr_diabetes_primary_pop.primary_key = type2_labs.patient_key
    left join retinopathy
        on stg_usnwr_diabetes_primary_pop.primary_key = retinopathy.patient_key
    left join flow_eye
        on stg_usnwr_diabetes_primary_pop.primary_key = flow_eye.patient_key
