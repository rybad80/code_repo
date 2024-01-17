with all_labs as (
    select
        procedure_order.pat_key,
        master_date.full_dt as measure_date,
        case
            when (comp_nm like '%A1C%' or base_nm in ('A1C', 'A1CQ', 'A1CLC')
                or common_nm = 'HEMOGLOBIN A1C')
            then 'HbA1c'
            when (base_nm in ('LDL', 'LDLQ', 'LDLLC', 'LDLCLC')
                or common_nm = 'DIRECT LOW DENSITY LIPOPROT CHOL-1001090-LGH')
            then 'LDL'
            when (comp_nm like '%TSH%' or comp_nm like '%THYROID STIM%')
            then 'TSH'
            when (comp_nm like 'MICRO%ALB%' or upper(comp_nm) like 'UR ALBUMIN%')
            then 'Microalbumin'
            when (comp_nm like 'TRIGLYCERIDE%') then 'Triglyceride'
            else comp_nm
        end as measure_name,
        cast(replace(replace(trim(upper(rslt_val)), '>', ''), '<', '') as numeric) as result,
        row_number() over (
            partition by
                procedure_order.pat_key,
                measure_name
            order by
                master_date.full_dt desc
        ) as most_recent
    from
        {{source('cdw', 'procedure_order') }} as procedure_order
        inner join {{source('cdw', 'procedure_order_result') }} as procedure_order_result
            on procedure_order_result.proc_ord_key = procedure_order.proc_ord_key
        inner join {{source('cdw', 'master_date') }} as master_date
            on master_date.dt_key = procedure_order_result.ord_rslt_dt_key
        left join {{source('cdw', 'result_component') }} as result_component
            on result_component.rslt_comp_key = procedure_order_result.rslt_comp_key
    where
        (base_nm in ('A1C', 'A1CQ', 'A1CLC', 'LDL', 'LDLQ', 'LDLLC', 'LDLCLC')
            or common_nm in ('HEMOGLOBIN A1C', 'DIRECT LOW DENSITY LIPOPROT CHOL-1001090-LGH')
            or comp_nm like '%A1C%'
            or comp_nm like '%TSH%'
            or comp_nm like '%THYROID STIM%'
            or comp_nm like 'MICRO%ALB%'
            or comp_nm like 'TRIGLYCERIDE%'
            or upper(comp_nm) like 'UR ALBUMIN%'
        )
        and rslt_num_val is not null
        -- Brian wrote this, I think it removes text results
        and length(trim(translate(replace(replace(trim(
            upper(rslt_val)), '>', ''), '<', ''), ' +-.0123456789', ' '))) = 0
        and year(master_date.full_dt) between year(current_date - interval('4 year')) and year(current_date)
        --2020 and 2024 -- Need 3 years of data + 3 months prior
)

select
    all_labs.pat_key,
    max(case
        when (all_labs.measure_date between (current_date - interval('2 year')) and current_date)
            and all_labs.measure_name = 'TSH'
        then 1 else 0
    end) as tsh_2_yr,
    max(case
        when (all_labs.measure_date between (current_date - interval('2 year')) and current_date)
            and all_labs.measure_name = 'TSH'
        then all_labs.measure_date
    end) as tsh_date,
    max(case
        when (all_labs.measure_date between (current_date - interval('3 year')) and current_date)
            and all_labs.measure_name = 'LDL'
        then 1 else 0
    end) as lipid_3_yr,
    max(case
        when (all_labs.measure_date between (current_date - interval('3 year')) and current_date)
            and all_labs.measure_name = 'LDL'
        then all_labs.measure_date
    end) as lipid_date,
    max(case
        when (all_labs.measure_date between (current_date - interval('1 year')) and current_date)
            and all_labs.measure_name = 'LDL'
        then 1 else 0
    end) as lipid_1_yr,
    max(case
        when all_labs.most_recent = 1 and all_labs.measure_name = 'LDL'
            and all_labs.result <= 130
        then 1 else 0
    end) as lipid_under_130_most_recent,
    max(case
        when (all_labs.measure_date between (current_date - interval('1 year')) and current_date)
            and all_labs.measure_name = 'Microalbumin'
        then 1 else 0
    end) as microa_1_yr,
    max(case
        when (all_labs.measure_date between (current_date - interval('1 year')) and current_date)
            and all_labs.measure_name = 'Microalbumin'
        then all_labs.measure_date
    end) as microa_date,
    max(case
        when all_labs.most_recent = 1 and all_labs.measure_name = 'Triglyceride'
            and all_labs.result < 150
        then 1 else 0
    end) as tg_under_150_most_recent
from
    all_labs
group by
    all_labs.pat_key
