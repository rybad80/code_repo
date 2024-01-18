select distinct
    stg_usnwr_diabetes_primary_pop.submission_year,
    stg_usnwr_diabetes_primary_pop.primary_key as patient_key,
    stg_usnwr_diabetes_primary_pop.patient_name,
    stg_usnwr_diabetes_primary_pop.mrn,
    stg_usnwr_diabetes_primary_pop.dob,
    stg_usnwr_diabetes_primary_pop.diabetes_type_12,
    stg_usnwr_diabetes_primary_pop.metric_date,
    case
        when stg_usnwr_diabetes_primary_pop.dx_duration_year >= '5'
        then 1 else 0
    end as diabetes_5_years,
    case
        when stg_usnwr_diabetes_primary_pop.current_age >= '11'
        then 1 else 0
    end as over_11,
    stg_usnwr_diabetes_scans.scan_labs,
    -- TSH
    case
        when
            ((stg_usnwr_diabetes_scans.tsh_scan_date
                between current_date - interval('2 year') and current_date)
                and stg_usnwr_diabetes_scans.tsh_scans = 1)
        then stg_usnwr_diabetes_scans.tsh_scans
        when stg_usnwr_diabetes_sde_labs.tsh_2_yr_ind = 1
            or stg_usnwr_diabetes_sde_labs.annual_labs_2yr_ind = 1
        then 1 else 0
    end as tsh_2_yr,
    greatest(coalesce(stg_usnwr_diabetes_scans.tsh_scan_date, date('1999-01-01')),
            coalesce(stg_usnwr_diabetes_sde_labs.labs_last_screened_date, date('1999-01-01')),
            coalesce(stg_usnwr_diabetes_sde_labs.tsh_date, date('1999-01-01')),
            coalesce(stg_usnwr_diabetes_flowsheet_labs_hx.type1_tsh_date, date('1999-01-01'))
    ) as tsh_date_final,
    --LIPID
    greatest(coalesce(stg_usnwr_diabetes_sde_labs.labs_last_screened_date, date('1999-01-01')),
        coalesce(cast(stg_usnwr_diabetes_scans.lipid_scan_date as date), date('1999-01-01')),
        --coalesce(stg_usnwr_diabetes_labs_scans.lipid_date, date('1999-01-01')),
        coalesce(stg_usnwr_diabetes_sde_labs.lipid_result_date, date('1999-01-01'))
    ) as lipid_final_date,
    stg_usnwr_diabetes_sde_labs.lipid_result_date,
    case
        when lipid_final_date
            between (current_date - interval('3 year')) and current_date
        then 1 else 0
    end as lipid_3yr_ind,
    case
        when lipid_final_date
            between (current_date - interval('1 year')) and current_date
        then 1 else 0
    end as lipid_1yr_ind,
    coalesce(stg_usnwr_diabetes_scans.lipid_scan_result_value,
        stg_usnwr_diabetes_sde_labs.lipid_result) as lipid_result_value,
    case
        when lipid_result_value < 130
        then 1 else 0
    end as lipid_under_130,
    -- MICRO
    case
        when --stg_usnwr_diabetes_flowsheet_labs_hx.type1_microa_1_yr = 1
            --or stg_usnwr_diabetes_flowsheet_labs_hx.type2_microa_1_yr = 1
            ((stg_usnwr_diabetes_scans.microa_scan_date
                between current_date - interval('1 year') and current_date)
                and stg_usnwr_diabetes_scans.microa_scans = 1)
        then stg_usnwr_diabetes_scans.microa_scans
        when stg_usnwr_diabetes_sde_labs.microalbuminuria_1yr_ind = 1
        then 1 else 0
    end as microa_1_yr,
    greatest(coalesce(cast(stg_usnwr_diabetes_scans.microa_scan_date as date), date('1999-01-01')),
        coalesce(stg_usnwr_diabetes_sde_labs.microalbuminuria_last_screened_date, date('1999-01-01')),
        coalesce(stg_usnwr_diabetes_flowsheet_labs_hx.type2_microa_date, date('1999-01-01'))
    ) as microa_date_final,
    -- RETINOPATHY
    case
        when stg_usnwr_diabetes_sde_labs.retinopathy_2yr_ind = 1
            or (date(stg_usnwr_diabetes_flowsheet_labs_hx.req_flow_eye_date)
                    between current_date and (current_date - interval('2 year'))
                and stg_usnwr_diabetes_flowsheet_labs_hx.flow_eye_ind = 1)
            or (date(stg_usnwr_diabetes_flowsheet_labs_hx.real_flow_eye_date)
                    between current_date and (current_date - interval('2 year'))
                and stg_usnwr_diabetes_flowsheet_labs_hx.flow_eye_ind = 1)
            or ((stg_usnwr_diabetes_scans.retinopathy_scan_date
                between current_date - interval('2 year') and current_date)
                and stg_usnwr_diabetes_scans.retinopathy_scans = 1)
        then 1 else 0
    end as retinopathy_screen_2_yr,
    greatest(coalesce(stg_usnwr_diabetes_sde_labs.retinopathy_last_screened_date, date('1999-01-01')),
        coalesce(stg_usnwr_diabetes_flowsheet_labs_hx.real_flow_eye_date, date('1999-01-01')),
        coalesce(date(stg_usnwr_diabetes_flowsheet_labs_hx.req_flow_eye_date), date('1999-01-01')),
        coalesce(stg_usnwr_diabetes_flowsheet_labs_hx.retinopathy_date, date('1999-01-01'))
    ) as retinopathy_date_final,
    case
        when stg_usnwr_diabetes_sde_labs.annual_labs_1yr_ind = 1
            or stg_usnwr_diabetes_sde_labs.annual_labs_2yr_ind = 1
            or stg_usnwr_diabetes_sde_labs.annual_labs_3yr_ind = 1
        then 1 else 0
    end as annual_labs_sde_ind
from
    {{ref('stg_usnwr_diabetes_primary_pop')}} as stg_usnwr_diabetes_primary_pop
    left join {{ref('stg_usnwr_diabetes_flowsheet_labs_hx')}} as stg_usnwr_diabetes_flowsheet_labs_hx
        on stg_usnwr_diabetes_flowsheet_labs_hx.patient_key = stg_usnwr_diabetes_primary_pop.primary_key
    left join {{ref('stg_usnwr_diabetes_scans')}} as stg_usnwr_diabetes_scans
        on stg_usnwr_diabetes_scans.primary_key = stg_usnwr_diabetes_primary_pop.primary_key
    left join {{ref('stg_usnwr_diabetes_sde_labs')}} as stg_usnwr_diabetes_sde_labs
        on stg_usnwr_diabetes_sde_labs.patient_key = stg_usnwr_diabetes_primary_pop.primary_key
