-- Get numerator
with
    overall_data_pull as (
        select
              'CAUTI' as harm_type,
             event_dt,
             conf_dt,
             dept_key,
             'N/A' as division,
             pat_key,
             hai_event_id as harm_id,
             'METRICS_HAI' as numerator_source,
             visit_key,
             pathogen_code_1,
             pathogen_code_2,
             pathogen_code_3,
            1 as numerator_value
        from {{ ref('fact_ip_cauti') }}
        where reportable_ind = 1
        union distinct
        select
              'CLABSI' as harm_type,
             event_dt,
             conf_dt,
             dept_key,
             'N/A' as division,
             pat_key,
             hai_event_id as harm_id,
             'METRICS_HAI' as numerator_source,
             visit_key,
             pathogen_code_1,
             pathogen_code_2,
             pathogen_code_3,
             1 as numerator_value
        from {{ ref('fact_ip_clabsi') }}
        where reportable_ind = 1
        union distinct
        select
              'HAVI' as harm_type,
             event_dt,
             conf_dt,
             dept_key,
             'N/A' as division,
             pat_key,
             hai_event_id as harm_id,
             'METRICS_HAI' as numerator_source,
             visit_key,
             pathogen_code_1,
             pathogen_code_2,
             pathogen_code_3,
             1 as numerator_value
        from {{ ref('fact_ip_havi') }}
        where reportable_ind = 1
        union distinct
        select
              'VAP' as harm_type,
             event_dt,
             conf_dt,
             dept_key,
             'N/A' as division,
             pat_key,
             hai_event_id as harm_id,
             'METRICS_HAI' as numerator_source,
             visit_key,
             pathogen_code_1,
             pathogen_code_2,
             pathogen_code_3,
             1 as numerator_value
        from {{ ref('fact_ip_vap') }}
        where reportable_ind = 1
        union distinct
        select
            'Falls with Injury' as harm_type,
            fall_dt as event_dt,
            coalesce(conf_dt, submit_dt, fall_dt) as conf_dt,
            dept_key,
            'N/A' as division,
            pat_key,
            cast(record_id as bigint) as harm_id,
            'REDCAP' as numerator_source,
            visit_key as visit_key,
            'N/A' as pathogen_code_1,
            'N/A' as pathogen_code_2,
            'N/A' as pathogen_code_3,
            1 as numerator_value
        from {{ ref('fact_ip_fall') }}
        where reportable_ind = 1
        union distinct
        -- Get Falls from METRICS_NURSING_UNIT (prior to 2015)
       select
             'Falls with Injury' as harm_type,
             to_date(fall.metric_dt_key, 'YYYYMMDD') as event_dt,
             null as conf_dt,
             coalesce(m.historical_dept_key, fall.dept_key) as dept_key,
             'N/A' as division,
             -1 as pat_key,
             -1 as harm_id,
             'METRICS_NURSING_UNIT' as numerator_source,
             -1 as visit_key,
            'N/A' as pathogen_code_1,
            'N/A' as pathogen_code_2,
            'N/A' as pathogen_code_3,
            sum(fall.numerator) as numerator_value
        from
            {{ source('cdw', 'metrics_nursing_unit') }} as fall
            left join {{ source('cdw', 'department') }} as d on d.dept_key = fall.dept_key
            left join {{ ref('master_harm_prevention_dept_mapping') }} as m --remove mapping
                on m.harm_type = 'Falls with Injury'
                and m.current_dept_key = fall.dept_key
                and to_date(fall.metric_dt_key, 'YYYYMMDD') between m.start_dt and m.end_dt
                and m.denominator_only_ind = 0
        where
            fall.metric_key = 113
            and fall.metric_dt_key between 20130101 and 20141231
            and lower(fall.fall_severity) in ('level 2', 'level 3', 'level 4')
            and d.dept_id not in ('10292012', '101001045', '900100100' ) --exclude outpatient unit 
        group by
            event_dt,
            coalesce(m.historical_dept_key, fall.dept_key)
        having
            sum(fall.numerator) > 0
        union distinct
        select
            'HAPI' as harm_type,
            discovered_dt as event_date,
            --, SUBMIT_DT AS CONF_DATE
            coalesce(cast(conf_dt as date), submit_dt, discovered_dt ) as conf_date,
            coalesce(developed_dept_key, discovered_dept_key) as dept_key,
            'N/A' as division,
            pat_key,
            cast(record_id as bigint) as harm_id,
            'REDCAP' as numerator_source,
            visit_key,
            'N/A' as pathogen_code_1,
            'N/A' as pathogen_code_2,
            'N/A' as pathogen_code_3,
            1 as numerator_value
        from {{ ref('fact_ip_hapi') }}
        where reportable_ind = 1
        union distinct
        select
            'PIVIE' as harm_type,
            date(infiltration_dt) as event_date,
            --, SUBMIT_DT AS CONF_DATE
            coalesce(cast(conf_dt as date), submit_dt, date(infiltration_dt)) as conf_date,
            developed_dept_key as dept_key,
            'N/A' as division,
            pat_key,
            cast(record_id as bigint) as harm_id,
            'REDCAP' as numerator_source,
            visit_key,
            'N/A' as pathogen_code_1,
            'N/A' as pathogen_code_2,
            'N/A' as pathogen_code_3,
            1 as numerator_value
        from {{ ref('fact_ip_pivie') }}
        where reportable_ind = 1
        union distinct
        select
            'VTE' as harm_type,
            event_dt as event_date,
            --, SUBMIT_DT AS CONF_DATE
            coalesce(cast(conf_dt as date), submit_dt, cast(event_dt as date)) as conf_date,
            developed_dept_key as dept_key,
            'N/A' as division,
            pat_key,
            cast(record_id as bigint) as harm_id,
            'REDCAP' as numerator_source,
            visit_key,
            'N/A' as pathogen_code_1,
            'N/A' as pathogen_code_2,
            'N/A' as pathogen_code_3,
            1 as numerator_value
        from {{ ref('fact_ip_vte') }}
        where reportable_ind = 1
        union distinct
        select
              'SSI' as harm_type,
             surg_dt,
             conf_dt,
             dept_key,
             division,
             pat_key,
             hai_event_id as harm_id,
             'METRICS_HAI' as numerator_source,
             visit_key,
             pathogen_code_1,
             pathogen_code_2,
             pathogen_code_3,
             1 as numerator_value
        from {{ ref('fact_ip_ssi') }}
        where reportable_ind = 1
        union distinct
        select
            'UE' as harm_type,
             coalesce((event_dt + event_tm), event_dt) as event_date,
             coalesce(submit_dt, event_dt) as conf_date,
             dept_key,
             'N/A' as division,
             pat_key,
             record_id as harm_id,
             'REDCAP' as numerator_source,
             visit_key,
             'N/A' as pathogen_code_1,
             'N/A' as pathogen_code_2,
             'N/A' as pathogen_code_3,
             1 as numerator_value
        from {{ ref('fact_ip_ue') }}
     --   where event_date >= '2021-01-19' --program start
        where reportable_ind = 1
)
select
    isnull(visit.visit_key, 0) as visit_key,
    overall_data_pull.event_dt,
    overall_data_pull.pat_key,
    overall_data_pull.dept_key,
    coalesce(dept_groups_by_date.mstr_dept_grp_chop_key, dept_groups_imputation.mstr_dept_grp_chop_key) as mstr_dept_grp_key,
    coalesce(dept_groups_by_date.chop_dept_grp_nm, dept_groups_imputation.chop_dept_grp_nm) as dept_grp_nm,
    coalesce(dept_groups_by_date.chop_dept_grp_abbr, dept_groups_imputation.chop_dept_grp_abbr) as dept_grp_abbr,
    visit.enc_id as csn,
    overall_data_pull.harm_id,
    cast(overall_data_pull.harm_type as varchar(30)) as harm_type,
    cast(overall_data_pull.numerator_source as varchar(50)) as numerator_source,
    cast(upper(overall_data_pull.division) as varchar(50)) as division,
    cast(coalesce(overall_data_pull.pathogen_code_1, 'N/A')as varchar(50)) as pathogen_code_1,
    cast(coalesce(overall_data_pull.pathogen_code_2, 'N/A')as varchar(50)) as pathogen_code_2,
    cast(coalesce(overall_data_pull.pathogen_code_3, 'N/A')as varchar(50)) as pathogen_code_3,
    cast(overall_data_pull.numerator_value as integer) as numerator_value,
    cast(0 as integer) as denominator_value,
    overall_data_pull.conf_dt,
    visit.hosp_admit_dt,
    visit.hosp_dischrg_dt,
    cast(1 as byteint) as compare_to_hai_pop_days_ind,
    coalesce(race_eth.pat_race_ethnicity, 'blank') as pat_race_ethnicity,
    coalesce(pref_lang.pat_pref_lang, 'blank') as pat_pref_lang
from overall_data_pull
left join {{ref('stg_harm_dept_grp')}} as dept_groups_by_date
        on dept_groups_by_date.dept_key = overall_data_pull.dept_key
            and date(
                overall_data_pull.event_dt
            ) = dept_groups_by_date.dept_align_dt
    left join {{ref('stg_harm_dept_grp')}} as dept_groups_imputation
        on dept_groups_imputation.dept_key = OVERALL_DATA_PULL.DEPT_KEY
            and dept_groups_imputation.depts_seq_num = 1   
left join {{ source('cdw', 'visit') }} as VISIT on VISIT.VISIT_KEY = OVERALL_DATA_PULL.VISIT_KEY
left join {{ ref('stg_realdata_race_eth') }} as race_eth on race_eth.pat_key = overall_data_pull.pat_key
left join {{ ref('stg_realdata_pref_lang') }} as pref_lang on pref_lang.pat_key = overall_data_pull.pat_key
where date(OVERALL_DATA_PULL.EVENT_DT) >= '2010-07-01'
