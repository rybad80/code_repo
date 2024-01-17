with historical_dates as (-- historical dates for which historical registry status and metrics will be pulled
    select
        -- list of dates for which metric history will be presented for each patient
        date(dim_date.full_date) as historical_date,
        -- for each historical date, this is the cutoff date for query-able data (as of historical date).
        -- included to ensure data we're pulling now aligns to previously-manually-pulled data on various
        -- historical dates. this alignment may not be important in the future.
        date(historical_date - interval '1 day') as cutoff_date --noqa
    from
        {{ref('dim_date')}} as dim_date
    where
        dim_date.full_date between '2021-02-01' and current_date
        -- can edit this date range as desired. glomerular disease registry data is not available before Jan2021.
        and dim_date.day_of_week = 2 -- limit to one day a week (Monday) for patient-week table
),

registry_metric_value_sequence as (
    -- identify all unique entries of registry metrics.
    -- This accounts for duplicate records (same value, entry date, etc. but different record).
    select
        registry_data_info.pat_key,
        master_charge_edit_rule.rule_id,
        master_charge_edit_rule.rule_nm,
        master_charge_edit_rule.display_nm as metric_nm,
        registry_metric_history.metric_string_value,
        registry_metric_history.metric_last_upd_dt,
        registry_metric_history.create_dt,
        row_number() over (partition by registry_data_info.pat_key, master_charge_edit_rule.rule_id
                            order by registry_metric_history.metric_last_upd_dt desc) as seq
        -- sequence values from registry_metric_value_pretable so start and end times
        -- can be calculated for each value. 
    from
        {{source('cdw', 'registry_data_info')}} as registry_data_info
        inner join {{source('cdw', 'registry_metric_history')}} as registry_metric_history
            on registry_data_info.record_key = registry_metric_history.record_key
        inner join {{source('cdw', 'master_charge_edit_rule')}} as master_charge_edit_rule
            on registry_metric_history.mstr_chrg_edit_rule_key = master_charge_edit_rule.mstr_chrg_edit_rule_key
    where
        master_charge_edit_rule.rule_id in (--first three metrics are used in defining cohort, others in reporting 
                                            '1380979', -- chop dm glom evaluate nephrotic syndrome count
                                            '1382495', -- chop dm glom patient transferred
                                            '1386336', -- chop dm glom appointment with dialysis visit type
                                            '1387066', -- chop dm glom last nephrology visit (3y)
                                            '1387067', -- chop dm glom last nephrology provider (3y)
                                            '1017191', -- chop dm ckd next nephrology appt
                                            '1386535', -- chop dm glom primary dx last neph visit
                                            '1386009', -- chop dm glom remission status
                                            '1386811', -- chop dm glom remission status date
                                            '1017522', -- chop dm last urinalysis protein
                                            '644363', -- chop dm last urinalysis date
                                            '1386358', -- chop dm glom number of hospital admissions (30 days)
                                            '1386363', -- chop dm glom recent admission length of stay (30 days)
                                            '1389514', -- chop dm last ed/hosp discharge date prior to readmission
                                            '1387274', -- chop dm covid19 most recent vaccine date
                                            '642861', -- chop dm imm last flu vaccine
                                            '1386337', -- chop dm glom most recent pneumovax
                                            '1386339', -- chop dm glom 2nd most recent pneumovax
                                            '1386340', -- chop dm glom most recent prevnar
                                            '1386341', -- chop dm glom 2nd most recent prevnar
                                            '1386343', -- chop dm glom 3rd most recent prevnar
                                            '1386345', -- chop dm glom 4th most recent prevnar
                                            '1387086' -- chop dm glom last nephrology department (3y)
                                            )
    group by
        registry_data_info.pat_key,
        master_charge_edit_rule.rule_id,
        master_charge_edit_rule.rule_nm,
        metric_nm,
        registry_metric_history.metric_string_value,
        registry_metric_history.metric_last_upd_dt,
        registry_metric_history.create_dt
),

registry_metric_value_dates as (
    -- assign start and end time to each metric value, limit values to only those active given date parameters, 
    -- sequence values in instances where metrics were updated multiple times on date specified. 
    select
        registry_metric_value_sequence.pat_key,
        historical_dates.historical_date,
        historical_dates.cutoff_date,
        registry_metric_value_sequence.rule_id,
        registry_metric_value_sequence.rule_nm,
        registry_metric_value_sequence.metric_nm,
        registry_metric_value_sequence.metric_string_value,
        registry_metric_value_sequence.seq, -- 1 is current value as of date of data pull
        registry_metric_value_sequence.metric_last_upd_dt as value_start_date,
        registry_metric_end_dates.metric_last_upd_dt as value_end_date,
        registry_metric_value_sequence.create_dt as value_start_create_date,
        registry_metric_end_dates.create_dt as value_end_create_date,
        -- find update sequence in time period. 1 is most recent value as of pull.
        -- this is necessary for metrics which were updated multiple times on pull date. 
        row_number() over (partition by registry_metric_value_sequence.pat_key,
                            registry_metric_value_sequence.rule_id, historical_dates.historical_date
                            order by registry_metric_value_sequence.metric_last_upd_dt desc) as historical_seq
    from
        registry_metric_value_sequence
        inner join historical_dates
            on 1 = 1
        left join registry_metric_value_sequence as registry_metric_end_dates
            on registry_metric_value_sequence.pat_key = registry_metric_end_dates.pat_key
            and registry_metric_value_sequence.rule_id = registry_metric_end_dates.rule_id
            and registry_metric_value_sequence.seq = (registry_metric_end_dates.seq + 1)
    where
        date(registry_metric_value_sequence.create_dt) <= historical_dates.historical_date
        -- using create_date and historical_date as these criteria produce a table with registry metric values
        -- which matched TDL snapshot in validation.
        and (date(registry_metric_end_dates.create_dt) > historical_dates.historical_date
            or registry_metric_end_dates.create_dt is null)
)

-- identify registry metric values as of each historical date.
select
    registry_metric_value_dates.pat_key,
    registry_metric_value_dates.historical_date,
    -- metrics to define cohort
    max(case when registry_metric_value_dates.rule_id = '1380979'
                then registry_metric_value_dates.metric_string_value else null end) as neph_count_ind,
    max(case when registry_metric_value_dates.rule_id = '1382495'
                then registry_metric_value_dates.metric_string_value else null end) as transfer_ind,
    max(case when registry_metric_value_dates.rule_id = '1386336'
                then registry_metric_value_dates.metric_string_value else null end) as dialysis_ind,
    -- metrics for reporting
    max(case when registry_metric_value_dates.rule_id = '1387066'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as last_neph_visit,
    max(case when registry_metric_value_dates.rule_id = '1017191'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as next_neph_appt,
    max(case when registry_metric_value_dates.rule_id = '1386535'
                then registry_metric_value_dates.metric_string_value
                else null end) as gd_primary_dx_last_neph_visit, --returns dx_id
    max(case when registry_metric_value_dates.rule_id = '1387067'
                then registry_metric_value_dates.metric_string_value
                else null end) as last_neph_prov, --returns prov_id
    max(case when registry_metric_value_dates.rule_id = '1386009'
                then registry_metric_value_dates.metric_string_value
                else null end) as remission_status,
    max(case when registry_metric_value_dates.rule_id = '1386811'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as remission_status_date,
    max(case when registry_metric_value_dates.rule_id = '1017522'
                then registry_metric_value_dates.metric_string_value
                else null end) as urine_protein,
    max(case when registry_metric_value_dates.rule_id = '644363'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as last_urinalysis_3yr,
    max(case when registry_metric_value_dates.rule_id = '1386358'
                then registry_metric_value_dates.metric_string_value
                else null end) as admission_count_past_30_days,
    max(case when registry_metric_value_dates.rule_id = '1386363'
                then registry_metric_value_dates.metric_string_value
                else null end) as ip_days_past_30_days,
    max(case when registry_metric_value_dates.rule_id = '1389514'
                then registry_metric_value_dates.metric_string_value
                else null end) as revisit_7_day_acute_3_month,
    max(case when registry_metric_value_dates.rule_id = '1387274'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as last_covid_19_vaccine,
    max(case when registry_metric_value_dates.rule_id = '642861'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as most_recent_flu_vaccine,
    max(case when registry_metric_value_dates.rule_id = '1386337'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as most_recent_pneumovax,
    max(case when registry_metric_value_dates.rule_id = '1386339'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as second_most_recent_pneumovax,
    max(case when registry_metric_value_dates.rule_id = '1386340'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as most_recent_prevnar_13,
    max(case when registry_metric_value_dates.rule_id = '1386341'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as second_most_recent_prevnar_13,
    max(case when registry_metric_value_dates.rule_id = '1386343'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as third_most_recent_prevnar_13,
    max(case when registry_metric_value_dates.rule_id = '1386345'
                then cast('1840-12-31' as date) + cast(registry_metric_value_dates.metric_string_value as int)
                else null end) as fourth_most_recent_prevnar_13,
    max(case when registry_metric_value_dates.rule_id = '1387086'
                then registry_metric_value_dates.metric_string_value
                else null end) as last_nephrology_department_id
from
    registry_metric_value_dates
where
    registry_metric_value_dates.historical_seq = 1
    -- for metrics updated twice in the same day, ensure we are pulling most recent update. 
group by
    registry_metric_value_dates.pat_key,
    registry_metric_value_dates.historical_date
