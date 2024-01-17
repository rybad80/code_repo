with redcap_events as (
    select
        stg_pcoti_redcap_all.pat_key,
        stg_pcoti_redcap_all.visit_key,
        stg_pcoti_redcap_all.record as record_id,
        coalesce(
            stg_pcoti_redcap_all.visit_key::varchar(25),
            stg_pcoti_redcap_all.record::varchar(25)
        ) as visit_key_or_record_id,
        case
            when stg_pcoti_redcap_all.event_type = 'cat call'
                then 'REDCap - CAT Call'
            when stg_pcoti_redcap_all.event_type = 'code: other'
                then 'REDCap - Code (Other)'
            when stg_pcoti_redcap_all.event_type = 'non-patient response team (tech)'
                then 'REDCap - Non-Patient Response Team'
            when stg_pcoti_redcap_all.event_type = 'code: arc'
                then 'REDCap - Code (ARC)'
            when stg_pcoti_redcap_all.event_type = 'code: arc progressing to cpa'
                then 'REDCap - Code (ARC -> CPA)'
            when stg_pcoti_redcap_all.event_type = 'non patient response team (medical)'
                then 'REDCap - Non-Patient Response Team'
            when stg_pcoti_redcap_all.event_type = 'code: cpa'
                then 'REDCap - Code (CPA)'
            when stg_pcoti_redcap_all.event_type = 'code: cpa'
                then 'REDCap - Code (CPA)'
            when stg_pcoti_redcap_all.event_type = 'accidental activation'
                then 'REDCap - Code (Accidental Activation)'
        end as event_type_name,
        case
            when stg_pcoti_redcap_all.event_type = 'cat call'
                then 'REDCAP_CAT_CALL'
            when stg_pcoti_redcap_all.event_type = 'code: other'
                then 'REDCAP_CODE_OTHER'
            when stg_pcoti_redcap_all.event_type = 'non-patient response team (tech)'
                then 'REDCAP_NONPAT_RESPTEAM'
            when stg_pcoti_redcap_all.event_type = 'code: arc'
                then 'REDCAP_CODE_ARC'
            when stg_pcoti_redcap_all.event_type = 'code: arc progressing to cpa'
                then 'REDCAP_CODE_ARC_CPA'
            when stg_pcoti_redcap_all.event_type = 'non patient response team (medical)'
                then 'REDCAP_NONPAT_RESPTEAM'
            when stg_pcoti_redcap_all.event_type = 'code: cpa'
                then 'REDCAP_CODE_CPA'
            when stg_pcoti_redcap_all.event_type = 'accidental activation'
                then 'REDCAP_CODE_AA'
        end as event_type_abbrev,
        stg_pcoti_redcap_all.event_location as redcap_event_location,
        case
            when stg_pcoti_redcap_all.event_location = '11 nw' then 101001622
            when stg_pcoti_redcap_all.event_location = '12 nw' then 101001623
            when stg_pcoti_redcap_all.event_location = '1east observation unit' then 101001617
            when stg_pcoti_redcap_all.event_location = '3c main' then 13
            when stg_pcoti_redcap_all.event_location = '3e main' then 13
            when stg_pcoti_redcap_all.event_location = '3e seashore' then 28
            when stg_pcoti_redcap_all.event_location = '3south' then 40
            when stg_pcoti_redcap_all.event_location = '3w seashore' then 27
            when stg_pcoti_redcap_all.event_location = '4e seashore (mbu)' then 104
            when stg_pcoti_redcap_all.event_location = '4east' then 14
            when stg_pcoti_redcap_all.event_location = '4south' then 41
            when stg_pcoti_redcap_all.event_location = '4w seashore' then 21
            when stg_pcoti_redcap_all.event_location = '5e main' then 45
            when stg_pcoti_redcap_all.event_location = '5s' then 35
            when stg_pcoti_redcap_all.event_location = '5w a' then 20
            when stg_pcoti_redcap_all.event_location = '5w b' then 124
            when stg_pcoti_redcap_all.event_location = '6e ccu' then 65
            when stg_pcoti_redcap_all.event_location = '6ne cpru' then 66
            when stg_pcoti_redcap_all.event_location = '6s cicu' then 36
            when stg_pcoti_redcap_all.event_location = '6w sdu' then 22
            when stg_pcoti_redcap_all.event_location = '7ne pcu' then 51
            when
                stg_pcoti_redcap_all.event_location = '7w non-picu'
                and stg_pcoti_redcap_all.event_dt_tm <= '2019-12-15'
                then 23
            when
                stg_pcoti_redcap_all.event_location = '7w non-picu'
                and stg_pcoti_redcap_all.event_dt_tm > '2019-12-15'
                then 123
            when stg_pcoti_redcap_all.event_location = '8 south' then 44
            when stg_pcoti_redcap_all.event_location = '8c main' then 101001401
            when stg_pcoti_redcap_all.event_location = '9 south' then 122
            when stg_pcoti_redcap_all.event_location = 'ed' then 10292012
            when stg_pcoti_redcap_all.event_location = 'koph 2 picu/onco' then 101003003
            when stg_pcoti_redcap_all.event_location = 'koph 3 or' then 101003004
            when stg_pcoti_redcap_all.event_location = 'koph 4 med/surg' then 101003005
            when stg_pcoti_redcap_all.event_location = 'koph 6 adol/gen peds' then 101003007
            when stg_pcoti_redcap_all.event_location = 'nicu (retired option)' then 10
            when stg_pcoti_redcap_all.event_location = 'nicu c (beds 40-51)' then 12
            when stg_pcoti_redcap_all.event_location = 'nicu east (beds 52-75)' then 101001070
            when stg_pcoti_redcap_all.event_location = 'nicu northeast (beds 76-98)' then 101001071
            when stg_pcoti_redcap_all.event_location = 'nicu west 1 (beds 24-39)' then 11
            when stg_pcoti_redcap_all.event_location = 'nicu west 2 (beds 1-23)' then 10
            when stg_pcoti_redcap_all.event_location = 'pacu' then 70
            when stg_pcoti_redcap_all.event_location = 'picu' then 123
        end as dept_id,
        stg_pcoti_redcap_all.event_dt_tm as event_start_date,
        case
            when stg_pcoti_redcap_all.event_dt_tm > current_date then current_date
            else stg_pcoti_redcap_all.event_dt_tm
        end as event_start_date_safe, -- deal with erroneous future dates
        null as event_end_date
    from
        {{ ref('stg_pcoti_redcap_all') }} as stg_pcoti_redcap_all
    where
        stg_pcoti_redcap_all.event_dt_tm >= '2017-01-01'
)

select
    redcap_events.pat_key,
    redcap_events.visit_key,
    redcap_events.record_id,
    redcap_events.visit_key_or_record_id,
    redcap_events.event_type_name,
    redcap_events.event_type_abbrev,
    redcap_events.dept_id,
    fact_department_rollup_summary.dept_key,
    coalesce(
        fact_department_rollup_summary.dept_nm,
        upper(redcap_events.redcap_event_location)
    ) as department_name,
    coalesce(
        fact_department_rollup_summary.unit_dept_grp_abbr,
        'OTHER'
    ) as department_group_name,
    coalesce(
        fact_department_rollup_summary.bed_care_dept_grp_abbr,
        'OTHER'
    ) as bed_care_group,
    -- for the moment, list non-matching dept_keys as being at PHL;
    -- this needs to be resolve upstream by correcting the list of location
    -- choices in the REDCap
    case
        when lower(fact_department_rollup_summary.department_center_abbr) like '%kop%' then 'KOPH'
        else 'PHL'
    end as campus_name,
    redcap_events.event_start_date,
    redcap_events.event_end_date
from
    redcap_events
    left join {{ source('cdw_analytics', 'fact_department_rollup_summary') }} as fact_department_rollup_summary
        on redcap_events.dept_id = fact_department_rollup_summary.dept_id
        and redcap_events.event_start_date_safe::date >= fact_department_rollup_summary.min_dept_align_dt
        and redcap_events.event_start_date_safe::date <= fact_department_rollup_summary.max_dept_align_dt
            