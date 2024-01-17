with vps_procedures as (
    --region
    select
        'PHL' as picu_unit,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'caseid'
            ])
        }} as vps_episode_key,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'caseid',
                'procid'
            ])
        }} as procedure_vps_episode_key,
        caseid as case_id,
        procid as procedure_id,
        descript_name as procedure_name,
        cat_name as proc_category,
        subcat_name as proc_subcategory,
        actual_startdatetime,
        actual_enddatetime,
        actual_procedureduration_indays as procedure_duration_days,
        icuconstrained_startdatetime,
        icuconstrained_enddatetime,
        presonadmit as present_on_admn,
        presondisch as present_on_disch,
        geoloc as geographic_location,
        anatomicalloc as anatomical_location,
        anatomical_other,
        side
    from
        {{source('vps_phl_ods', 'vps_phl_int_procs')}}

    union all

    select
        'KOPH' as picu_unit,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'caseid'
            ])
        }} as vps_episode_key,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'caseid',
                'procid'
            ])
        }} as procedure_vps_episode_key,
        caseid as case_id,
        procid as procedure_id,
        descript_name as procedure_name,
        cat_name as proc_category,
        subcat_name as proc_subcategory,
        actual_startdatetime,
        actual_enddatetime,
        actual_procedureduration_indays as procedure_duration_days,
        icuconstrained_startdatetime,
        icuconstrained_enddatetime,
        presonadmit as present_on_admn,
        presondisch as present_on_disch,
        geoloc as geographic_location,
        anatomicalloc as anatomical_location,
        anatomical_other,
        side
    from
        {{source('vps_koph_ods', 'vps_koph_int_procs')}}
    --end region
),

procedure_elements as (
    --region
    select
        proccodecaseid,
        element_type,
        element
    from
        {{source('vps_phl_ods', 'vps_phl_int_procs_elements')}}
    where
        element_type is not null

    union all

    select
        proccodecaseid,
        element_type,
        element
    from
        {{source('vps_koph_ods', 'vps_koph_int_procs_elements')}}
    where
        element_type is not null
    --end region
)

select
    picu_unit,
    vps_episode_key,
    procedure_vps_episode_key,
    case_id,
    procedure_id,
    procedure_name,
    proc_category,
    proc_subcategory,
    case
        when length(trim(actual_startdatetime)) = 16 then to_timestamp(actual_startdatetime, 'mm/dd/yyyy HH24:MI')
        when length(trim(actual_startdatetime)) = 10 then to_timestamp(actual_startdatetime, 'mm/dd/yyyy')
    end as actual_procedure_start,
    case
        when length(trim(actual_enddatetime)) = 16 then to_timestamp(actual_enddatetime, 'mm/dd/yyyy HH24:MI')
        when length(trim(actual_enddatetime)) = 10 then to_timestamp(actual_enddatetime, 'mm/dd/yyyy')
    end as actual_procedure_end,
    procedure_duration_days,
    case
        when
            length(trim(icuconstrained_startdatetime)) = 16
            then to_timestamp(icuconstrained_startdatetime, 'mm/dd/yyyy HH24:MI')
        when
            length(trim(icuconstrained_startdatetime)) = 10
            then to_timestamp(icuconstrained_startdatetime, 'mm/dd/yyyy')
    end as icu_constrained_start,
    case
        when
            length(trim(icuconstrained_enddatetime)) = 16
            then to_timestamp(icuconstrained_enddatetime, 'mm/dd/yyyy HH24:MI')
        when
            length(trim(icuconstrained_enddatetime)) = 10
            then to_timestamp(icuconstrained_enddatetime, 'mm/dd/yyyy')
    end as icu_constrained_end,
    present_on_admn,
    present_on_disch,
    geographic_location,
    anatomical_location::varchar(100) as anatomical_location,
    initcap(anatomical_other) as anatomical_other,
    side::varchar(50) as side,
    group_concat(
        case when procedure_elements.element_type = 'Removal' then procedure_elements.element end, ';'
    )::varchar(255) as removal_reason,
    group_concat(
        case when procedure_elements.element_type = 'Complications' then procedure_elements.element end, ';'
    )::varchar(255) as complications,
    group_concat(
        case when procedure_elements.element_type = 'Variables' then procedure_elements.element end, ';'
    )::varchar(255) as variables
from
    vps_procedures
    left join procedure_elements
        on vps_procedures.procedure_id = procedure_elements.proccodecaseid
group by
    picu_unit,
    vps_episode_key,
    procedure_vps_episode_key,
    case_id,
    procedure_id,
    procedure_name,
    proc_category,
    proc_subcategory,
    actual_startdatetime,
    actual_enddatetime,
    procedure_duration_days,
    icuconstrained_startdatetime,
    icuconstrained_enddatetime,
    present_on_admn,
    present_on_disch,
    geographic_location,
    anatomical_location,
    anatomical_other,
    side
