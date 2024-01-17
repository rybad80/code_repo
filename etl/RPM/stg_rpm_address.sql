with delivery_address as (
    select
        stg_rpm_patient.pat_id,
        group_concat(initcap(replace(replace(
            patient_oth_staddr.oth_street_addr, '?', ''), ',', '')), ' ')
        as delivery_address,
        initcap(patient_2.oth_city) as city,
        patient_2.oth_zip as zip,
        zc_state.name as state --noqa: L029
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'patient_oth_staddr') }} as patient_oth_staddr
            on stg_rpm_patient.pat_id = patient_oth_staddr.pat_id
        left join {{ source('clarity_ods', 'patient_2') }} as patient_2
            on patient_oth_staddr.pat_id = patient_2.pat_id
        left join {{ source('clarity_ods', 'zc_state') }} as zc_state
            on patient_2.oth_state_c = zc_state.state_c
    group by
        stg_rpm_patient.pat_id,
        patient_2.oth_city,
        patient_2.oth_zip,
        zc_state.name
),

temp_address as (
    select
        stg_rpm_patient.pat_id,
        min(pat_temp_st_addr.line) as latest_addr,
        replace(pat_temp_st_addr.temp_address, ',', '') as temp_address,
        initcap(patient.tmp_city) as city,
        patient.tmp_zip as zip,
        zc_state.name as state --noqa: L029
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'pat_temp_st_addr') }} as pat_temp_st_addr
            on stg_rpm_patient.pat_id = pat_temp_st_addr.pat_id
            and pat_temp_st_addr.line = 1
        left join {{ source('clarity_ods', 'patient') }} as patient
            on pat_temp_st_addr.pat_id = patient.pat_id
        left join {{ source('clarity_ods', 'zc_state') }} as zc_state
            on patient.tmp_state_c = zc_state.state_c
    group by
        stg_rpm_patient.pat_id,
        pat_temp_st_addr.temp_address,
        patient.tmp_city,
        patient.tmp_zip,
        zc_state.name
),

permanent_address as (
    select
        stg_rpm_patient.pat_id,
        group_concat(pat_address.address, ' ') as home_address,
        patient.city,
        patient.zip,
        zc_state.name as state --noqa: L029
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'pat_address') }} as pat_address
            on stg_rpm_patient.pat_id = pat_address.pat_id
        left join {{ source('clarity_ods', 'patient') }} as patient
            on pat_address.pat_id = patient.pat_id
        left join {{ source('clarity_ods', 'zc_state') }} as zc_state
            on patient.state_c = zc_state.state_c
    group by
        stg_rpm_patient.pat_id,
        patient.city,
        patient.zip,
        zc_state.name
)

select
	stg_rpm_patient.pat_id,
	(delivery_address.delivery_address || ' ' || delivery_address.city || '; '
        || delivery_address.state || ' ' || delivery_address.zip) as delivery_address, --noqa: L016
	(temp_address.temp_address || ' ' || temp_address.city || '; '
        || temp_address.state || ' ' || temp_address.zip) as temporary_address, --noqa: L016
	(permanent_address.home_address || ' ' || permanent_address.city || '; '
        || permanent_address.state || ' ' || permanent_address.zip) as home_address --noqa: L016
from
    {{ref('stg_rpm_patient') }} as stg_rpm_patient
    left join delivery_address
        on stg_rpm_patient.pat_id = delivery_address.pat_id
    left join temp_address
        on stg_rpm_patient.pat_id = temp_address.pat_id
    left join permanent_address
        on stg_rpm_patient.pat_id = permanent_address.pat_id
