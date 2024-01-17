with flowsheet_agg as (
    select
        stg_cardiac_arrest_huddle.pat_key,
        stg_cardiac_arrest_huddle.huddle_key,
        stg_cardiac_arrest_huddle.recorded_date as huddle_date,
        max(case when cardiac_arrest_all.arrest_date between stg_cardiac_arrest_huddle.recorded_date
                and stg_cardiac_arrest_huddle.recorded_date + cast('4 hours' as interval)
                then 1
                else 0 end
        ) as arrest_ind,
        min(case when stg_cardiac_arrest_epi.action_date >= stg_cardiac_arrest_huddle.recorded_date
                then stg_cardiac_arrest_epi.action_date
                else null end
        ) as huddle_next_epi_admin_date,
        extract(
            epoch from (huddle_next_epi_admin_date - stg_cardiac_arrest_huddle.recorded_date)
        ) / 3600.0 as time_to_epi_hr,
        min(case when stg_cardiac_arrest_ecmo.rec_dt >= stg_cardiac_arrest_huddle.recorded_date
                then stg_cardiac_arrest_ecmo.rec_dt
                else null end
        ) as huddle_next_ecmo_date,
        extract(
            epoch from (huddle_next_ecmo_date - stg_cardiac_arrest_huddle.recorded_date)
        ) / 3600.0 as time_to_ecmo_hr
    from
        {{ref('stg_cardiac_arrest_huddle')}} as stg_cardiac_arrest_huddle
        left join {{ref('cardiac_arrest_all')}} as cardiac_arrest_all
            on cardiac_arrest_all.pat_key = stg_cardiac_arrest_huddle.pat_key
        left join {{ref('stg_cardiac_arrest_epi')}} as stg_cardiac_arrest_epi
            on stg_cardiac_arrest_epi.pat_key = stg_cardiac_arrest_huddle.pat_key
        left join {{ref('stg_cardiac_arrest_ecmo')}} as stg_cardiac_arrest_ecmo
            on stg_cardiac_arrest_ecmo.pat_key = stg_cardiac_arrest_huddle.pat_key
    group by
        stg_cardiac_arrest_huddle.pat_key,
        stg_cardiac_arrest_huddle.huddle_key,
        stg_cardiac_arrest_huddle.recorded_date
),

flowsheet_huddle as (
    select
        huddle_key,
        max(
            case when fs_short_nm = 'huddle_needed' then cast(meas_val as varchar(500)) end
        ) as huddle_needed,
        max(
            case when fs_short_nm = 'no_huddle_reason' then cast(meas_val as varchar(500)) end
        ) as no_huddle_reason,
        max(
            case when fs_short_nm = 'staff_present' then cast(meas_val as varchar(1000)) end
        ) as staff_present,
        max(
            case when fs_short_nm = 'nurse_supported_ind' then cast(meas_val as varchar(5) ) end
        ) as nurse_supported_ind,
        max(
            case when fs_short_nm = 'bedside_medications' then cast(meas_val as varchar(5)) end
        ) as bedside_medications,
        max(
            case when fs_short_nm = 'lines_access' then cast(meas_val as varchar(300) ) end
        ) as lines_access,
        max(
            case when fs_short_nm = 'intub_extub_plan' then cast(meas_val as varchar(300) ) end
        ) as intub_extub_plan,
        max(
            case when fs_short_nm = 'roles_identified' then cast(meas_val as varchar(300) ) end
        ) as roles_identified,
        max(
            case
                when fs_short_nm = 'location_of_meds_given_during_resuscitation'
                    then cast(meas_val as varchar(300) ) end
        ) as location_of_meds_given_during_resuscitation,
        max(
            case when fs_short_nm = 'testing_needed' then cast(meas_val as varchar(300) ) end
        ) as testing_needed,
        max(
            case when fs_short_nm = 'testing_other' then cast(meas_val as varchar(300) ) end
        ) as testing_other,
        max(
            case when fs_short_nm = 'equipment_needed' then cast(meas_val as varchar(300) ) end
        ) as equipment_needed,
        max(
            case when fs_short_nm = 'blood_product_availability' then cast(meas_val as varchar(300) ) end
        ) as blood_product_availability,
        max(
            case when fs_short_nm = 'family_communication_plan' then cast(meas_val as varchar(300) ) end
        ) as family_communication_plan,
        max(
            case when fs_short_nm = 'reassess_plan' then cast(meas_val as varchar(300) ) end
        ) as reassess_plan,
        max(
            case when fs_short_nm = 'reassess_other' then cast(meas_val as varchar(300) ) end
        ) as reassess_other
    from
        {{ref('stg_cardiac_arrest_huddle')}}
    group by
        huddle_key
)

select
    flowsheet_agg.pat_key,
    flowsheet_agg.huddle_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    flowsheet_agg.huddle_date,
    flowsheet_agg.arrest_ind,
    flowsheet_agg.huddle_next_epi_admin_date,
    flowsheet_agg.time_to_epi_hr,
    flowsheet_agg.huddle_next_ecmo_date,
    flowsheet_agg.time_to_ecmo_hr,
    flowsheet_huddle.huddle_needed,
    flowsheet_huddle.no_huddle_reason,
    flowsheet_huddle.staff_present,
    flowsheet_huddle.nurse_supported_ind,
    flowsheet_huddle.bedside_medications,
    'Epic' as data_source,
    flowsheet_huddle.lines_access,
    flowsheet_huddle.intub_extub_plan,
    flowsheet_huddle.roles_identified,
    flowsheet_huddle.location_of_meds_given_during_resuscitation,
    flowsheet_huddle.testing_needed,
    flowsheet_huddle.testing_other,
    flowsheet_huddle.equipment_needed,
    flowsheet_huddle.blood_product_availability,
    flowsheet_huddle.family_communication_plan,
    flowsheet_huddle.reassess_plan,
    flowsheet_huddle.reassess_other
from
    flowsheet_huddle
inner join flowsheet_agg
    on flowsheet_agg.huddle_key = flowsheet_huddle.huddle_key
inner join {{ref('stg_patient')}} as stg_patient on stg_patient.pat_key = flowsheet_agg.pat_key
