with flowsheet_ventilation_info as (
    select
        flowsheet_measure.rec_dt as recorded_date,
        flowsheet_record.fs_rec_id as flowsheet_record_id,
        max(case when flowsheet.fs_id = 40002468 then flowsheet_measure.meas_val_num end) as fio2_percent,
        max(case when flowsheet.fs_id = 10 then flowsheet_measure.meas_val_num end) as spo2_percent,
        max(
            case when flowsheet.fs_id = 40000242 then cast(flowsheet_measure.meas_val as varchar(40)) end
        ) as resp_o2_device,
        max(
            case when flowsheet.fs_id = 40000234 then flowsheet_measure.meas_val_num end
        ) as o2_flow_rate_liters_per_minute,
        max(
            case when flowsheet.fs_id = 40061268 then cast(flowsheet_measure.meas_val as varchar(30)) end
        ) as respiratory_within_defined_limits,
        max(case when flowsheet.fs_id = 40002725 then flowsheet_measure.meas_val_num end) as etco2_mmhg,
        max(case when flowsheet.fs_id = 9 then flowsheet_measure.meas_val_num end) as respirations_per_minute,
        max(
            case when flowsheet.fs_id = 40003569 then cast(flowsheet_measure.meas_val as varchar(30)) end
        ) as pulse_ox_site_rotated,
        max(
            case when flowsheet.fs_id = 40068126 then cast(flowsheet_measure.meas_val as varchar(30)) end
        ) as secretion_suction_location,
        max(
            case when flowsheet.fs_id = 40068114 then cast(flowsheet_measure.meas_val as varchar(30)) end
        ) as airway_secretion_amount,
        max(
            case when flowsheet.fs_id = 40068115 then cast(flowsheet_measure.meas_val as varchar(300)) end
        ) as airway_secretion_color_and_consistency,
        max(case when flowsheet.fs_id = 7631 then flowsheet_measure.meas_val_num end) as barometric_pressure,
        max(case when flowsheet.fs_id = 7671 then flowsheet_measure.meas_val_num end) as airway_pressure,
        max(case when flowsheet.fs_id = 7745 then flowsheet_measure.meas_val_num end) as minute_volume,
        max(case when flowsheet.fs_id = 7737 then flowsheet_measure.meas_val_num end) as tidal_volume,
        max(case when flowsheet.fs_id = 40000564 then flowsheet_measure.meas_val_num end) as tidal_volume_set,
        max(case when flowsheet.fs_id = 40000702 then flowsheet_measure.meas_val_num end) as tidal_volume_expired,
        max(
            case when flowsheet.fs_id = 40008116 then cast(flowsheet_measure.meas_val as varchar(40)) end
        ) as artificial_airway_type,
        max(case when flowsheet.fs_id in (3009165, 40010946) then flowsheet_measure.meas_val_num end
        ) as pip_set_cm_h2o,
        max(case when flowsheet.fs_id in (3009166, 40000566) then flowsheet_measure.meas_val_num end
        ) as peep_cpap_set_cm_h2o,
        max(case when flowsheet.fs_id  in (7681, 40000699) then flowsheet_measure.meas_val_num end) as pip_cm_h2o,
        max(case when flowsheet.fs_id in (7679, 40010947) then flowsheet_measure.meas_val_num end) as peep_cm_h2o,
        max(
            case when flowsheet.fs_id = 40010942 then cast(flowsheet_measure.meas_val as varchar(30)) end
        ) as invasive_mode,
        max(
            case when flowsheet.fs_id = 40002718 then cast(flowsheet_measure.meas_val as varchar(30)) end
        ) as non_invasive_mode,
        max(case when flowsheet.fs_id = 40011001 then flowsheet_measure.meas_val_num end) as map_non_inv,
        max(case when flowsheet.fs_id = 40002559 then flowsheet_measure.meas_val_num end) as map_inv,
        max(case when flowsheet.fs_id = 40010984 then flowsheet_measure.meas_val_num end) as map_high_freq,
        max(case when flowsheet.fs_id = 40069509 then flowsheet_measure.meas_val_num end) as map_vdr,
        max(case when flowsheet.fs_id = 40000715 then flowsheet_measure.meas_val_num end
        ) as hfov_amplitude_ordered,
        max(case when flowsheet.fs_id = 40000705 then flowsheet_measure.meas_val_num end
        ) as hfov_frequency_hz,
        max(case when flowsheet.fs_id = 40010978 then flowsheet_measure.meas_val_num end
        ) as hfov_paw_ordered_cm_h2o,
        max(case when flowsheet.fs_id = 40010977 then flowsheet_measure.meas_val_num end
        ) as hfov_amplitude_actual,
        max(case when flowsheet.fs_id = 40010999 then flowsheet_measure.meas_val_num end
        ) as hfov_paw_actual_cm_h2o,
        max(case when flowsheet.fs_id = 40002606 then flowsheet_measure.meas_val_num end
        ) as hfjv_pip_set_cm_h2o,
        max(case when flowsheet.fs_id = 40002605 then flowsheet_measure.meas_val_num end
        ) as hfjv_resp_rate_set,
        max(case when flowsheet.fs_id = 40002607 then flowsheet_measure.meas_val_num end
        ) as hfjv_ti,
        max(case when flowsheet.fs_id in (40000695, 40000563) then flowsheet_measure.meas_val_num end
        ) as rr_set_bpm,
        max(case when flowsheet.fs_id = 40000712 then flowsheet_measure.meas_val_num end
        ) as pressure_support_cm_h2o,
        max(case when flowsheet.fs_id in (40010963, 40002731) then flowsheet_measure.meas_val_num end
        ) as ti_sec,
        max(case when flowsheet.fs_id = 40011003 then flowsheet_measure.meas_val_num end
        ) as aprv_pressure_high,
        max(case when flowsheet.fs_id = 40011005 then flowsheet_measure.meas_val_num end
        ) as aprv_pressure_low,
        max(case when flowsheet.fs_id = 40011006 then flowsheet_measure.meas_val_num end
        ) as aprv_time_high,
        max(case when flowsheet.fs_id = 40011007 then flowsheet_measure.meas_val_num end
        ) as aprv_time_low,
        max(case when flowsheet.fs_id = 40069501 then flowsheet_measure.meas_val_num end
        ) as vdr_convective_rate_ordered,
        max(case when flowsheet.fs_id = 40069503 then flowsheet_measure.meas_val_num end
        ) as vdr_percussive_rate_ordered,
        max(case when flowsheet.fs_id = 40069505 then flowsheet_measure.meas_val_num end
        ) as vdr_pip_ordered,
        max(case when flowsheet.fs_id = 4006951811 then flowsheet_measure.meas_val_num end
        ) as vdr_peep_total,
        max(case when flowsheet.fs_id = 40002716 then flowsheet_measure.meas_val_num end) as epap_set_cm_h2o,
        max(case when flowsheet.fs_id = 40002717 then flowsheet_measure.meas_val_num end
        ) as epap_actual_cm_h2o,
        max(case when flowsheet.fs_id = 40002715 then flowsheet_measure.meas_val_num end
        ) as ipap_actual_cm_h2o,
        max(case when flowsheet.fs_id = 40002720 then flowsheet_measure.meas_val end
        ) as interface_type_noninvasive,
        max(case when
            flowsheet_group_lookup.group_id in (
                40002603, -- High Frequency Jet Ventilation (HJFV)
                40002602, -- High Frequency Oscillator Ventilation (HFOV)
                40010962, -- High Frequency Ventilation
                40069500 -- Volumetric Diffusive Respiration (VDR)
            )
            and flowsheet_measure.meas_val_num is not null
            then 1 else 0 end
        ) as high_frequency_ind,
        flowsheet_record.vsi_key
    from
        {{source('cdw', 'visit')}} as visit
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = visit.visit_key
        inner join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
            on visit_stay_info.visit_key = visit.visit_key
        inner join {{source('cdw', 'flowsheet_record')}} as flowsheet_record
            on flowsheet_record.vsi_key = visit_stay_info.vsi_key
        inner join {{source('cdw', 'flowsheet_measure')}} as flowsheet_measure
            on flowsheet_measure.fs_rec_key = flowsheet_record.fs_rec_key
        inner join {{source('cdw', 'flowsheet')}} as flowsheet
            on flowsheet.fs_key = flowsheet_measure.fs_key
        left join {{ref('flowsheet_group_lookup')}} as flowsheet_group_lookup
            on flowsheet_group_lookup.fs_key = flowsheet.fs_key
    where
        flowsheet.fs_id in (
            40002468,
            10,
            40000242,
            40000234,
            40061268,
            40002725,
            9,
            40003569,
            40068126,
            40068114,
            40068115,
            7631,
            7671,
            7745,
            7737,
            40008116,
            7681,
            7679,
            40010942,
            40002718,
            40011001,
            40002559,
            40010984,
            40069509,
            40000699,
            40010947,
            3009165,
            3009166,
            40000715,
            40000705,
            40010978,
            40010977,
            40010999,
            40010946,
            4006280,
            40002606,
            40010971,
            40000564,
            40000566,
            40000695,
            40000712,
            40010963,
            40000702,
            40011003,
            40011005,
            40011006,
            40011007,
            40069501,
            40069503,
            40069505,
            4006951811,
            40002607,
            40002605,
            40002717,
            40002715,
            40002716,
            40002720,
            40000563,
            40002731
        )
    group by
        flowsheet_measure.rec_dt,
        flowsheet_record.fs_rec_id,
        flowsheet_record.vsi_key
)

select
    flowsheet_ventilation_info.recorded_date,
    flowsheet_ventilation_info.flowsheet_record_id,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    visit.hosp_admit_dt as hospital_admit_date,
    visit.hosp_dischrg_dt as hospital_discharge_date,
    flowsheet_ventilation_info.fio2_percent,
    flowsheet_ventilation_info.spo2_percent,
    flowsheet_ventilation_info.resp_o2_device,
    flowsheet_ventilation_info.o2_flow_rate_liters_per_minute,
    flowsheet_ventilation_info.respiratory_within_defined_limits,
    flowsheet_ventilation_info.etco2_mmhg,
    flowsheet_ventilation_info.respirations_per_minute,
    case
        when lower(flowsheet_ventilation_info.pulse_ox_site_rotated) = 'yes'
        then 1
        when lower(flowsheet_ventilation_info.pulse_ox_site_rotated) = 'no ( see comment)'
        then 0
    end as pulse_ox_site_rotated_ind,
    flowsheet_ventilation_info.secretion_suction_location,
    flowsheet_ventilation_info.airway_secretion_amount,
    flowsheet_ventilation_info.airway_secretion_color_and_consistency,
    flowsheet_ventilation_info.barometric_pressure,
    flowsheet_ventilation_info.airway_pressure,
    flowsheet_ventilation_info.minute_volume,
    flowsheet_ventilation_info.tidal_volume,
    flowsheet_ventilation_info.tidal_volume_set,
    flowsheet_ventilation_info.tidal_volume_expired,
    flowsheet_ventilation_info.artificial_airway_type,
    flowsheet_ventilation_info.pip_set_cm_h2o,
    flowsheet_ventilation_info.peep_cpap_set_cm_h2o,
    flowsheet_ventilation_info.pip_cm_h2o,
    flowsheet_ventilation_info.peep_cm_h2o,
    coalesce(
        flowsheet_ventilation_info.map_high_freq,
        flowsheet_ventilation_info.map_vdr,
        flowsheet_ventilation_info.map_non_inv,
        flowsheet_ventilation_info.map_inv
    ) as mean_airway_pressure,
    flowsheet_ventilation_info.invasive_mode,
    flowsheet_ventilation_info.non_invasive_mode,
    case
        when (
            flowsheet_ventilation_info.invasive_mode is not null
            and flowsheet_ventilation_info.non_invasive_mode is null
            )
            or lower(flowsheet_ventilation_info.resp_o2_device) in (
                'ventilation~ invasive', 'invasive ventilation'
            )
            then 1
        else 0
    end as invasive_ind,
    flowsheet_ventilation_info.high_frequency_ind,
    flowsheet_ventilation_info.hfov_amplitude_ordered,
    flowsheet_ventilation_info.hfov_amplitude_actual,
    flowsheet_ventilation_info.hfov_frequency_hz,
    flowsheet_ventilation_info.hfov_paw_ordered_cm_h2o,
    flowsheet_ventilation_info.hfov_paw_actual_cm_h2o,
    flowsheet_ventilation_info.hfjv_pip_set_cm_h2o,
    flowsheet_ventilation_info.hfjv_resp_rate_set,
    flowsheet_ventilation_info.hfjv_ti,
    flowsheet_ventilation_info.rr_set_bpm,
    flowsheet_ventilation_info.pressure_support_cm_h2o,
    flowsheet_ventilation_info.ti_sec,
    flowsheet_ventilation_info.aprv_pressure_high,
    flowsheet_ventilation_info.aprv_pressure_low,
    flowsheet_ventilation_info.aprv_time_high,
    flowsheet_ventilation_info.aprv_time_low,
    flowsheet_ventilation_info.vdr_convective_rate_ordered,
    flowsheet_ventilation_info.vdr_percussive_rate_ordered,
    flowsheet_ventilation_info.vdr_pip_ordered,
    flowsheet_ventilation_info.vdr_peep_total,
    flowsheet_ventilation_info.epap_set_cm_h2o,
    flowsheet_ventilation_info.epap_actual_cm_h2o,
    flowsheet_ventilation_info.ipap_actual_cm_h2o,
    flowsheet_ventilation_info.interface_type_noninvasive,
    flowsheet_ventilation_info.vsi_key,
    stg_patient.pat_key,
    coalesce(anesthesia_encounter_link.visit_key, visit.visit_key) as visit_key,
    anesthesia_encounter_link.anes_visit_key,
    stg_encounter.encounter_key
from
    {{source('cdw', 'visit')}} as visit
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = visit.visit_key
    inner join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
        on visit_stay_info.visit_key = visit.visit_key
    left join {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
        on anesthesia_encounter_link.anes_visit_key = visit.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = visit.pat_key
    inner join flowsheet_ventilation_info
        on flowsheet_ventilation_info.vsi_key = visit_stay_info.vsi_key
where
    coalesce(anesthesia_encounter_link.visit_key, visit.visit_key) > 0
