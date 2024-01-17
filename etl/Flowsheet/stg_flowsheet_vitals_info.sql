select
    flowsheet_measure.rec_dt as recorded_date,
    flowsheet_record.fs_rec_id as flowsheet_record_id,
    max(
        case when flowsheet.fs_id = 14 then (round(flowsheet_measure.meas_val_num * 0.02835, 2)) end
    ) as weight_kg,
    max(case when flowsheet.fs_id = 51157 then flowsheet_measure.meas_val_num end) as weight_change_kg,
    max(
        case when flowsheet.fs_id = 40022107 then (round(flowsheet_measure.meas_val_num * 0.02835, 2)) end
    ) as dosing_weight_kg,
    max(
        case when flowsheet.fs_id = 11 then (round(flowsheet_measure.meas_val_num * 2.54, 2)) end
    ) as height_cm,
    max(
        case when flowsheet.fs_id = 16 then (round(flowsheet_measure.meas_val_num * 2.54, 2)) end
    ) as head_circumference_cm,
    max(case when flowsheet.fs_id = 1006 then flowsheet_measure.meas_val_num end) as bmi,
    max(case when flowsheet.fs_id = 1007 then flowsheet_measure.meas_val_num end) as bsa,
    max(case when flowsheet.fs_id = 8 then flowsheet_measure.meas_val_num end) as pulse,
    max(case when flowsheet.fs_id = 10 then flowsheet_measure.meas_val end) as spo2,
    max(case when flowsheet.fs_id = 9 then flowsheet_measure.meas_val_num end) as respiration,
    max(
        case when flowsheet.fs_id = 6 then (round((flowsheet_measure.meas_val_num - 32) / 1.8, 1)) end
    ) as temperature_c,
    max(
        case when flowsheet.fs_id = 40000303 then flowsheet_measure.meas_val end
    ) as temperature_source,
    max(
        case when flowsheet.fs_id = 5657 then flowsheet_measure.meas_val_num end
    )  as secondary_temperature_c,
    max(
        case when flowsheet.fs_id = 5658 then flowsheet_measure.meas_val end
    )  as secondary_temperature_source,
    max(
        case when flowsheet.fs_id = 1004 then flowsheet_measure.meas_val_num end
    )  as  systolic_blood_pressure_value,
    max(
        case when flowsheet.fs_id = 1005 then flowsheet_measure.meas_val_num end
    )  as diastolic_blood_pressure_value,
    max(
        case when flowsheet.fs_id = 40000244 then flowsheet_measure.meas_val end
    )  as blood_pressure_location_inpatient,
    max(
        case when flowsheet.fs_id = 1003 then flowsheet_measure.meas_val end
    ) as blood_pressure_location_outpatient,
    max(
        case when flowsheet.fs_id = 40000235 then flowsheet_measure.meas_val end
    )  as blood_pressure_cuff_size,
    max(
        case when flowsheet.fs_id = 40000241 then flowsheet_measure.meas_val end
    )  as patient_position,
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
where
    flowsheet.fs_id in (
        6, --temperature_c
        8, --pulse
        9, --respiration
        10, --spo2
        11, --height_cm
        14, --weight
        16, --head circumference
        1003, --blood_pressure_location_outpatient
        1004, --systolic_blood_pressure_value
        1005, --diastolic_blood_pressure_value
        1006, --bmi
        1007, --bsa
        5657, --secondary_temperature_c
        5658, --secondary_temperature_source
        51157, --weight_change_kg
        40000235, --blood_pressure_cuff_size
        40000241, --patient_position
        40000244, --blood_pressure_location_inpatient
        40000303, --temperature_source
        40022107 --dosing weight
    )
group by
    flowsheet_measure.rec_dt,
    flowsheet_record.fs_rec_id,
    flowsheet_record.vsi_key
