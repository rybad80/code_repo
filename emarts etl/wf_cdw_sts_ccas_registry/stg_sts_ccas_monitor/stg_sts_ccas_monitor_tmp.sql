select
    or_log.log_key,
    entry_dt,
    cast(((fm.meas_val_num) - 32) * (5 / 9.0) as NUMERIC(4, 1)) as tmp,
    row_number() over (partition by or_log.log_key order by tmp) as tmp_order
from
    {{source('cdw','or_log')}} as or_log
    inner join {{source('cdw','anesthesia_encounter_link')}} as ael on or_log.log_key = ael.or_log_key
    inner join {{source('cdw','visit_stay_info')}} as vsi on ael.anes_visit_key = vsi.visit_key
    inner join {{source('cdw','flowsheet_record')}} as fr on vsi.vsi_key = fr.vsi_key
    inner join {{source('cdw','flowsheet_measure')}} as fm on fr.fs_rec_key = fm.fs_rec_key
    inner join {{source('cdw','flowsheet')}} as f on fm.fs_key = f.fs_key
    inner join {{ ref('stg_sts_ccas_monitor_encs') }} as encs on encs.or_log_key = ael.or_log_key
        and fm.rec_dt between encs.anes_start_tm
        and encs.anes_end_tm
where
    f.fs_id in (7727, 7717, 7715, 7725, 7729, 7723, 7735, 7733, 7719)
    and meas_val_num > 95.0
