select
    fr.vsi_key,
    fm.occurance,
    fm.meas_val,
    fm.meas_cmt
from
    {{ ref('stg_sts_ccas_monitor_encs') }} as encs
    inner join {{source('cdw','flowsheet_record')}} as fr on encs.hsp_vai_key = fr.vsi_key
    inner join {{source('cdw','flowsheet_measure')}} as fm on fr.fs_rec_key = fm.fs_rec_key
    inner join {{source('cdw','flowsheet')}} as f on fm.fs_key = f.fs_key
group by
    fr.vsi_key,
    fm.occurance,
    fm.meas_val,
    fm.meas_cmt
