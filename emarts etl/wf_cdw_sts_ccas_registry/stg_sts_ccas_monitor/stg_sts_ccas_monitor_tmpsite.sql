select
    or_log.log_key,
    vai.vsi_key,
    entry_dt,
    case
        when upper(flow_mea.meas_val) = 'AXILLARY' then 414
        when upper(flow_mea.meas_val) = 'ORAL' then 410
        when upper(flow_mea.meas_val) = 'RECTAL' then 413
        when upper(flow_mea.meas_val) = 'ESOPHAGEAL PROBE' then 411
        when upper(flow_mea.meas_val) = 'TEMPORAL' then 409
        when upper(flow_mea.meas_val) = 'BLADDER PROBE' then 412
        when upper(flow_mea.meas_val) = 'CORE TEMPERATURE (ECMO, SWAN GANZ)' then 2895
    end as tmp_site,
    row_number() over (partition by vai.vsi_key order by entry_dt, rec_dt) as tmp_site_order
from
    {{source('cdw','or_log')}} as or_log
    inner join {{source('cdw','anesthesia_encounter_link')}} as ael on or_log.log_key = ael.or_log_key
    inner join {{source('cdw','visit_addl_info')}} as vai on vai.visit_key = ael.visit_key
    inner join {{source('cdw','flowsheet_record')}} as flow_rec on flow_rec.vsi_key = vai.vsi_key
    inner join {{source('cdw','flowsheet_measure')}} as flow_mea on flow_mea.fs_rec_key = flow_rec.fs_rec_key
    inner join {{source('cdw','flowsheet')}} as flow on flow.fs_key = flow_mea.fs_key
    inner join {{ ref('stg_sts_ccas_monitor_encs') }} as encs on encs.or_log_key = ael.or_log_key
        and flow_mea.rec_dt between encs.anes_start_tm
        and encs.anes_end_tm
where
    flow.fs_id in (40000303)
    and meas_val is not null
