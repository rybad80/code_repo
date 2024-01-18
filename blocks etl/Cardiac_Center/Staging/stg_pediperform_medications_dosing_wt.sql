select
      log_key,
      median(cast(meas_val_num / 35.2740 as numeric (6, 2))) as dosing_wt
from
     {{ref('surgery_encounter')}} as surgery_encounter
     inner join {{ref('flowsheet_all')}} as flowsheet_all
         on surgery_encounter.vsi_key = flowsheet_all.vsi_key
where
    flowsheet_id = 40022107 and exists (select vsi_key from {{ref('surgery_encounter')}} )
group by
    log_key
