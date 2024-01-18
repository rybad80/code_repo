select
    log_key,
    entry_date as blood_prime_date,
    sum(meas_val_num) as blood_prime_vol,
    row_number() over (partition by log_key order by entry_date) as entry_order --select *
from
     {{ref('surgery_encounter')}} as surgery_encounter
     inner join {{ref('flowsheet_all')}} as flowsheet_all
        on surgery_encounter.vsi_key = flowsheet_all.vsi_key
where
     flowsheet_id = 500025331 and exists (select vsi_key from {{ref('surgery_encounter')}} )
group by
     log_key,
     entry_date
