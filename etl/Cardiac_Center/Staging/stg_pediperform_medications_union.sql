select
      surgery.log_id,
      3438 as primemedname,
      case when dosing_wt <= 10.00 then 500.0
           when dosing_wt <= 15 then 1000.0
           when dosing_wt <= 25 then 1500.0
           when dosing_wt <= 60 then 2000.0
           when dosing_wt > 60.01 then 5000.0
       end as primemeddose,
       case when dosing_wt <= 10.00 then round(500 / 1000.0, 1)
           when dosing_wt <= 15 then round(1000 / 1000.0, 1)
           when dosing_wt <= 25 then round(1500 / 1000.0, 1)
           when dosing_wt <= 60 then round(2000 / 1000.0, 1)
           when dosing_wt > 60.01 then round(5000 / 1000.0, 1)
        end as primemedvol,
      1 as sort --select *
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      left join {{ref('stg_pediperform_medications_prime_time')}} as stg_pediperform_medications_prime_time
        on surgery.log_key = stg_pediperform_medications_prime_time.log_key
        and stg_pediperform_medications_prime_time.entry_order = 1
      left join {{ref('stg_pediperform_medications_dosing_wt')}} as stg_pediperform_medications_dosing_wt
          on surgery.log_key = stg_pediperform_medications_dosing_wt.log_key

union all

select
       surgery.log_id,
        3462 as primemedname,
        case when blood_prime_vol > 0 then round(5 + dosing_wt, 1)
        end as primemeddose,
        case when blood_prime_vol > 0 then round(5 + dosing_wt, 1)
        end as primemedvol,
        2 as sort
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      left join {{ref('stg_pediperform_medications_prime_time')}} as stg_pediperform_medications_prime_time
          on surgery.log_key = stg_pediperform_medications_prime_time.log_key
          and stg_pediperform_medications_prime_time.entry_order = 1
      left join {{ref('stg_pediperform_medications_dosing_wt')}} as stg_pediperform_medications_dosing_wt
         on surgery.log_key = stg_pediperform_medications_dosing_wt.log_key

union all

select
       surgery.log_id,
        3433 as primemedname,
        round(dosing_wt, 1) as primemeddose,
        round(dosing_wt / 10.0, 1) as primemedvol,
        3 as sort
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      left join {{ref('stg_pediperform_medications_prime_time')}} as stg_pediperform_medications_prime_time
          on surgery.log_key = stg_pediperform_medications_prime_time.log_key
               and stg_pediperform_medications_prime_time.entry_order = 1
      left join {{ref('stg_pediperform_medications_dosing_wt')}} as stg_pediperform_medications_dosing_wt
         on surgery.log_key = stg_pediperform_medications_dosing_wt.log_key

union all


select
       surgery.log_id,
        3418 as primemedname, --25mg/kg up to 80kg (2g dose) ( dose = vol/100)
        case when dosing_wt < 80 then round(dosing_wt * 25, 1)
             when dosing_wt >= 80 then 2000.0
             end as primemeddose,
                case when dosing_wt < 80 then round((dosing_wt * 25) / 100.0, 1)
             when dosing_wt >= 80 then 20.0
             end as primemedvol,
        4 as sort
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      inner join {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
      left join {{ref('stg_pediperform_medications_prime_time')}} as stg_pediperform_medications_prime_time
        on surgery.log_key = stg_pediperform_medications_prime_time.log_key
          and stg_pediperform_medications_prime_time.entry_order = 1
      left join {{ref('stg_pediperform_medications_dosing_wt')}} as stg_pediperform_medications_dosing_wt
          on surgery.log_key = stg_pediperform_medications_dosing_wt.log_key

union all

 select
       surgery.log_id,
        3417 as primemedname, --if blood in prime, 450 mg | dose/100 = 4.5
        case when blood_prime_vol > 0 then 450.0
        end as primemeddose,
        case when blood_prime_vol > 0 then 4.5
        end as primemedvol,
        5 as sort
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      inner join {{ref('cardiac_perfusion_bypass')}} as bypass
        on surgery.anes_visit_key = bypass.visit_key
      left join {{ref('stg_pediperform_medications_prime_time')}} as stg_pediperform_medications_prime_time
          on surgery.log_key = stg_pediperform_medications_prime_time.log_key
            and stg_pediperform_medications_prime_time.entry_order = 1
      left join {{ref('stg_pediperform_medications_dosing_wt')}} as stg_pediperform_medications_dosing_wt
          on surgery.log_key = stg_pediperform_medications_dosing_wt.log_key
