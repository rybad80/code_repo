with cpb_meds_stg as (
select
      perfusion_meds.log_key,
      sum(case when medication_id = 11976 then admin_dose / 1000.0
               when medication_id = 200200082 then admin_dose / 100.0
               when medication_id = 5677 and admin_dose >= 12.5 then 62.5
               when medication_id = 5677 and admin_dose < 12.5 then admin_dose / 0.2
               when medication_id in (145295, 200201029, 135992) then admin_dose / 100.0
         end) as medvol
from
    {{ref('stg_perfusion_meds')}} as perfusion_meds
    inner join {{ref('surgery_encounter_timestamps')}} as surgery_encounter_timestamps on
                perfusion_meds.log_key = surgery_encounter_timestamps.log_key
    inner join {{ref('cardiac_perfusion_bypass')}} as cardiac_perfusion_bypass on
                surgery_encounter_timestamps.mrn = cardiac_perfusion_bypass.mrn
                and (
                    administration_date between bypass_start_date_1 and bypass_stop_date_1
                    or administration_date between bypass_start_date_2 and bypass_stop_date_2
                    or administration_date between bypass_start_date_3 and bypass_stop_date_3
                    or administration_date between bypass_start_date_4 and bypass_stop_date_4
                    or administration_date between bypass_start_date_5 and bypass_stop_date_5
                    or administration_date between bypass_start_date_6 and bypass_stop_date_6
                    or administration_date between bypass_start_date_7 and bypass_stop_date_7
                    or administration_date between bypass_start_date_8 and bypass_stop_date_8
                    or administration_date between bypass_start_date_9 and bypass_stop_date_9
                    or administration_date between bypass_start_date_10 and bypass_stop_date_10
                                                         )
where
      medication_id in (11976, 200200082, 5677, 145295, 200201029, 135992)
group by
      perfusion_meds.log_key
)

select
      log_key,
      round(sum(medvol), 0) as medvoloncpb
 from
      cpb_meds_stg
group by
      log_key
