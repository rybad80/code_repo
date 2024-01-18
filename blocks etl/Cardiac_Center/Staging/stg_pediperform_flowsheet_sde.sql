with perfusion_sde as (
 select
        anes_visit_key,
        log_key,
        concept_id,
        concept_description,
        element_value
 from
      {{ref('cardiac_perfusion_smart_data_element')}}
where
      concept_id in ('CHOP#2865', 'CHOP#2855')
),

perfusion_flowsheet as (
  select
          anes_visit_key,
          mrn,
          log_key,
          flowsheet_name,
          flowsheet_id,
          recorded_date,
          meas_val,
          meas_val_num
    from
         {{ref('cardiac_perfusion_flowsheet')}}
   where
         (flowsheet_name in ('Cell Svr Product Vol', 'Cell Saver Volume Given', 'Concentrate Prime')
         or flowsheet_id in (
             112700040,
             1120100044,
             112700034,
             112700041,
             112700043,
             112700021,
             112700023,
             1120090107,
             112700031,
             112700037,
             1120090108,
             1120090109,
             40001428,
             40001432,
             40001456,
             40001452,
             40001440,
             40000248,
             112700029,
             40003207
                         )
         )

),

cellsaver as (
 select
         perfusion_flowsheet.anes_visit_key,
         perfusion_flowsheet.mrn,
         perfusion_flowsheet.log_key,
         sum(case when flowsheet_id = 1120100044 then perfusion_flowsheet.meas_val_num end) as cellsavergiven,
         sum(case when flowsheet_id = 112700029 then perfusion_flowsheet.meas_val_num end) as cellsavertaken
 from
     perfusion_flowsheet
     inner join
         {{ref('cardiac_perfusion_bypass')}} as cardiac_perfusion_bypass on
             perfusion_flowsheet.anes_visit_key = cardiac_perfusion_bypass.visit_key
            and (
                recorded_date between bypass_start_date_1 and bypass_stop_date_1
                or recorded_date between bypass_start_date_2 and bypass_stop_date_2
                or recorded_date between bypass_start_date_3 and bypass_stop_date_3
                or recorded_date between bypass_start_date_4 and bypass_stop_date_4
                or recorded_date between bypass_start_date_5 and bypass_stop_date_5
                or recorded_date between bypass_start_date_6 and bypass_stop_date_6
                or recorded_date between bypass_start_date_7 and bypass_stop_date_7
                or recorded_date between bypass_start_date_8 and bypass_stop_date_8
                or recorded_date between bypass_start_date_9 and bypass_stop_date_9
                or recorded_date between bypass_start_date_10 and bypass_stop_date_10
                    )

 where
     flowsheet_name in ('Cell Svr Product Vol', 'Cell Saver Volume Given')
group by
      perfusion_flowsheet.anes_visit_key,
      perfusion_flowsheet.mrn,
      perfusion_flowsheet.log_key

),

postcpbvol as (
select
     perfusion_flowsheet.anes_visit_key,
     perfusion_flowsheet.mrn,
     perfusion_flowsheet.log_key,
     sum(case when flowsheet_id = 112700029 then perfusion_flowsheet.meas_val_num end) as cellsavpostcpb,
     sum(case when flowsheet_id = 112700034 then perfusion_flowsheet.meas_val_num end) as concirc
from
    perfusion_flowsheet
     inner join
         {{ref('cardiac_perfusion_bypass')}} as cardiac_perfusion_bypass on
             perfusion_flowsheet.anes_visit_key = cardiac_perfusion_bypass.visit_key
                                                        and recorded_date > last_bypass_stop_date
where
    flowsheet_id in (112700029, 112700034)
group by
      perfusion_flowsheet.anes_visit_key,
      perfusion_flowsheet.mrn,
      perfusion_flowsheet.log_key
),

plasmalyte_cpb as (
select
      log_key,
      sum(meas_val_num) as plasmalyte_cpb
from
      perfusion_flowsheet
       inner join {{ref('cardiac_perfusion_bypass')}} as cardiac_perfusion_bypass
        on perfusion_flowsheet.anes_visit_key = cardiac_perfusion_bypass.visit_key
            and (recorded_date between bypass_start_date_1 and bypass_stop_date_1
                 or recorded_date between bypass_start_date_2 and bypass_stop_date_2
                 or recorded_date between bypass_start_date_3 and bypass_stop_date_3
                 or recorded_date between bypass_start_date_4 and bypass_stop_date_4
                 or recorded_date between bypass_start_date_5 and bypass_stop_date_5
                 or recorded_date between bypass_start_date_6 and bypass_stop_date_6
                 or recorded_date between bypass_start_date_7 and bypass_stop_date_7
                 or recorded_date between bypass_start_date_8 and bypass_stop_date_8
                 or recorded_date between bypass_start_date_9 and bypass_stop_date_9
                 or recorded_date between bypass_start_date_10 and bypass_stop_date_10)
    where
      flowsheet_id = 40003207
group by
      log_key
),

plasmalyte_muf as (
select
      log_key,
      sum(meas_val_num) as plasmalyte_muf
from
      perfusion_flowsheet
       inner join {{ref('cardiac_perfusion_muf')}} as cardiac_perfusion_muf
        on perfusion_flowsheet.anes_visit_key = cardiac_perfusion_muf.visit_key
            and recorded_date between coalesce(muf_start_date_5, muf_start_date_4,
                             muf_start_date_3, muf_start_date_2, muf_start_date_1)
                and last_muf_stop_date + interval('5 minute')
    where
      flowsheet_id = 40003207
group by
      log_key
),


perfusion_events as (
select
       cardiac_perfusion_surgery.log_key,
       min(case when evttype.event_id = 112700005 then event_dt end) as cool_start,
       max(case when evttype.event_id = 112700006 then event_dt end) as cool_stop,
       sum(case when evttype.event_id = 112700054 then 1 else null end) as cperf_count,
       min(case when evttype.event_id = 112700017 then event_dt end) as ultrafil_start,
       max(case when evttype.event_id = 112700017 then 1 else null end) as ultrafil_ind,
       max(case when evttype.event_id = 112700018 then event_dt end) as ultrafil_stop,
       min(case when evttype.event_id = 112700011 then event_dt end) as muf_start,
       max(case when evttype.event_id = 112700011 then 1 else null end) as muf_ind,
       max(case when evttype.event_id = 112700012 then event_dt end) as muf_stop,
       min(case when evttype.event_id = 112700009 then event_dt end) as indfib_start,
       max(case when evttype.event_id = 112700010 then event_dt end) as indfib_stop
from
       {{ref('cardiac_perfusion_surgery')}} as cardiac_perfusion_surgery
       inner join
           {{source('cdw', 'visit_ed_event')}} as evt on evt.visit_key = cardiac_perfusion_surgery.anes_visit_key
       inner join {{source('cdw', 'master_event_type')}} as evttype on evt.event_type_key = evttype.event_type_key
where
       evttype.event_id in (
           112700005,
           112700006,
           112700013,
           112700014,
           112700054,
           112700055,
           112700017,
           112700018,
           112700011,
           112700012,
           112700009,
           112700010
       )
group by
       cardiac_perfusion_surgery.log_key
),

rap as (
select
      anes_visit_key,
      sum(meas_val_num) as autocircprimevol
  from
      perfusion_flowsheet
 where
      flowsheet_id = 112700040
group by
      anes_visit_key
),

muf as (
select
      anes_visit_key,
      sum(meas_val_num) as modultrafiltvolrem
  from
      perfusion_flowsheet
 where
      flowsheet_id = 112700037
group by
      anes_visit_key
),

uf as (
select
      anes_visit_key,
      max(3303) as ultrafilt,
      sum(meas_val_num) as ultrafiltvol
  from
      perfusion_flowsheet
 where
      flowsheet_id = 112700041
group by
      anes_visit_key
),

venous_sequestor as (
 select
      anes_visit_key,
      sum(meas_val_num) as autoharvvol
  from
      perfusion_flowsheet
 where
      flowsheet_id = 112700043
group by
      anes_visit_key
),

ac_vc_size_raw as (
select
     log_key,
     substring(element_value, 1, instr(element_value, ' x ', 1) - 1) as artlinesz_raw,
     substring(
         element_value,
         instr(element_value, ' x ', 1) + 3,
         length(element_value) - instr(element_value, ' x ', 1) + 2
     ) as venlinesz_raw
from
    perfusion_sde
where
     concept_id = 'CHOP#2865'
),

ac_vc_size as (
select
     log_key,
     artlinesz_raw as artlinesz,
     venlinesz_raw as venlinesz
from
    ac_vc_size_raw
),

oxygenator as (
select
     log_key,
     element_value as oxygenatorty
from
    perfusion_sde
where
     concept_id = 'CHOP#2855'
),

circuit as (
select
     surgery.log_key,
     ac_vc_size.artlinesz,
     ac_vc_size.venlinesz,
     oxygenator.oxygenatorty

 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      inner join
          {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
      inner join ac_vc_size on ac_vc_size.log_key = surgery.log_key
      inner join oxygenator on oxygenator.log_key = surgery.log_key
),

plasmalyte_prime as (
select
      surgery.log_key,
      case when circuit.oxygenatorty = 'FX05' and artlinesz = '1/8' and venlinesz = '3/16' then 115
           when circuit.oxygenatorty = 'FX05' and artlinesz = '3/16' and venlinesz = '3/16' then 135
           when circuit.oxygenatorty = 'FX05' and artlinesz = '3/16' and venlinesz = '1/4' then 155
           when circuit.oxygenatorty = 'FX05' and artlinesz = '1/4' and venlinesz = '1/4' then 175
           when circuit.oxygenatorty = 'FX15' and artlinesz = '1/4' and venlinesz = '3/8' then 340
           when circuit.oxygenatorty = 'FX15' and artlinesz = '3/8' and venlinesz = '3/8' then 650
           when circuit.oxygenatorty = 'FX25' and artlinesz = '3/8' and venlinesz = '3/8' then 800
           when circuit.oxygenatorty = 'FX25' and artlinesz = '3/8' and venlinesz = '1/2' then 950
      end as primefluidvol
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      inner join
          {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
      inner join circuit on circuit.log_key = surgery.log_key
),

perfusion_temps as (
select
     log_key,
     recorded_date,
     (meas_val_num - 32) * (5.0 / 9.0) as ventemp
 from
     perfusion_flowsheet
where
     flowsheet_id = 112700023
),


high_low_temp as (
select
     log_key,
     max(case when cardiac_perfusion_bypass.visit_key is not null
               and cardiac_perfusion_cerebral_perfusion.visit_key is null
               and cardiac_perfusion_circ_arrest.visit_key is null
               and flowsheet_id = 112700021
               then 1.0 * (meas_val_num - 32) * (5.0 / 9.0) else null end) as arttemphigh,
     min(case when flowsheet_id = 112700023
              then 1.0 * (meas_val_num - 32) * (5.0 / 9.0) else null end) as lwsttemp,
     3397 as lwsttempsrc
 from
     perfusion_flowsheet
     left join {{ref('cardiac_perfusion_bypass')}} as cardiac_perfusion_bypass
     on perfusion_flowsheet.anes_visit_key = cardiac_perfusion_bypass.visit_key
            and (recorded_date between bypass_start_date_1 and bypass_stop_date_1
                 or recorded_date between bypass_start_date_2 and bypass_stop_date_2
                 or recorded_date between bypass_start_date_3 and bypass_stop_date_3
                 or recorded_date between bypass_start_date_4 and bypass_stop_date_4
                 or recorded_date between bypass_start_date_5 and bypass_stop_date_5
                 or recorded_date between bypass_start_date_6 and bypass_stop_date_6
                 or recorded_date between bypass_start_date_7 and bypass_stop_date_7
                 or recorded_date between bypass_start_date_8 and bypass_stop_date_8
                 or recorded_date between bypass_start_date_9 and bypass_stop_date_9
                 or recorded_date between bypass_start_date_10 and bypass_stop_date_10)
     left join {{ref('cardiac_perfusion_cerebral_perfusion')}} as cardiac_perfusion_cerebral_perfusion
     on perfusion_flowsheet.anes_visit_key = cardiac_perfusion_cerebral_perfusion.visit_key
            and (recorded_date between cerebral_perfusion_start_date_1 and cerebral_perfusion_stop_date_1
                 or recorded_date between cerebral_perfusion_start_date_2 and cerebral_perfusion_stop_date_2
                 or recorded_date between cerebral_perfusion_start_date_3 and cerebral_perfusion_stop_date_3)
     left join {{ref('cardiac_perfusion_circ_arrest')}} as cardiac_perfusion_circ_arrest
     on perfusion_flowsheet.anes_visit_key = cardiac_perfusion_circ_arrest.visit_key
            and (recorded_date between circ_arrest_start_date_1 and circ_arrest_stop_date_1
                 or recorded_date between circ_arrest_start_date_2 and circ_arrest_stop_date_2
                 or recorded_date between circ_arrest_start_date_3 and circ_arrest_stop_date_3
                 or recorded_date between circ_arrest_start_date_4 and circ_arrest_stop_date_4
                 or recorded_date between circ_arrest_start_date_5 and circ_arrest_stop_date_5)
where
     flowsheet_id in (112700021, 112700023)
     and meas_val_num > 0
group by
     log_key
),

relative_temps as (
select
      surgery.log_key,
      recorded_date,
      ventemp,
      row_number() over (partition by surgery.log_key order by recorded_date - cool_start) as temp_order
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      inner join
          {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
      left join perfusion_events on perfusion_events.log_key = surgery.log_key
      left join perfusion_temps on perfusion_temps.log_key = surgery.log_key
                and perfusion_events.cool_start is not null
                and perfusion_events.cool_start + time('00:02') <= perfusion_temps.recorded_date

),

cpbseptemp as (
select
      surgery.log_key,
      recorded_date,
      ventemp,
      case when bypass.last_bypass_stop_date = perfusion_temps.recorded_date then 1
           when bypass.last_bypass_stop_date = perfusion_temps.recorded_date + time('00:01') then 2
           when bypass.last_bypass_stop_date = perfusion_temps.recorded_date + time('00:02') then 3
           else 0 end as cpb_sep_temp_ind
 from
      {{ref('cardiac_perfusion_surgery')}} as surgery
      inner join
          {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
      left join perfusion_events on perfusion_events.log_key = surgery.log_key
      left join perfusion_temps on perfusion_temps.log_key = surgery.log_key
),

urine as (
select
      anes_visit_key,
      sum(meas_val_num) as cpburinevol
from
    perfusion_flowsheet
    inner join
        {{ref('cardiac_perfusion_bypass')}} as cardiac_perfusion_bypass on
            perfusion_flowsheet.anes_visit_key = cardiac_perfusion_bypass.visit_key
            and (recorded_date between bypass_start_date_1 and bypass_stop_date_1
                 or recorded_date between bypass_start_date_2 and bypass_stop_date_2
                 or recorded_date between bypass_start_date_3 and bypass_stop_date_3
                 or recorded_date between bypass_start_date_4 and bypass_stop_date_4
                 or recorded_date between bypass_start_date_5 and bypass_stop_date_5
                 or recorded_date between bypass_start_date_6 and bypass_stop_date_6
                 or recorded_date between bypass_start_date_7 and bypass_stop_date_7
                 or recorded_date between bypass_start_date_8 and bypass_stop_date_8
                 or recorded_date between bypass_start_date_9 and bypass_stop_date_9
                 or recorded_date between bypass_start_date_10 and bypass_stop_date_10)
where
    flowsheet_id = 40000248
group by
     anes_visit_key
),

blood_admin as (
select
        or_log.log_key,
        surgery.anes_visit_key,
        recorded_date,
        first_bypass_start_date,
        description,
        case when description like '%IN%PRIME%' then 1 else 0 end as prime_ind,
        replace(order_proc.display_name, 'Transfusion Order: ', '') as blood_product_type,
        case when lower(order_proc.display_name) like '%packed%red%cells%' then 'PRBC'
             when lower(order_proc.display_name) like '%platelets%' then 'PLATELETS'
             when lower(order_proc.display_name) like '%fresh%frozen%plasma%' then 'FFP'
             when lower(order_proc.display_name) like '%cryoprecipitate%' then 'CRYO'
             when lower(order_proc.display_name) like '%whole%blood%' then 'WB'
             else blood_product_type end as blood_product_category,
        blood_start_instant,
        blood_end_instant,
        flowsheet_all.meas_val_num as blood_vol,
        blood_product_code
 from
    {{ref('cardiac_perfusion_surgery')}} as surgery
    inner join {{ref('cardiac_perfusion_bypass')}} as bypass
         on bypass.visit_key = surgery.anes_visit_key
    inner join {{ref('surgery_encounter_timestamps')}} as timestamps
         on surgery.log_key = timestamps.or_key
    inner join {{source('cdw', 'or_log')}} as or_log on surgery.log_key = or_log.log_key
    inner join {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
         on anesthesia_encounter_link.or_log_key = or_log.log_key
    inner join {{source('cdw', 'visit')}} as anes_enc
         on anes_enc.visit_key =  anesthesia_encounter_link.anes_visit_key
    inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
         on procedure_order_clinical.visit_key = anesthesia_encounter_link.visit_key
    inner join {{source('cdw', 'visit_addl_info')}} as visit_addl_info
         on visit_addl_info.visit_key = anesthesia_encounter_link.visit_key
    inner join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
         on visit_stay_info.vsi_key = visit_addl_info.vsi_key
    inner join {{source('cdw', 'visit_stay_info_rows')}} as visit_stay_info_rows
         on visit_stay_info_rows.vsi_key = visit_stay_info.vsi_key
    inner join {{source('cdw', 'visit_stay_info_rows_order')}} as visit_stay_info_rows_order
         on visit_stay_info_rows_order.vsi_key = visit_stay_info_rows.vsi_key
         and visit_stay_info_rows.seq_num = visit_stay_info_rows_order.seq_num
         and visit_stay_info_rows_order.ord_key = procedure_order_clinical.proc_ord_key
    inner join {{source('clarity_ods', 'ord_blood_admin')}} as ord_blood_admin
         on procedure_order_clinical.procedure_order_id = ord_blood_admin.order_id
    inner join {{source('clarity_ods', 'order_proc')}} as order_proc
         on ord_blood_admin.order_id = order_proc.order_proc_id
    inner join {{ref('flowsheet_all')}} as flowsheet_all
         on flowsheet_all.vsi_key = visit_stay_info_rows.vsi_key
         and visit_stay_info_rows.seq_num = flowsheet_all.occurance
where
    procedure_order_type = 'Child Order'
    and flowsheet_id = 500025331
    and recorded_date between in_room_date and out_room_date
),

prime_cpb_blood as (

select
    blood_admin.log_key,
    sum(case
            when prime_ind = 1
                and blood_product_category = 'PRBC' then (blood_vol)
            else 0
        end) as primerbcvol,
    sum(case
            when prime_ind = 1
                and blood_product_category = 'FFP' then (blood_vol)
            else 0
        end) as primeffpvol,
    sum(case
            when prime_ind = 1
                and blood_product_category = 'PLATELETS' then (blood_vol)
            else 0
        end) as primeplatvol,
    sum(case
            when prime_ind = 1
                and blood_product_category = 'CRYO' then (blood_vol)
            else 0
        end) as primecryovol,
    sum(case
            when prime_ind = 1
                and blood_product_category = 'WB' then (blood_vol)
            else 0
        end) as primewholebloodvol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is not null
                and prime_ind = 0
                and blood_product_category = 'PRBC' then (blood_vol)
            else 0
        end) as cpbrbcvol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is not null
                and prime_ind = 0
                and blood_product_category = 'FFP' then (blood_vol)
            else 0
        end) as cpbffpvol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is not null
                and prime_ind = 0
                and blood_product_category = 'PLATELETS' then (blood_vol)
            else 0
        end) as cpbplatvol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is not null
                and prime_ind = 0
                and blood_product_category = 'CRYO' then (blood_vol)
            else 0
        end) as cpbcryovol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is not null
                and prime_ind = 0
                and blood_product_category = 'WB' then (blood_vol)
            else 0
        end) as cpbwholebloodvol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is null
                and prime_ind = 0
                and blood_product_category = 'PRBC' then (blood_vol)
            else 0
        end) as noncpbrbcvol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is null
                and prime_ind = 0
                and blood_product_category = 'FFP' then (blood_vol)
            else 0
        end) as noncpbffpvol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is null
                and prime_ind = 0
                and blood_product_category = 'PLATELETS' then (blood_vol)
            else 0
        end) as noncpbplatvol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is null
                and prime_ind = 0
                and blood_product_category = 'CRYO' then (blood_vol)
            else 0
        end) as noncpbcryovol,
    sum(case
            when cardiac_perfusion_bypass.visit_key is null
                and prime_ind = 0
                and blood_product_category = 'WB' then (blood_vol)
            else 0
        end) as noncpbwholebloodvol

from
     blood_admin
     left join
         {{ref('cardiac_perfusion_bypass')}} as cardiac_perfusion_bypass on
             blood_admin.anes_visit_key = cardiac_perfusion_bypass.visit_key
            and (recorded_date between bypass_start_date_1 and bypass_stop_date_1
                 or recorded_date between bypass_start_date_2 and bypass_stop_date_2
                 or recorded_date between bypass_start_date_3 and bypass_stop_date_3
                 or recorded_date between bypass_start_date_4 and bypass_stop_date_4
                 or recorded_date between bypass_start_date_5 and bypass_stop_date_5
                 or recorded_date between bypass_start_date_6 and bypass_stop_date_6
                 or recorded_date between bypass_start_date_7 and bypass_stop_date_7
                 or recorded_date between bypass_start_date_8 and bypass_stop_date_8
                 or recorded_date between bypass_start_date_9 and bypass_stop_date_9
                 or recorded_date between bypass_start_date_10 and bypass_stop_date_10)
group by
      blood_admin.log_key
),

chesttube as (
select
      surgery.log_key,
      cast(sum(meas_val_num) as integer) as chesttubeoutlt24
from
    {{ref('flowsheet_all')}} as flowsheet_all
    inner join
        {{ref('cardiac_perfusion_surgery')}} as surgery on flowsheet_all.pat_key = surgery.pat_key
    inner join
        {{ref('surgery_encounter_timestamps')}} as timestamps on timestamps.or_key = surgery.log_key
where
     flowsheet_id = 40072339
     and recorded_date between out_room_date and out_room_date + ('23:59:59')
group by
     surgery.log_key
),

death_sde as (
select
    sdei.pat_key,
    max(
        case when concept_id = 'CHOPIP#007' then sdev.elem_val end
    ) over (partition by sdei.pat_key) as date_of_death,
    max(
        case
            when
                concept_id = 'CHOPIP#008' then regexp_replace(
                    sdev.elem_val, '[' || chr(58) || '-' || chr(255) || ']', ' '
                )
        end
    )
                                    over (partition by sdei.pat_key) as time_of_death
from
    {{source('cdw', 'smart_data_element_info')}} as sdei
    inner join {{source('cdw', 'smart_data_element_value')}} as sdev on sdev.sde_key = sdei.sde_key
    inner join {{source('cdw', 'clinical_concept')}} as clinical_concept
       on clinical_concept.concept_key = sdei.concept_key
where
    concept_id in ('CHOPIP#010', 'CHOPIP#007', 'CHOPIP#008')
),

death as (
select distinct

      pat_key,
      cast('1840-12-31' as timestamp)
      + cast(
          date_of_death as float)
      + cast(time_of_death as float) * interval '1 second' as death_dttm
from
    death_sde
),

non_bypass_leftside as (
select
     or_log.log_key,
     --select *
     max(
         case when surgery_procedure.surgery_date < cardiac_surgery.surg_date then 1 else 0 end
     ) as prior_leftsidethoracotomy
from
    {{ref('cardiac_surgery')}} as cardiac_surgery
    inner join
        {{ref('surgery_procedure')}} as surgery_procedure on
            cardiac_surgery.mrn = surgery_procedure.mrn
    inner join
        {{source('cdw', 'registry_sts_surgery')}} as registry_sts_surgery on
            cardiac_surgery.cardiac_study_id = registry_sts_surgery.r_surg_key
    inner join {{source('cdw', 'or_log')}} as or_log on
            or_log.log_id = registry_sts_surgery.caselinknum
where
    lower(or_procedure_name) in ('pda closure', 'coarctation repair', 'pda closure, surgical',
    'coarctation repair, end to end',
    'vascular ring repair',
    'vascular ring repair, innominate artery compression repair'
)
group by
     or_log.log_key
),

bypass_ct_procs as (
select
      cardiac_surgery.log_key,
      max(case when cpb_mn is null then 1 else 0 end) as prior_noncpb,
      max(case when coalesce(cpb_mn, 0) > 0 then 1 else 0 end) as prior_cpb --select *
from
     {{ref('cardiac_surgery')}} as cardiac_surgery
     inner join {{ref('cardiac_surgery')}} as priorsurg on
             cardiac_surgery.mrn = priorsurg.mrn and date(cardiac_surgery.surg_date) > date(priorsurg.surg_date)
     left join {{source('cdw', 'registry_sts_surgery')}} as registry_sts_surgery on
             cardiac_surgery.cardiac_study_id = registry_sts_surgery.r_surg_key
     left join {{source('cdw', 'registry_sts_surgery_perfusion')}} as registry_sts_surgery_perfusion on
             registry_sts_surgery.r_surg_key = registry_sts_surgery_perfusion.r_surg_key
     left join {{source('cdw', 'or_log')}} as or_log on
             (
                 or_log.log_id = registry_sts_surgery.caselinknum
             ) or (
                 or_log.pat_key = cardiac_surgery.pat_key and date(
                     or_log.surg_dt_key
                 ) = date(registry_sts_surgery.r_surg_dt)
             )
where
     or_log.surg_dt_key > 0
group by
      cardiac_surgery.log_key
)


select
      surgery.log_id,

      case when prior_cpb = 1 then 3483
           when prior_noncpb = 1 then 3485
           when prior_leftsidethoracotomy = 1 then 3484
           else 3487 end as reop,
      coalesce(cellsavergiven, 0) * 2 as atscollvol,
      coalesce(cellsavergiven, 0) as atsretvol,
      case when venlinesz = '1/2' then 3481
           when artlinesz in ('1/8', '3/16', '1/4') then 3478
           when artlinesz = '3/8' then 3479
           else 3482 end as pumpbootsz,
      case when artlinesz = '1/8' then 5427
          when artlinesz = '3/16' then 5428
          when artlinesz = '1/4' then 5429
          when artlinesz = '3/8' then 5430
          when artlinesz = '5/16' then 5431
          when artlinesz = '1/2' then 5432
          else 5433 end as artlinesz,
     case when venlinesz = '1/8' then 3327
          when venlinesz = '3/16' then 3328
          when venlinesz = '1/4' then 3329
          when venlinesz = '3/8' then 3330
          when venlinesz = '5/16' then 3331
          when venlinesz = '1/2' then 3332
          else 3334 end as venlinesz,
      case when oxygenatorty = 'FX05' then 3579
          when oxygenatorty = 'FX15' then 3580
          when oxygenatorty = 'FX25' then 3581
          end as oxygenatorty,
      primefluidvol,
      primefluidvol + 50 as primevol,
      case when coalesce(primerbcvol, 0) > 0 then 3475 else 3473 end as primerbctreat,
       round(arttemphigh, 1) as arttemphigh,
       round(lwsttemp, 1) as lwsttemp,
      lwsttempsrc,
      coalesce(postcpbvol.concirc, postcpbvol.cellsavpostcpb, 0) as residprocessreturn,
      round(coalesce(cpbseptemp3.ventemp, cpbseptemp2.ventemp, cpbseptemp1.ventemp), 1) as cpbseptemp,
      case when round(lwsttemp, 1) <= 35 then 3409
           when round(lwsttemp, 1) > 35 then 3407
           else null end as phmgmt,
      3304 as phstatwarm,
      case when cool_start is not null or cool_stop is not null then 3303
           else 3304 end as phstatcool,
      case
          when
              cool_start is not null or cool_stop is not null then round(
                  relative_temps.ventemp, 1
              )
           else null end as phstatcoolthresh,
      case when rap.anes_visit_key is not null then 3303 else 3304 end as autocirc,
      case when rap.anes_visit_key is not null then autocircprimevol else null end as autocircprimevol,
      case when venous_sequestor.anes_visit_key is not null
           then 3303 else 3304 end as autoharv,
      coalesce(autoharvvol, 0) as autoharvvol,
      case when coalesce(cpbcryovol, 0) + coalesce(cpbcryovol, 0) + coalesce(cpbffpvol, 0)
           + coalesce(cpbplatvol, 0) + coalesce(cpbrbcvol, 0) + coalesce(cpbwholebloodvol, 0)
           + coalesce(noncpbcryovol, 0) + coalesce(noncpbffpvol, 0) + coalesce(noncpbplatvol, 0)
           + coalesce(noncpbrbcvol, 0) + coalesce(noncpbwholebloodvol, 0) + coalesce(primecryovol, 0)
           + coalesce(primeffpvol, 0) + coalesce(primeplatvol, 0) + coalesce(primerbcvol, 0)
           + coalesce(primewholebloodvol, 0) > 0
           then 3303
           else 3304
      end as bloodprodused,
      coalesce(cpbcryovol, 0) as cpbcryovol,
      coalesce(cpbffpvol, 0) as cpbffpvol,
      coalesce(cpbplatvol, 0) as cpbplatvol,
      coalesce(cpbrbcvol, 0) as cpbrbcvol,
      coalesce(cpbwholebloodvol, 0) as cpbwholebloodvol,
      coalesce(noncpbcryovol, 0) as noncpbcryovol,
      coalesce(noncpbffpvol, 0) as noncpbffpvol,
      coalesce(noncpbplatvol, 0) as noncpbplatvol,
      coalesce(noncpbrbcvol, 0) as noncpbrbcvol,
      coalesce(noncpbwholebloodvol, 0) as noncpbwholebloodvol,
      coalesce(primecryovol, 0) as primecryovol,
      coalesce(primeffpvol, 0) as primeffpvol,
      coalesce(primeplatvol, 0) as primeplatvol,
      coalesce(primerbcvol, 0) as primerbcvol,
      coalesce(primewholebloodvol, 0) as primewholebloodvol,
      case when muf_ind = 1 then 3303 else 3304 end as modultrafilt,
      case when muf_ind = 1 then extract(epoch from muf_stop - muf_start) / 60
           else null end as modultrafilttm,
      case when muf_ind = 1
           then coalesce(modultrafiltvolrem, primefluidvol + 50) else null end as modultrafiltvolrem,
      coalesce(primefluidvol, 0) as residpumpvol,
      ultrafilt,
      ultrafiltvol,
      case when muf_ind = 1 then 3398 else null end as modultrafiltty,
      coalesce(cpburinevol, 0) as cpburinevol,
      coalesce(plasmalyte_cpb, 0) as plasmalyte_cpb,
      coalesce(plasmalyte_muf, 0) as plasmalyte_muf,
      coalesce(plasmalyte_cpb + primefluidvol + 50 + plasmalyte_muf, 0) as crysvol,
      case when death.death_dttm between in_room_date and out_room_date then 3303 else 3304 end as intraopdeath,
      coalesce(chesttubeoutlt24, 0) as chesttubeoutlt24

 from
      {{ref('cardiac_perfusion_surgery')}} as perfusion
      inner join {{ref('surgery_encounter')}} as surgery
                on perfusion.log_key = surgery.log_key
      inner join {{ref('cardiac_perfusion_bypass')}} as bypass
                on perfusion.anes_visit_key = bypass.visit_key
      inner join {{ref('surgery_encounter_timestamps')}} as timestamps
                on surgery.log_key = timestamps.or_key
      left join non_bypass_leftside on non_bypass_leftside.log_key = surgery.log_key
      left join bypass_ct_procs on bypass_ct_procs.log_key = surgery.log_key
      left join cellsaver on cellsaver.log_key = perfusion.log_key
      left join ac_vc_size on ac_vc_size.log_key = surgery.log_key
      left join oxygenator on oxygenator.log_key = surgery.log_key
      left join high_low_temp on high_low_temp.log_key = surgery.log_key
      left join perfusion_events on perfusion_events.log_key = surgery.log_key
      left join relative_temps on relative_temps.log_key = surgery.log_key and temp_order = 1
      left join cpbseptemp as cpbseptemp1
                on cpbseptemp1.log_key = surgery.log_key
                and cpbseptemp1.cpb_sep_temp_ind = 1
      left join cpbseptemp as cpbseptemp2
                on cpbseptemp2.log_key = surgery.log_key
                and cpbseptemp2.cpb_sep_temp_ind = 2
      left join cpbseptemp as cpbseptemp3
                on cpbseptemp3.log_key = surgery.log_key
                and cpbseptemp3.cpb_sep_temp_ind = 3
      left join rap on rap.anes_visit_key = perfusion.anes_visit_key
      left join muf on muf.anes_visit_key = perfusion.anes_visit_key
      left join uf on uf.anes_visit_key = perfusion.anes_visit_key
      left join venous_sequestor on venous_sequestor.anes_visit_key = perfusion.anes_visit_key
      left join prime_cpb_blood on prime_cpb_blood.log_key = surgery.log_key
      left join urine on urine.anes_visit_key = perfusion.anes_visit_key
      left join death on death.pat_key = surgery.pat_key
      left join plasmalyte_prime on plasmalyte_prime.log_key = surgery.log_key
      left join plasmalyte_cpb on plasmalyte_cpb.log_key = surgery.log_key
      left join plasmalyte_muf on plasmalyte_muf.log_key = surgery.log_key
      left join chesttube on chesttube.log_key = surgery.log_key
      left join postcpbvol on postcpbvol.anes_visit_key = perfusion.anes_visit_key
where
     date(surgery.surgery_date) >= '2021-10-01'
