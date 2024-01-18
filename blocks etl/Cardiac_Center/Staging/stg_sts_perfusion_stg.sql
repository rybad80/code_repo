with perfusion_flowsheet as (
select
    *
from
    {{ref('cardiac_perfusion_flowsheet')}}
where
    flowsheet_id in (
        7727,
        112700021,
        40001452,
        40001440,
        40001456,
        500025331,
        1120090107,
        112700031,
        1120090108
            )
),
tmp as (
    select distinct
        or_log.log_key,
        first_value(cast(((meas_val_num) - 32) * (5 / 9.0) as numeric(4, 1)) ignore nulls)
        over (partition by or_log.log_key order by meas_val_num
        rows between unbounded preceding and unbounded following) as tmp
    from
        {{source('cdw', 'or_log')}} as or_log
        inner join {{source('cdw', 'anesthesia_encounter_link')}} as ael
          on or_log.log_key = ael.or_log_key
        inner join {{source('cdw', 'visit_stay_info')}} as vsi
          on ael.anes_visit_key = vsi.visit_key
        inner join perfusion_flowsheet as flowsheet_all
          on flowsheet_all.vsi_key = vsi.vsi_key
        inner join {{ref('cardiac_perfusion_surgery')}} as perf
          on perf.log_key = ael.or_log_key
        inner join {{ref('cardiac_perfusion_bypass')}} as bypass
          on perf.anes_visit_key = bypass.visit_key
        and (
            recorded_date between bypass_start_date_1
            and bypass_stop_date_1
            or recorded_date between bypass_start_date_2
            and bypass_stop_date_2
            or recorded_date between bypass_start_date_3
            and bypass_stop_date_3
            or recorded_date between bypass_start_date_4
            and bypass_stop_date_4
            or recorded_date between bypass_start_date_5
            and bypass_stop_date_5
            or recorded_date between bypass_start_date_6
            and bypass_stop_date_6
        )
    where
        flowsheet_id = 7727
        and meas_val_num > 60.0
),
event_times_setup as (
    select
        or_case.log_key,
        sum(
            case
                when evttype.event_id = 112700054 then 1
                else null
            end
        ) as cperf_count,
        min(
            case
                when evttype.event_id = 112700017 then event_dt
            end
        ) as ultrafil_start,
        max(
            case
                when evttype.event_id = 112700017 then 1
                else null
            end
        ) as ultrafil_ind,
        max(
            case
                when evttype.event_id = 112700018 then event_dt
            end
        ) as ultrafil_stop,
        min(
            case
                when evttype.event_id = 112700011 then event_dt
            end
        ) as muf_start,
        max(
            case
                when evttype.event_id = 112700011 then 1
                else null
            end
        ) as muf_ind,
        max(
            case
                when evttype.event_id = 112700012 then event_dt
            end
        ) as muf_stop,
        min(
            case
                when evttype.event_id = 112700009 then event_dt
            end
        ) as indfib_start,
        max(
            case
                when evttype.event_id = 112700010 then event_dt
            end
        ) as indfib_stop
    from
        {{ref('cardiac_perfusion_surgery')}} as perf
        inner join {{source('cdw', 'or_log')}} as or_log
            on perf.log_key = or_log.log_key
        inner join {{source('cdw', 'or_case')}} as or_case
            on or_case.log_key = or_log.log_key
        inner join {{source('cdw', 'anesthesia_encounter_link')}} as aneslink
            on aneslink.or_case_key = or_case.or_case_key
        inner join {{source('cdw', 'visit_ed_event')}} as evt
            on evt.visit_key = aneslink.anes_visit_key
        inner join {{source('cdw', 'master_event_type')}} as evttype
            on evt.event_type_key = evttype.event_type_key
    where
        evttype.event_id in (
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
        or_case.log_key
),
event_times as (
    select
        log_key,
        cperf_count,
        ultrafil_ind,
        muf_ind,
        coalesce(
            extract(
                epoch
                from
                    ultrafil_stop - ultrafil_start
            ) / 60,
            0
        ) as ultrafiltime,
        coalesce(
            extract(
                epoch
                from
                    muf_stop - muf_start
            ) / 60,
            0
        ) as muftime,
        coalesce(
            extract(
                epoch
                from
                    indfib_stop - indfib_start
            ) / 60,
            0
        ) as indfibtime
    from
        event_times_setup
),
art_temp as (
    select
        or_log.log_key,
        cast(avg(((meas_val_num) - 32) * (5 / 9.0)) as integer) as avg_art_temp
    from
        {{source('cdw', 'or_log')}} as or_log
        inner join {{source('cdw', 'anesthesia_encounter_link')}} as ael
          on or_log.log_key = ael.or_log_key
        inner join {{source('cdw', 'visit_stay_info')}} as vsi
          on ael.anes_visit_key = vsi.visit_key
        inner join perfusion_flowsheet as flowsheet_all
          on flowsheet_all.vsi_key = vsi.vsi_key
        inner join event_times
          on event_times.log_key = or_log.log_key
        inner join {{ref('cardiac_perfusion_surgery')}} as perf
          on perf.log_key = ael.or_log_key
        inner join {{ref('cardiac_perfusion_cerebral_perfusion')}} as cperf
          on perf.anes_visit_key = cperf.visit_key
        and (
            recorded_date between cerebral_perfusion_start_date_1
            and cerebral_perfusion_stop_date_1
            or recorded_date between cerebral_perfusion_start_date_2
            and cerebral_perfusion_stop_date_2
            or recorded_date between cerebral_perfusion_start_date_3
            and cerebral_perfusion_stop_date_3
        )
    where
        flowsheet_id = 112700021
        and meas_val_num > 60.0
    group by
        or_log.log_key
),
cplegia as (
    select
        perf.log_key,
        count(medadmin.dose) as cplegiadose
    from
        {{ref('cardiac_perfusion_surgery')}} as perf
        inner join {{source('cdw', 'medication_administration')}} as medadmin
          on medadmin.visit_key = perf.anes_visit_key
        inner join {{source('cdw', 'medication_order')}} as medord
          on medord.med_ord_key = medadmin.med_ord_key
        left join {{source('cdw', 'cdw_dictionary')}} as dict_rslt_key
          on dict_rslt_key.dict_key = medadmin.dict_rslt_key
    where
        med_ord_nm = 'cardioplegia soln'
        and dict_rslt_key.src_id in (105, 102, 122.0020, 6, 103, 1, 106, 112, 117) --given meds
    group by
        perf.log_key
),
prime as (
    select
        c.or_log_key,
        case
            when flowsheet_id = 1120090107 then recorded_date
        end as prbc_prime_entry_dt,
        case
            when flowsheet_id = 112700031 then recorded_date
        end as ffp_prime_entry_dt,
        case
            when flowsheet_id = 1120090108 then recorded_date
        end as wb_prime_entry_dt
    from
        {{source('cdw', 'anesthesia_encounter_link')}} as c
        inner join {{ref('cardiac_perfusion_surgery')}} as perf on c.or_log_key = perf.log_key
        inner join {{source('cdw', 'visit_stay_info')}} as vsi on c.anes_visit_key = vsi.visit_key
        inner join perfusion_flowsheet as flowsheet_all on flowsheet_all.vsi_key = vsi.vsi_key
    where
        flowsheet_id in (1120090107, 112700031, 1120090108)
        and lower(meas_val) = 'in prime'
),
vol as (
select
    c.or_log_key,
    case
        when flowsheet_id = 40001452 then recorded_date
    end as prbc_val_entry_dt,
    case
        when flowsheet_id = 40001440 then recorded_date
    end as ffp_val_entry_dt,
    case
        when flowsheet_id = 40001456 then recorded_date
    end as wb_val_entry_dt,
    case
        when flowsheet_id = 40001452 then meas_val_num
    end as prbc_val,
    case
        when flowsheet_id = 40001440 then meas_val_num
    end as ffp_val,
    case
        when flowsheet_id = 40001456 then meas_val_num
    end as wb_val
from
    {{source('cdw', 'anesthesia_encounter_link')}} as c
    inner join {{ref('cardiac_perfusion_surgery')}} as perf on c.or_log_key = perf.log_key
    inner join {{source('cdw', 'visit_stay_info')}} as vsi on c.anes_visit_key = vsi.visit_key
    inner join perfusion_flowsheet as flowsheet_all on flowsheet_all.vsi_key = vsi.vsi_key
where
    flowsheet_id in (40001452, 40001440, 40001456)
),
blood as (
    select
        prime.or_log_key,
        max(1) as prime,
        sum(vol.prbc_val) as prbc,
        sum(vol.ffp_val) as ffp,
        sum(vol.wb_val) as wholeblood
    from
        prime
        inner join vol on prime.or_log_key = vol.or_log_key
        and (
            prbc_prime_entry_dt = prbc_val_entry_dt
            or ffp_prime_entry_dt = ffp_val_entry_dt
            or wb_prime_entry_dt = wb_val_entry_dt
            )
    group by
        prime.or_log_key
),
blood_admin as (
    select
        surgery.log_key,
        surgery.anes_visit_key,
        recorded_date,
        first_bypass_start_date,
        order_description as description,
        prime_ind,
        blood_product_type,
        case
            when lower(blood_product_type) like '%packed%red%cells%' then 'prbc'
            when lower(blood_product_type) like '%platelets%' then 'platelets'
            when lower(blood_product_type) like '%fresh%frozen%plasma%' then 'ffp'
            when lower(blood_product_type) like '%cryoprecipitate%' then 'cryo'
            when lower(blood_product_type) like '%whole%blood%' then 'wb'
            else blood_product_type
        end as blood_product_category,
        blood_admin_start_date as blood_start_instant,
        blood_admin_end_date as blood_end_instant,
        blood_volume as blood_vol,
        blood_product_code --select *
    from
        {{ref('cardiac_perfusion_surgery')}} as surgery
        inner join {{ref('cardiac_perfusion_bypass')}} as bypass
          on bypass.visit_key = surgery.anes_visit_key
        inner join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
          on visit_stay_info.visit_key = surgery.visit_key
        inner join {{ref('blood_product_administration')}} as bpam
          on visit_stay_info.vsi_key = bpam.vsi_key
    where
        procedure_id in (
            129642,
            500200703,
            500200704,
            500200705,
            500200707,
            81295
        )
),
prime_cpb_blood as (
    select
        log_key,
        sum(
            case
                when blood_product_category = 'prbc' then (blood_vol)
                else 0
            end
        ) as prbc,
        sum(
            case
                when blood_product_category = 'ffp' then (blood_vol)
                else 0
            end
        ) as ffp,
        sum(
            case
                when blood_product_category = 'wb' then (blood_vol)
                else 0
            end
        ) as wholeblood
    from
        blood_admin
    where
        prime_ind = 1
    group by
        log_key
)

select
    or_log.log_id,
    or_log.log_key,
    perf.anes_visit_key,
    total_bypass_minutes as cpbtm,
    coalesce(total_cross_clamp_minutes, 0) as xclamptm,
    coalesce(total_circ_arrest_minutes, 0) as dhcatm,
    2 as tempsitebla,
    null as lowctmpbla,
    2 as tempsiteeso,
    null as lowctmpeso,
    1 as tempsitenas,
    cast(tmp.tmp as numeric(8, 2)) as lowctmpnas,
    2 as tempsiterec,
    null as lowctmprec,
    2 as tempsitetym,
    null as lowctmptym,
    2 as tempsiteoth,
    null as lowctmpoth,
    coalesce(total_rewarm_minutes, 0) as rewarmtime,
    case
        when total_cerebral_perfusion_minutes > 0 then 1
        else 2
    end as cperfutil,
    total_cerebral_perfusion_minutes as cperftime,
    case
        when total_cerebral_perfusion_minutes > 0 then 1
        else null
    end as cperfcaninn,
    case
        when total_cerebral_perfusion_minutes > 0 then 2
        else null
    end as cperfcanrsub,
    case
        when total_cerebral_perfusion_minutes > 0 then 2
        else null
    end as cperfcanrax,
    case
        when total_cerebral_perfusion_minutes > 0 then 2
        else null
    end as cperfcanrcar,
    case
        when total_cerebral_perfusion_minutes > 0 then 2
        else null
    end as cperfcanlcar,
    case
        when total_cerebral_perfusion_minutes > 0 then 2
        else null
    end as cperfcansvc,
    cperf_count as cperfper,
    case
        when total_cerebral_perfusion_minutes > 0 then 50
        else null
    end as cperfflow,
    case
        when total_cerebral_perfusion_minutes > 0 then avg_art_temp
        else null
    end as cperftemp,
    cplegiadose,
    case
        when cplegiadose is not null then 2845
        else null
    end as cplegsol,
    null as inflwoccltm,
    case
        when total_cerebral_perfusion_minutes > 0 then 1370
        else null
    end as cerebralflowtype,
    coalesce(blood.prime, 2) as cpbprimed,
    case
        when cplegiadose is not null then 2842
        else 2841
    end as cplegiadeliv,
    case
        when cplegiadose is not null then 2855
        else null
    end as cplegiatype,
    coalesce(blood.prbc, prime_cpb_blood.prbc, 0) as prbc,
    coalesce(blood.ffp, prime_cpb_blood.ffp, 0) as ffp,
    coalesce(blood.wholeblood, prime_cpb_blood.wholeblood, 0) as wholeblood,
    case
        when indfibtime > 0 then 1
        else 2
    end as inducedfib,
    case
        when indfibtime > 0 then indfibtime
        else null
    end as inducedfibtmmin,
    case
        when indfibtime > 0 then 0
        else null
    end as inducedfibtmsec,
    case
        when coalesce(ultrafil_ind, 0) > 0
        or coalesce(muf_ind, 0) > 0 then 1
        else 2
    end as ultrafilperform,
    case
        when coalesce(ultrafil_ind, 0) > 0
        and coalesce(muf_ind, 0) > 0 then 5283
        when coalesce(ultrafil_ind, 0) = 0
        and coalesce(muf_ind, 0) > 0 then 5282
        when coalesce(ultrafil_ind, 0) > 0
        and coalesce(muf_ind, 0) = 0 then 5281
        else null
    end as ultrafilperfwhen,
    25 as anticoagused,
    1 as anticoagunfhep,
    2 as anticoagarg,
    2 as anticoagbival,
    2 as anticoagoth,
    cast(perf.height_cm as numeric(5, 1)) as heightcm,
    cast(perf.weight_kg as numeric(5, 1)) as weightkg --select *
from
    {{ref('cardiac_perfusion_surgery')}} as perf
    left join {{source('cdw','or_log')}} as or_log
      on perf.log_key = or_log.log_key
    left join {{ref('cardiac_perfusion_circ_arrest')}} as circarrest
      on perf.anes_visit_key = circarrest.visit_key
    left join {{ref('cardiac_perfusion_cerebral_perfusion')}} as acp
      on perf.anes_visit_key = acp.visit_key
    left join {{ref('cardiac_perfusion_rewarm')}} as rewarm
      on perf.anes_visit_key = rewarm.visit_key
    left join {{ref('cardiac_perfusion_bypass')}} as bypass
      on perf.anes_visit_key = bypass.visit_key
    left join {{ref('cardiac_perfusion_cross_clamp')}} as xclamp
      on perf.anes_visit_key = xclamp.visit_key
    left join tmp
      on tmp.log_key = perf.log_key
    left join event_times as evttm
      on evttm.log_key = perf.log_key
    left join blood
      on blood.or_log_key = perf.log_key
    left join art_temp
      on perf.log_key = art_temp.log_key
    left join cplegia
      on perf.log_key = cplegia.log_key
    left join prime_cpb_blood
      on prime_cpb_blood.log_key = perf.log_key
where
    first_bypass_start_date is not null
    and or_log.log_id > 0
