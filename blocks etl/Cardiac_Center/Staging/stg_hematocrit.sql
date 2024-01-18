with hematocrit_setup as (
select
    surgery.anes_visit_key,
    surgery.log_key,
    bypass.first_bypass_start_date,
    bypass.last_bypass_stop_date,
    case when (resultdate between bypass_start_date_1 and bypass_stop_date_1
               or resultdate between bypass_start_date_2 and bypass_stop_date_2
                or resultdate between bypass_start_date_3 and bypass_stop_date_3
                or resultdate between bypass_start_date_4 and bypass_stop_date_4
                or resultdate between bypass_start_date_5 and bypass_stop_date_5
                or resultdate between bypass_start_date_6 and bypass_stop_date_6
                or resultdate between bypass_start_date_7 and bypass_stop_date_7
                or resultdate between bypass_start_date_8 and bypass_stop_date_8
                or resultdate between bypass_start_date_9 and bypass_stop_date_9
                or resultdate between bypass_start_date_10 and bypass_stop_date_10
         ) then 1 else 0 end as cpb_ind,
    resultdate as hct_date,
    case
        when
            length(
                trim(both ' ' from (regexp_replace(resultvalue, '[' || chr(58) || '-'
                || chr(255) || ']', '')))
            ) = 0
        then null
        else cast(regexp_replace(trim(both ' ' from resultvalue), '[' || chr(58) || '-'
                || chr(255) || ']', '') as int) end as hct_value,
    row_number() over (partition by surgery.anes_visit_key order by resultdate) as room_order,
    case when bypass_start_date_1 > resultdate then row_number() over (partition by surgery.anes_visit_key,
                                         (case when bypass_start_date_1 > resultdate
                                               then 1 else 0 end )
                                         order by resultdate - bypass_start_date_1 desc)
        end as pre_cpb_order,
    case
        when
            resultdate > first_bypass_start_date and resultdate < last_bypass_stop_date
        then row_number() over (
                partition by surgery.anes_visit_key,
                                         (case when resultdate > first_bypass_start_date
                                                    and resultdate < last_bypass_stop_date
                                               then 1 else 0 end )
                                         order by bypass_start_date_1 - resultdate desc)
        end as post_cpb_order_first,
    case
        when
            resultdate > first_bypass_start_date and resultdate < last_bypass_stop_date
        then row_number() over (
                partition by surgery.anes_visit_key,
                                         (case when resultdate > first_bypass_start_date
                                                    and resultdate < last_bypass_stop_date
                                               then 1 else 0 end )
                                         order by bypass_start_date_1 - resultdate)
        end as post_cpb_order_last,
    case when resultdate > last_protamine
         then row_number() over (partition by surgery.anes_visit_key,
                                           (case when resultdate > last_protamine
                                                 then 1 else 0 end )
                                           order by resultdate - last_protamine)
        end as post_prot_order,
    case when resultdate between last_bypass_stop_date + interval '30 minutes' and out_room_date
         then row_number() over (partition by surgery.anes_visit_key,
                           (case when resultdate between last_bypass_stop_date + interval '30 minutes'
                                                and out_room_date
                                 then 1 else 0 end )
                           order by resultdate - (last_bypass_stop_date + interval '30 minutes'))
        end as post_cpboff_plus30_order,
    case when resultdate > out_room_date
         then row_number() over (partition by surgery.anes_visit_key,
                                         (case when resultdate > out_room_date
                                               then 1 else 0 end )
                                         order by out_room_date - resultdate desc)
        end as postop_order
from
    {{ref('stg_perfusion_labs')}} as perfusion_labs
    inner join
        {{ref('cardiac_perfusion_surgery')}} as surgery on
            perfusion_labs.log_key = surgery.log_key
    inner join
        {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
    inner join
        {{ref('surgery_encounter_timestamps')}} as timestamps on timestamps.or_key = surgery.log_key
    left join {{ref('stg_protamine')}} as protamine on surgery.log_key = protamine.log_key
where
    result_component_name in ('HCT, CARDIAC OR ISTAT, POC', 'HCT, ISTAT8')
)

select
      cardiac_perfusion_surgery.anes_visit_key,
      cardiac_perfusion_surgery.log_key,
      max(hematocrit_room.hct_value) as hctbase,
      max(pre_cpb_order.hct_value) as hctlastprecpb,
      max(post_cpb_order.hct_value) as hctfirst,
      min(case when hematocrit_setup.cpb_ind = 1 then hematocrit_setup.hct_value else null end) as lwsthct,
      max(lastone.hct_value) as hctlast,
      coalesce(max(postpro.hct_value), max(postcpb_plus30.hct_value)) as hctpostpro,
      max(postop.hct_value) as hctfirsticu
  from
      {{ref('cardiac_perfusion_surgery')}} as cardiac_perfusion_surgery
      inner join
          hematocrit_setup on
              cardiac_perfusion_surgery.log_key = hematocrit_setup.log_key
      left join
          hematocrit_setup as hematocrit_room on
              cardiac_perfusion_surgery.log_key = hematocrit_room.log_key
              and hematocrit_room.room_order = 1
      left join
          hematocrit_setup as pre_cpb_order on
              cardiac_perfusion_surgery.log_key = pre_cpb_order.log_key
              and pre_cpb_order.pre_cpb_order = 1
      left join
          hematocrit_setup as post_cpb_order on
              cardiac_perfusion_surgery.log_key = post_cpb_order.log_key
              and post_cpb_order.post_cpb_order_first = 1
      left join
          hematocrit_setup as lastone on
              cardiac_perfusion_surgery.log_key = lastone.log_key
              and lastone.post_cpb_order_last = 1
      left join
          hematocrit_setup as postpro on
              cardiac_perfusion_surgery.log_key = postpro.log_key
              and postpro.post_prot_order = 1
      left join
          hematocrit_setup as postop on
              cardiac_perfusion_surgery.log_key = postop.log_key
              and postop.postop_order = 1
      left join
          hematocrit_setup as postcpb_plus30 on
              cardiac_perfusion_surgery.log_key = postcpb_plus30.log_key
              and postcpb_plus30.post_cpboff_plus30_order = 1
group by
      cardiac_perfusion_surgery.anes_visit_key,
      cardiac_perfusion_surgery.log_key
