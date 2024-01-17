with
lactate_setup as (
    select
        surgery.log_key,
        bypass.first_bypass_start_date,
        bypass.last_bypass_stop_date,
        case
            when (
                perfusion_labs.resultdate between bypass.bypass_start_date_1 and bypass.bypass_stop_date_1
                or perfusion_labs.resultdate between bypass.bypass_start_date_2 and bypass.bypass_stop_date_2
                or perfusion_labs.resultdate between bypass.bypass_start_date_3 and bypass.bypass_stop_date_3
                or perfusion_labs.resultdate between bypass.bypass_start_date_4 and bypass.bypass_stop_date_4
                or perfusion_labs.resultdate between bypass.bypass_start_date_5 and bypass.bypass_stop_date_5
                or perfusion_labs.resultdate between bypass.bypass_start_date_6 and bypass.bypass_stop_date_6
                or perfusion_labs.resultdate between bypass.bypass_start_date_7 and bypass.bypass_stop_date_7
                or perfusion_labs.resultdate between bypass.bypass_start_date_8 and bypass.bypass_stop_date_8
                or perfusion_labs.resultdate between bypass.bypass_start_date_9 and bypass.bypass_stop_date_9
                or perfusion_labs.resultdate between bypass.bypass_start_date_10 and bypass.bypass_stop_date_10
            ) then 1
            else 0
            end as cpb_ind,
        perfusion_labs.resultdate as lactate_date,
        regexp_extract(perfusion_labs.resultvalue, '[\d\.]+')::int as lactate_value,
        row_number() over (partition by surgery.anes_visit_key order by perfusion_labs.resultdate) as room_order,
        case when bypass.bypass_start_date_1 > perfusion_labs.resultdate then row_number() over (
            partition by
                surgery.anes_visit_key,
                (case when bypass.bypass_start_date_1 > perfusion_labs.resultdate then 1 else 0 end)
            order by perfusion_labs.resultdate - bypass.bypass_start_date_1 desc
        ) end as pre_cpb_order,
        case
            when
                perfusion_labs.resultdate > bypass.first_bypass_start_date
                and perfusion_labs.resultdate < bypass.last_bypass_stop_date
                then row_number() over (
                    partition by
                        surgery.anes_visit_key,
                        case
                            when
                                perfusion_labs.resultdate > bypass.first_bypass_start_date
                                and perfusion_labs.resultdate < bypass.last_bypass_stop_date
                                then 1
                            else 0
                            end
                    order by bypass.bypass_start_date_1 - perfusion_labs.resultdate desc
                )
            end as post_cpb_order_first,
        case
            when
                perfusion_labs.resultdate > bypass.first_bypass_start_date
                and perfusion_labs.resultdate < bypass.last_bypass_stop_date
                then row_number() over (
                    partition by
                        surgery.anes_visit_key,
                        case
                            when
                                perfusion_labs.resultdate > bypass.first_bypass_start_date
                                and perfusion_labs.resultdate < bypass.last_bypass_stop_date
                                then 1
                            else 0
                            end
                    order by bypass.bypass_start_date_1 - perfusion_labs.resultdate
            )
            end as post_cpb_order_last,
        case
            when perfusion_labs.resultdate > protamine.last_protamine then row_number() over (
                partition by
                    surgery.anes_visit_key,
                    case when perfusion_labs.resultdate > protamine.last_protamine then 1 else 0 end
                order by perfusion_labs.resultdate - protamine.last_protamine
            ) end as post_prot_order,
        case when perfusion_labs.resultdate > out_room_date then row_number() over (
            partition by
                surgery.anes_visit_key,
                case when perfusion_labs.resultdate > out_room_date then 1 else 0 end
            order by out_room_date - perfusion_labs.resultdate desc
        ) end as postop_order
from
    {{ref('stg_perfusion_labs')}} as perfusion_labs
    inner join
        {{ref('cardiac_perfusion_surgery')}} as surgery on
            perfusion_labs.log_key = surgery.log_key
    inner join
        {{ref('cardiac_perfusion_bypass')}} as bypass on surgery.anes_visit_key = bypass.visit_key
    inner join
        {{ref('surgery_encounter_timestamps')}} as timestamps on timestamps.or_key = surgery.log_key
    left join {{ref('stg_protamine')}} as protamine
        on surgery.log_key = protamine.log_key
where
    perfusion_labs.result_component_name like '%LACTATE%W%B%'
)

select
    lactate_setup.log_key,
    max(case when lactate_setup.room_order = 1 then lactate_setup.lactate_value end) as lactatefirstor,
    max(case when lactate_setup.pre_cpb_order = 1 then lactate_setup.lactate_value end) as lactatelastprecpb,
    max(
        case when lactate_setup.post_cpb_order_first = 1 then lactate_setup.lactate_value end
    ) as lactatefirstoncpb,
    max(case when lactate_setup.post_cpb_order_last = 1 then lactate_setup.lactate_value end) as lactatelastoncpb,
    max(case when lactate_setup.post_prot_order = 1 then lactate_setup.lactate_value end) as lactatepostpro
from
    lactate_setup
group by
    lactate_setup.log_key
