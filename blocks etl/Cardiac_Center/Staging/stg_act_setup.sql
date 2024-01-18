select
       cardiac_perfusion_surgery.anes_visit_key,
       cardiac_perfusion_surgery.log_key,
       meas_val_num as act_value,
       recorded_date as act_date,
       first_heparin,
       last_heparin,
       first_protamine,
       last_protamine,
       first_bypass_start_date,
       last_bypass_stop_date,
       case when (recorded_date between bypass_start_date_1 and bypass_stop_date_1
                 or recorded_date between bypass_start_date_2 and bypass_stop_date_2
                 or recorded_date between bypass_start_date_3 and bypass_stop_date_3
                 or recorded_date between bypass_start_date_4 and bypass_stop_date_4
                 or recorded_date between bypass_start_date_5 and bypass_stop_date_5
                 or recorded_date between bypass_start_date_6 and bypass_stop_date_6
                 or recorded_date between bypass_start_date_7 and bypass_stop_date_7
                 or recorded_date between bypass_start_date_8 and bypass_stop_date_8
                 or recorded_date between bypass_start_date_9 and bypass_stop_date_9
                 or recorded_date between bypass_start_date_10 and bypass_stop_date_10
                 ) then 1 else 0 end as cpb_ind,
       row_number() over (
           partition by cardiac_perfusion_surgery.anes_visit_key order by recorded_date
       ) as base_order,
       case
           when
               recorded_date > first_heparin then row_number() over (
                   partition by cardiac_perfusion_surgery.anes_visit_key,
                    (
                        case
                            when
                                recorded_date > first_heparin then 1
                            else 0
                        end
                    )
                    order by recorded_date - first_heparin) end as post_hep_order,
       case
           when
               recorded_date > last_protamine then row_number() over (
                   partition by cardiac_perfusion_surgery.anes_visit_key,
                            (
                                case
                                    when
                                        recorded_date > last_protamine then 1
                                    else 0
                                end
                            )
                            order by recorded_date - last_protamine) end as post_prot_order
     from
        {{ref('cardiac_perfusion_surgery')}} as cardiac_perfusion_surgery
        inner join {{ref('cardiac_perfusion_flowsheet')}} as cardiac_perfusion_flowsheet on
            cardiac_perfusion_surgery.anes_visit_key = cardiac_perfusion_flowsheet.anes_visit_key
        left join {{ref('cardiac_perfusion_bypass')}} as bypass on
            cardiac_perfusion_surgery.anes_visit_key = bypass.visit_key
        left join {{ref('stg_heparin')}} as heparin on
            cardiac_perfusion_surgery.log_key = heparin.log_key
        left join {{ref('stg_protamine')}} as protamine on
            cardiac_perfusion_surgery.log_key = protamine.log_key
    where
        flowsheet_id in ('9028')
