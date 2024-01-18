with act_setup as (
    select
        stg_act_setup.anes_visit_key,
        stg_act_setup.log_key,
        stg_act_setup.act_value,
        stg_act_setup.act_date,
        stg_act_setup.first_heparin,
        stg_act_setup.last_heparin,
        stg_act_setup.first_protamine,
        stg_act_setup.last_protamine,
        stg_act_setup.first_bypass_start_date,
        stg_act_setup.last_bypass_stop_date,
        stg_act_setup.cpb_ind,
        stg_act_setup.base_order,
        stg_act_setup.post_hep_order,
        stg_act_setup.post_prot_order
    from
        {{ref('stg_act_setup')}} as stg_act_setup
)
select
      cardiac_perfusion_surgery.anes_visit_key,
      cardiac_perfusion_surgery.log_key,
      max(base.act_value) as actbase,
      max(case when act_setup.cpb_ind = 1 then act_setup.act_value else null end) as actmaxcpb,
      min(case when act_setup.cpb_ind = 1 then act_setup.act_value else null end) as actmincpb,
      max(hep.act_value) as actpostheparin,
      max(prota.act_value) as actpostprot
  from
      {{ref('cardiac_perfusion_surgery')}} as cardiac_perfusion_surgery
      inner join act_setup on cardiac_perfusion_surgery.log_key = act_setup.log_key
      inner join act_setup as base on
              cardiac_perfusion_surgery.log_key = base.log_key and base.base_order = 1
      left join act_setup as hep on
              cardiac_perfusion_surgery.log_key = hep.log_key and hep.post_hep_order = 1
      left join act_setup as prota on
              cardiac_perfusion_surgery.log_key = prota.log_key and prota.post_prot_order = 1
group by
      cardiac_perfusion_surgery.anes_visit_key,
      cardiac_perfusion_surgery.log_key
