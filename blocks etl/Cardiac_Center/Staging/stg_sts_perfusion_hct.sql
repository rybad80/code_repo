with protamine as (
    select
        perf.log_key,
        min(medadmin.action_dt) as protamine_tm
    from
        {{ref('cardiac_perfusion_surgery')}} as perf
        inner join {{source('cdw', 'medication_administration')}} as medadmin
          on medadmin.visit_key = perf.anes_visit_key
        inner join {{source('cdw', 'medication_order')}} as medord
          on medord.med_ord_key = medadmin.med_ord_key
        left join {{source('cdw', 'cdw_dictionary')}} as dict_rslt_key
          on dict_rslt_key.dict_key = medadmin.dict_rslt_key
    where
        lower(med_ord_nm) like '%protamine%'
        and dict_rslt_key.src_id in (105, 102, 122.0020, 6, 103, 1, 106, 112, 117) --given meds)
    group by
        perf.log_key
),
hct as (
    select
        perf.log_key,
        specimen_taken_date as rslt_dt,
        result_value_numeric as hct_value,
        first_bypass_start_date as on_bypass_tm,
        first_circ_arrest_start_date,
        first_cerebral_perfusion_start_date as cperf_start,
        last_bypass_stop_date as off_bypass_tm,
        protamine_tm,
        procord.procedure_order_id as sort
    from
        {{ref('procedure_order_result_clinical')}} as procord
        inner join {{ref('cardiac_perfusion_surgery')}} as perf
          on perf.visit_key = procord.visit_key
        left join {{ref('cardiac_perfusion_circ_arrest')}} as circarrest
          on perf.anes_visit_key = circarrest.visit_key
        left join {{ref('cardiac_perfusion_bypass')}} as bypass
          on bypass.visit_key = perf.anes_visit_key
        left join {{ref('cardiac_perfusion_cerebral_perfusion')}} as acp
           on acp.visit_key = perf.anes_visit_key
        left join protamine on
          protamine.log_key = perf.log_key
          and protamine.protamine_tm > last_bypass_stop_date
    where
        result_component_id in (502952, 123130039)
        and result_value_numeric != 9999999
),
hct_distinct as (
select
    log_key,
    hct_value,
    on_bypass_tm,
    off_bypass_tm,
    first_circ_arrest_start_date,
    cperf_start,
    rslt_dt,
    sort
from
    hct
group by
    log_key,
    hct_value,
    on_bypass_tm,
    off_bypass_tm,
    first_circ_arrest_start_date,
    cperf_start,
    rslt_dt,
    sort
),
hctprior_raw as (
select
    log_key,
    hct_value as hctprior,
    row_number() over(
        partition by log_key
        order by
            (
                coalesce(first_circ_arrest_start_date, cperf_start) - rslt_dt
            ),
            sort desc
    ) as hct_prior_row
from
    hct_distinct
where
    rslt_dt between on_bypass_tm
    and coalesce(first_circ_arrest_start_date, cperf_start)
),

hctprior as (
select
    *
from
    hctprior_raw
where
    hct_prior_row = 1
),
hctfirst_raw as (
select
        log_key,
        hct_value as hctfirst,
        row_number() over(
            partition by log_key
            order by
                (rslt_dt - on_bypass_tm),
                sort
        ) as hct_first_row
    from
        hct_distinct
    where
        rslt_dt between on_bypass_tm
        and off_bypass_tm
),
hctfirst as (
    select
        *
    from
        hctfirst_raw
    where
        hct_first_row = 1
),
hctlast_raw as (
    select
        log_key,
        hct_value as hctlast,
        row_number() over(
            partition by log_key
            order by
                (rslt_dt - off_bypass_tm) desc,
                sort
        ) as hct_last_row
    from
         hct_distinct
    where
        rslt_dt between on_bypass_tm
        and off_bypass_tm
),
hctlast as (
    select
        *
    from
        hctlast_raw
    where
        hct_last_row = 1
),
hctpostprot_raw as (
    select
        log_key,
        hct_value as hctpostprot,
        row_number() over(
            partition by log_key
            order by
                (rslt_dt - protamine_tm),
                sort
        ) as hct_post_prot_row
    from
        hct
    where
        rslt_dt > off_bypass_tm
        and rslt_dt > protamine_tm
),
hctpostprot as (
    select
        *
    from
        hctpostprot_raw
    where
        hct_post_prot_row = 1
)

 select
     perf.log_key,
     hctfirst,
     hctprior,
     hctlast,
     hctpostprot
 from
     chop_analytics..cardiac_perfusion_surgery as perf
    left join hctprior
      on hctprior.log_key = perf.log_key
    left join hctfirst
      on hctfirst.log_key = perf.log_key
    left join hctlast
      on hctlast.log_key = perf.log_key
    left join hctpostprot
      on hctpostprot.log_key = perf.log_key
