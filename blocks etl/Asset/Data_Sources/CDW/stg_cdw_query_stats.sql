select
    stg_cdw_query_inclusion.query_key,
    extract(
        epoch from
        min(
            case when hist_plan_prolog.ismainplan then hist_plan_prolog.submittime end
        ) - stg_cdw_query_inclusion.submit_time_utc
    ) as dist_main_plan_secs,
    round(
        log(
            sum(
                abs(hist_plan_prolog.estimatedcost) -- sometimes value is negative
            ) + 1 -- ensure value is at least 1 so log is positive
        ),
        2
    ) as cost_log10,
    round(log(sum(hist_plan_prolog.estimatedmem) + 1), 2) as memory_used_log10,
    sum(hist_plan_prolog.estimateddisk) as disk_space,
    count(hist_plan_prolog.planid) as n_plans,
    sum(hist_plan_prolog.totalsnippets) as n_snippets,
    sum(case when hist_plan_prolog.ismainplan then 1 else 0 end) as n_main_plan
from
    {{ ref('stg_cdw_query_inclusion') }} as stg_cdw_query_inclusion
    inner join {{ source('histdb', 'hist_plan_prolog') }} as hist_plan_prolog
        using (npsid, npsinstanceid, opid)
group by
    stg_cdw_query_inclusion.query_key,
    stg_cdw_query_inclusion.submit_time_utc
