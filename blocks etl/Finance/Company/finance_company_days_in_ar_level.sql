/*      finance_company_days_in_ar_level block –
display days in ar as fiscal periods close at the
company or higher levels in the company hieararchy
*/
with gather_days_in_ar as (
    select
        finance_company_level.*,
        finance_company_days_in_ar.metric_date_key,
        finance_company_days_in_ar.monthly_asset_balance,
        finance_company_days_in_ar.avg_net_revenue_per_day,
        finance_company_days_in_ar.days_in_ar as metric_value_for_company_period,
        finance_company_days_in_ar.rolling_net_patient_revenue,
        finance_company_days_in_ar.days_for_average,
        finance_company_fiscal_period_status.company_period_earliest_close_ind,
        finance_company_fiscal_period_status.company_period_close_not_yet_ind,
        finance_company_fiscal_period_status.company_period_not_current_closed_ind
    from
        {{ref('finance_company_level')}} as finance_company_level
    inner join
        {{ref('finance_company_days_in_ar')}} as finance_company_days_in_ar
            on finance_company_level.company_id = finance_company_days_in_ar.company_id
            and (finance_company_days_in_ar.rolling_month_range = 12)
    inner join
        {{ref('finance_company_fiscal_period_status')}} as finance_company_fiscal_period_status
            on finance_company_level.company_id = finance_company_fiscal_period_status.company_id
            and finance_company_days_in_ar.metric_date_key = finance_company_fiscal_period_status.fiscal_end_dt_key
    order by
        finance_company_level.level_1_name,
        finance_company_level.level_2_name,
        finance_company_level.level_3_name,
        finance_company_days_in_ar.company_id
),

max_month_with_close as (
    select
        max(metric_date_key) as show_metric_to_this_date
    from
        gather_days_in_ar
    where
        company_period_earliest_close_ind = 1
),

level_1_metric_rollup as (
    select
        company_hierarchy_level_1_wid as layer_wid,
        level_1_name,
        null as level_2_name,
        null as level_3_name,
        null as level_4_name,
        1 as company_level,
        null as company_id,
        level_1_name as dimension_name,
        metric_date_key,
        null as metric_value_for_company_period,
        sum(monthly_asset_balance)
        / (sum(rolling_net_patient_revenue)
            / max(days_for_average)
        ) as calc,
        sum(monthly_asset_balance) as rollup_monthly_asset_balance,
        sum(rolling_net_patient_revenue) as rollup_rolling_net_patient_revenue,
        max(days_for_average) as days_for_average,
        sum(company_period_close_not_yet_ind) as over_zero_dont_show_level
    from
        gather_days_in_ar
    where
        company_hierarchy_level_1_wid is not null
    group by
        company_hierarchy_level_1_wid,
        level_1_name,
        dimension_name,
        metric_date_key
),

level_2_metric_rollup as (
    select
        company_hierarchy_level_2_wid as layer_wid,
        level_1_name,
        level_2_name,
        null as level_3_name,
        null as level_4_name,
        2 as company_level,
        null as company_id,
        level_2_name as dimension_name,
        metric_date_key,
        null as metric_value_for_company_period,
        sum(monthly_asset_balance)
            / (sum(rolling_net_patient_revenue)
            / max(days_for_average)
        ) as calc,
        sum(monthly_asset_balance) as rollup_monthly_asset_balance,
        sum(rolling_net_patient_revenue) as rollup_rolling_net_patient_revenue,
        max(days_for_average) as days_for_average,
        sum(company_period_close_not_yet_ind) as over_zero_dont_show_level
    from
        gather_days_in_ar
    where
        company_hierarchy_level_2_wid is not null
    group by
        company_hierarchy_level_2_wid,
        level_1_name,
        level_2_name,
        dimension_name,
        metric_date_key
),

level_3_metric_rollup as (
    select
        company_hierarchy_level_3_wid as layer_wid,
        level_1_name,
        level_2_name,
        level_3_name,
        null as level_4_name,
        3 as company_level,
        null as company_id,
        parent_hierarchy_name as dimension_name,
        metric_date_key,
        null as metric_value_for_company_period,
        sum(monthly_asset_balance)
            / (sum(rolling_net_patient_revenue)
                / max(days_for_average)
        ) as calc,
        sum(monthly_asset_balance) as lvl3_monthly_asset_balance,
        sum(rolling_net_patient_revenue) as lvl3_rolling_net_patient_revenue,
        max(days_for_average) as days_for_average,
        sum(company_period_close_not_yet_ind) as over_zero_dont_show_level
    from
        gather_days_in_ar
    where
        company_hierarchy_level_3_wid is not null
    group by
        company_hierarchy_level_3_wid,
        level_1_name,
        level_2_name,
        level_3_name,
        dimension_name,
        metric_date_key
),

all_levels as (
    select
        company_wid as layer_wid,
        level_1_name,
        level_2_name,
        level_3_name,
        company_name as level_4_name,
        4 as company_level,
        company_id,
        company_name as dimension_name,
        metric_date_key,
        metric_value_for_company_period,
        case when company_period_earliest_close_ind = 1
            then monthly_asset_balance / (rolling_net_patient_revenue / days_for_average)
            else cast(null as double precision)
            end as calc,
        case when company_period_earliest_close_ind = 1
            then monthly_asset_balance
            else cast(null as numeric(38, 2))
            end as monthly_asset_balance,
        case when company_period_earliest_close_ind = 1
            then rolling_net_patient_revenue
            else cast(null as numeric(38, 2))
            end as rolling_net_patient_revenue,
        days_for_average
    from
            gather_days_in_ar
    group by
            company_wid,
            level_1_name,
            level_2_name,
            level_3_name,
            company_id,
            dimension_name,
            metric_date_key,
            metric_value_for_company_period,
            monthly_asset_balance,
            rolling_net_patient_revenue,
            days_for_average,
            company_period_earliest_close_ind
    union
    /* level 3 of the company hierarachy rollups - consolidateds */
    select
        layer_wid,
        level_1_name,
        level_2_name,
        level_3_name,
        level_4_name,
        company_level,
        company_id,
        dimension_name,
        metric_date_key,
        metric_value_for_company_period,
        case when over_zero_dont_show_level > 0
            then cast(null as integer)
            else calc
            end as calc,
        case when over_zero_dont_show_level > 0
            then cast(null as integer)
            else lvl3_monthly_asset_balance
            end as lvl3_monthly_asset_balance,
        case when over_zero_dont_show_level > 0
            then cast(null as integer)
            else lvl3_rolling_net_patient_revenue
            end as lvl3_rolling_net_patient_revenue,
        days_for_average
    from level_3_metric_rollup
    union
    /* level 2 of the company hierarachy rollups
    - 'obligated group'  or 'practice plans' */
    select
        layer_wid,
        level_1_name,
        level_2_name,
        level_3_name,
        level_4_name,
        company_level,
        company_id,
        dimension_name,
        metric_date_key,
        metric_value_for_company_period,
        case when over_zero_dont_show_level > 0
            then cast(null as integer)
            else calc
            end as calc,
        case when over_zero_dont_show_level > 0
            then cast(null as integer)
            else rollup_monthly_asset_balance
            end as lvl2_monthly_asset_balance,
        case when over_zero_dont_show_level > 0
            then cast(null as integer)
            else rollup_rolling_net_patient_revenue
            end as lvl2_rolling_net_patient_revenue,
        days_for_average
    from level_2_metric_rollup
    union
    /* level 1 of the company hierarachy rollups
    - chop as a whole */
    select
        layer_wid,
        level_1_name,
        level_2_name,
        level_3_name,
        level_4_name,
        company_level,
        company_id,
        dimension_name,
        metric_date_key,
        metric_value_for_company_period,
        case when over_zero_dont_show_level > 0
            then cast(null as integer)
            else calc
            end as calc,
        case when over_zero_dont_show_level > 0
            then cast(null as integer)
            else rollup_monthly_asset_balance
            end as lvl2_monthly_asset_balance,
        case when over_zero_dont_show_level > 0
            then cast(null as integer)
            else rollup_rolling_net_patient_revenue
            end as lvl2_rolling_net_patient_revenue,
        days_for_average
    from level_1_metric_rollup
),

days_in_ar_layers as (
    select
        all_levels.level_1_name,
        all_levels.level_2_name,
        all_levels.level_3_name,
        all_levels.level_4_name,
        all_levels.company_level,
        all_levels.company_id,
        all_levels.dimension_name as hierarchy_dimension_name,
        round(all_levels.calc, 3) as calc_round_3,
        round(all_levels.calc, 0) as days_in_ar,
        all_levels.metric_date_key,
        all_levels.monthly_asset_balance,
        all_levels.rolling_net_patient_revenue,
        all_levels.days_for_average,
        all_levels.layer_wid
    from
        all_levels
        cross join max_month_with_close
where
    all_levels.metric_date_key <= max_month_with_close.show_metric_to_this_date
order by
    all_levels.metric_date_key desc,
    all_levels.level_1_name,
    all_levels.level_2_name,
    all_levels.level_3_name,
    all_levels.company_level,
    all_levels.dimension_name
)

select
    company_level,
    level_1_name,
    level_2_name,
    level_3_name,
    level_4_name,
    company_id,
    hierarchy_dimension_name,
    days_in_ar,
    metric_date_key,
    layer_wid,
    level_1_name
        || case when level_2_name is null then '' else ' -> ' || level_2_name end
        || case when level_3_name is null then '' else ' -> ' || level_3_name end
        || case when level_4_name is null then '' else ' -> ' || level_4_name end
        as full_company_hierarchy_level_path
from
    days_in_ar_layers
order by
    metric_date_key desc,
    level_1_name,
    level_2_name,
    level_3_name,
    level_4_name,
    company_level,
    hierarchy_dimension_name
