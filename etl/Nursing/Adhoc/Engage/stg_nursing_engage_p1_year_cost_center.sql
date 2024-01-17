/* stg_nursing_engage_p1_year_cost_center
align the cost center rollups to unit_group for the
engagement survey years for which CHOP has received
Press Ganey data -- for use by stg_nursing_engage_p2_unit_group_aggregate
so that the unit group nursing enagagement data includes the applicable
cost centers' response data for that particular year
*/
with
process_year as (
    select
        2019 as survey_year
        union all
        select 2020
        union all
        select 2021
        union all
        select 2022
)
select
        process_year.survey_year,
        unit_grp.unit_group_name,
        cc.cost_center_display,
        grp_cc.unit_group_id,
        grp_cc.cost_center_id,
        cc.cost_center_name,
        grp_cc.alternate_match_name,
        grp_cc.reference_name as nursing_unit_group_historical_name
    from {{ ref('lookup_nursing_unit_group_cost_center') }} as grp_cc
        inner join {{ ref('nursing_cost_center_attributes') }} as cc
            on grp_cc.cost_center_id  = cc.cost_center_id
        inner join {{ ref('lookup_nursing_unit_group') }} as unit_grp
            on grp_cc.unit_group_id  = unit_grp.unit_group_id
        inner join process_year  /* to get right roll-ups for that year */
            on process_year.survey_year between unit_grp.year_survey_added
            and coalesce(unit_grp.year_survey_ended, 2099)
