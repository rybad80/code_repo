--region identify flowsheets of interest
select
    stg_restraints.visit_key,
    flowsheet_all.recorded_date,
    flowsheet_all.flowsheet_id,
    flowsheet_all.meas_val
from
    {{ ref('stg_restraints') }} as stg_restraints
    inner join {{ ref('flowsheet_all') }} as flowsheet_all
        on stg_restraints.visit_key = flowsheet_all.visit_key
where
    flowsheet_all.flowsheet_id in (
        /*Violent Restraint Justification*/
        40071738, --Precipitating Factors
        40071737, --Less Restrictive Alternative
        40071740, --Clinical Justification
        40071756, --Restraint Types
        /*Visual Observation*/
        40071747, --Visual Observation
        /*Upon Starting AND Q15 Minute RN Assessments Only*/
        40071746, --Circulation/Skin Integrity (WDL)
        40068161, --Behavior Warranting Restraints Continued
        40071744, --Observable Patient Behaviors
        40071745, --Physical Comfort/Device check
        8, --Pulse
        9, --Resp
        /*Q2 Hours Restraint Monitoring*/
        40071704, --Range of Motion
        40071787, --Elimination (Hygiene Needs)
        40071789 --Fluids/Food/Meal
    )
    and (
        flowsheet_all.meas_val is not null
        or flowsheet_all.meas_cmt is not null
    )
group by
    stg_restraints.visit_key,
    flowsheet_all.recorded_date,
    flowsheet_all.flowsheet_id,
    flowsheet_all.meas_val
