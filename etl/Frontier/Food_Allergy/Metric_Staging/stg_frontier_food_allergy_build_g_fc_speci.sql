select
    case
        when fpies_food_challenge_ind = 1 then 'Food Allergy: FPIES Food-Challenge'
        when oit_initiation_ind = 1 then 'Food Allergy: OIT Initiation'
        when oit_milestone_ind = 1 then 'Food Allergy: OIT Milestone'
        when oit_intake_ind = 1 then 'Food Allergy: OIT Intake'
        when oit_visit_ind = 1 then 'Food Allergy: OIT Visit'
        when oit_palforzia_ind = 1 then 'Food Allergy: OIT Palforzia Visit'
        when oit_palf_initiation_ind = 1 then 'Food Allergy: OIT Palf Initiation Food Challenge'
        when food_challenge_total_ind = 1
                and (fpies_food_challenge_ind
                    + oit_initiation_ind
                    + oit_milestone_ind = 0)
        then 'Food Allergy: IgE Food-Challenge'
    end as metric_name,
    visit_key as primary_key,
    provider_name as drill_down_one,
    department_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    case
        when fpies_food_challenge_ind = 1 then 'frontier_fa_fpies_fc'
        when oit_initiation_ind = 1 then 'frontier_fa_oit_init'
        when oit_milestone_ind = 1 then 'frontier_fa_oit_mile'
        when oit_intake_ind = 1 then 'frontier_fa_oit_intake'
        when oit_visit_ind = 1 then 'frontier_fa_oit_visit'
        when oit_palforzia_ind = 1 then 'frontier_fa_oit_palforzia'
        when oit_palf_initiation_ind = 1 then 'frontier_fa_oit_palf_init'
        when food_challenge_total_ind = 1
            and (fpies_food_challenge_ind
                + oit_initiation_ind
                + oit_milestone_ind
                + oit_intake_ind
                + oit_visit_ind
                + oit_palforzia_ind
                + oit_palf_initiation_ind
                = 0) then 'frontier_fa_ige_fc'
    end as metric_id,
    visit_key as num
from
    {{ ref('frontier_food_allergy_encounter_cohort')}}
where
    food_challenge_total_ind = 1
    or fpies_food_challenge_ind = 1
    or oit_initiation_ind = 1
    or oit_milestone_ind = 1
    or oit_intake_ind = 1
    or oit_visit_ind = 1
    or oit_palforzia_ind = 1
    or oit_palf_initiation_ind = 1
