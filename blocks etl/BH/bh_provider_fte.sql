select
    cast(record_id as integer) as redcap_id,
    provider_name_clin_eff as provider_name,
    submission_date,
    fiscal_year_change as fiscal_year,
    submission_reason,
    cast(total_fte_v2 as float) as changed_fte,
    clinical_nonclinical as fte_type,
    primary_group_1 as primary_group,
    secondary_group,
    tertiary_group,
    cast(program_1_calculated_fte_2 as float) as primary_fte,
    cast(program_2_calculated_fte_2 as float) as secondary_fte,
    cast(program_3_calculated_fte_2 as float) as tertiary_fte,
    cast(overall_clinical_fte as float) as total_clinical_fte,
    cast(fte_academic_calc as float) as academic_fte,
    cast(fte_admin_calc as float) as admin_fte,
    cast(fte_research_calc as float) as research_fte,
    cast(total_non_clinical_fte as float) as total_non_clinical_fte,
    cast(total_fte_annual as float) as total_fte_annual,
    total_effective_start_date,
    total_effective_end_date,
    case when do_you_anticipate_a_change = 'Yes' then 1 else 0 end as change_expected_ind,
    case when do_you_work_across_multipl = 'Yes' then 1 else 0 end as multiple_group_ind,
    case when submission_date is not null then 1 else 0 end as completed_survey_ind,
    case
        when max(redcap_id) over (partition by provider_name order by provider_name) = redcap_id then 1 else 0
    end as most_recent_submitted_ind
from {{source('ods_redcap_research', 'redcap_bh_fte_form')}}
where cast(fiscal_year as integer) >= 2024
