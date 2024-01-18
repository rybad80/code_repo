select

        cohort.pathway_infection_key,
        cohort.visit_key,
        cohort.pat_key,
        cohort.care_location,
        cohort.department_name,
        cohort.age_years,
        cohort.age_ge_2,
        (case when cohort.age_ge_2 = 1 then '2+' else '<2' end) as age_ge_2_str,
        cohort.complex_chronic_condition_ind,
        cohort.infection,
        cohort.encounter_date,
        cohort.ed_revisit_3_days,
        cohort.ed_revisit_7_days,
        cohort.ed_revisit_30_days,
        cohort.pc_revisits_3_days,
        cohort.pc_revisits_7_days,
        cohort.pc_revisits_30_days,
        cohort.smartset_ind,
        cohort.after_smartset_update_ind,
        (
            case when cohort.after_smartset_update_ind = 1 then 'After' else 'Before' end
        ) as after_smartset_update_str,
        cohort.pref_list_ind,
        meds_adherence.follow_duration_guid_ind,
        meds_adherence.follow_choice_guid_ind,
        meds_adherence.follow_guid_ind


    from {{ ref('stg_pathway_infection_adherence') }} as meds_adherence
        inner join
            {{ ref('stg_pathway_infection_cohort') }} as cohort on
                cohort.pathway_infection_key = meds_adherence.pathway_infection_key
