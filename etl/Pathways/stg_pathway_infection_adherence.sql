with duration as (

        select

            cohort_meds.pathway_infection_key,
            cohort.age_years,

            -- taking the minimum because if one is wrong, then all is wrong
            min(case

                --Acute Otitis Media
                when
                    lower(cohort.infection) = 'acute otitis media'
                    and lower(gen_nm_cln_sub) in(
                        'amoxicillin',
                        'amoxicillin clavulanate',
                        'clindamycin',
                        'cefdinir',
                        'levofloxacin',
                        'cefprozil',
                        'cefpodoxime',
                        'cefuroxime',
                        'ceftriaxone',
                        'ciprofloxacin')
                    and (

                        (cohort.encounter_date < '2023-04-01'
                            and (
                                (rx_days <= 7  and age_ge_2 = 1)
                                or (rx_days <= 10 and age_ge_2 = 0)
                            )
                        )

                        --after pathway update in March 2023 
                        or (cohort.encounter_date >= '2023-04-01'
                            and (
                                (rx_days <= 10 and cohort.age_years < 2)
                                or (rx_days <= 7 and cohort.age_years >= 2 and cohort.age_years < 6)
                                or (rx_days <= 5 and cohort.age_years >= 6)
                            )
                        )
                    )
                then 1

                --Community Acquired Pneumonia in the ED
                when
                    lower(cohort.infection) = 'community acquired pneumonia'
                    and lower(cohort.care_location) = 'emergency department'
                    and (

                            --After first smartset update, it was 7 days. First smartset update was 2020-11-01
                            (cohort.encounter_date < '2020-11-01'
                                and rx_days <= 7)

                            --After second smartset update, it was 5 days
                            or (cohort.complex_chronic_condition_ind = 0
                                and cohort.encounter_date > '2020-10-31'
                                and rx_days <= 5)

                            --After second smartset update, its 7 days for patients with CCC
                            or (cohort.complex_chronic_condition_ind = 1
                                and cohort.encounter_date > '2020-11-01'
                                and rx_days <= 7)

                    )
                then 1

                --Community Acquired Pneumonia in PC
                when lower(cohort.infection) = 'community acquired pneumonia'
                    and lower(cohort.care_location) in ('primary care', 'urgent care')
                    and (
                        (cohort.complex_chronic_condition_ind = 1
                            and rx_days <= 7)

                        or (cohort.complex_chronic_condition_ind = 0
                            and rx_days <= 5)

                    )
                then 1

            else 0 end) as follow_duration_guid_ind

        from {{ ref('pathway_infection_meds') }} as cohort_meds
            inner join
                {{ ref('stg_pathway_infection_cohort') }} as cohort on
                    cohort_meds.pathway_infection_key = cohort.pathway_infection_key

        group by
        cohort_meds.pathway_infection_key,
        cohort.age_years

),

    --to check if patient is allergic to penicillin or amoxicillin
    amox_pen_allergy as (

        select
            cohort_meds.pathway_infection_key,

            max(case when lower(patient_allergy.alrg_desc) like '%amoxi%'
                        or lower(patient_allergy.alrg_desc) like '%amoxy%'
                        or lower(patient_allergy.alrg_desc) like '%penicill%'
                    then 1
                    else 0
                    end) as allergy_ind

        from {{ ref('pathway_infection_meds') }} as cohort_meds
            inner join
                {{ ref('stg_pathway_infection_cohort') }} as cohort on
                    cohort_meds.pathway_infection_key = cohort.pathway_infection_key
            inner join {{source('cdw', 'patient_allergy')}} as patient_allergy
                on cohort.pat_key = patient_allergy.pat_key

        --before or same day
        where patient_allergy.entered_dt <= cohort.encounter_date

        group by
            cohort_meds.pathway_infection_key
),

    -- to check if patient was taking amox in the past 30 days
    past_visit_meds as (

        select

            cohort.pathway_infection_key,
            max(medication_order_administration.medication_start_date) as past_amox_start_date,
            max(case when lower(medication_name) like '%amoxicillin%' then 1
                else 0 end) as amox_ind


        from {{ ref('stg_pathway_infection_cohort') }} as cohort
            inner join
                {{ ref('medication_order_administration') }} as medication_order_administration on
                    cohort.pat_key = medication_order_administration.pat_key

        where
            medication_order_administration.medication_start_date < cohort.encounter_date
                and (
                --either still taking prescription
                medication_order_administration.medication_end_date is null
                --or it ended within the past 30 days
                    or medication_order_administration.medication_end_date
                    >= cohort.encounter_date - interval '30 days')
            and lower(medication_order_administration.medication_name) like '%amoxicillin%'
            and lower(medication_order_administration.medication_name) not like '%clavulanate%'
            and lower(medication_order_administration.order_status) not like '%cancel%'

        group by
            cohort.pathway_infection_key
),

    --to check if patient was diagnosed with conjunctivitis during encounter, only relevant for AOM
    conjunctivitis as (

        select
            cohort.pathway_infection_key,

            max(case when lower(diagnosis_encounter_all.icd10_code) like 'h10%'
                or lower(diagnosis_encounter_all.icd10_code) like 'b30%'
            then 1 else 0 end) as conjunctivitis_ind

        from {{ ref('stg_pathway_infection_cohort') }} as cohort
            left join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
                on cohort.pat_key = diagnosis_encounter_all.pat_key

        where
        -- had conjunctivitis within the past 30 days of encounter
            diagnosis_encounter_all.encounter_date between cohort.encounter_date - interval '30 days'
                and cohort.encounter_date
            and lower(cohort.infection) = 'acute otitis media'

        group by
            cohort.pathway_infection_key

),

    --to check if there was at least one amox prescription in encounter, only relevant for CAP
    current_amox as (

        select
            cohort_meds.pathway_infection_key,
            max(
                case when lower(cohort_meds.gen_nm_cln_sub) = 'amoxicillin'
            then 1 else 0
            end) as current_amox_ind

        from {{ ref('pathway_infection_meds') }} as cohort_meds
            inner join
                {{ ref('stg_pathway_infection_cohort') }} as cohort on
                    cohort_meds.pathway_infection_key = cohort.pathway_infection_key

        where lower(cohort.infection) = 'community acquired pneumonia'

        group by
            cohort_meds.pathway_infection_key

)

    -- final SFW

    select
        cohort_meds.pathway_infection_key,
        duration.follow_duration_guid_ind,
        coalesce(amox_pen_allergy.allergy_ind, 0) as amox_allergy_ind,
        coalesce(past_visit_meds.amox_ind, 0) as past_amox_ind,
        coalesce(current_amox.current_amox_ind, 0) as current_amox_cap_ind,
        coalesce(conjunctivitis.conjunctivitis_ind, 0) as conjunctivitis_ind,

        max(case

        --Acute Otitis Media
            when cohort_meds.infection = 'Acute Otitis Media'
                and (
                    (amox_allergy_ind = 1
                    -- if allergic to amox, then only these meds are allowed:
                    and lower(cohort_meds.gen_nm_cln_sub) in
                        ('amoxicillin',
                        'cefdinir',
                        'clindamycin',
                        'cefprozil',
                        'cefpodoxime',
                        'cefuroxime')
                    )

                    or (past_amox_ind = 1
                    -- if they took amox the past 30 days, then only these meds are allowed:
                        and lower(cohort_meds.gen_nm_cln_sub) in
                            ('amoxicillin clavulanate',
                            'ceftriaxone'
                            )
                        )

                        or (conjunctivitis.conjunctivitis_ind = 1
                        and lower(cohort_meds.gen_nm_cln_sub) = 'amoxicillin clavulanate'
                        and amox_allergy_ind = 0
                        and past_amox_ind = 0
                        )

                    or (amox_allergy_ind = 0
                            and past_amox_ind = 0
                        )

                    )
                then 1

            --Community Acquired Pneumonia
            when cohort_meds.infection = 'Community Acquired Pneumonia'
                and (
                    --if allergic to amox, only these meds are allowed.
                    (amox_allergy_ind = 1
                    and lower(cohort_meds.gen_nm_cln_sub) in
                        ('amoxicillin',
                        'amoxicillin clavulanate',
                        'clindamycin',
                        'levofloxacin')
                    )

                    --amox with anything else is okay.
                    or (
                    current_amox_cap_ind = 1
                    and amox_allergy_ind = 0
                    )

                    or (
                    --if took amox in past 30 days and not allergic, any antibiotic is okay.
                    past_amox_ind = 1
                    and amox_allergy_ind = 0
                    )

                    or (
                        lower(cohort_meds.gen_nm_cln_sub) = 'azithromycin'
                        and duration.age_years >= 5
                    )

                )

              then 1

            else 0 end) as follow_choice_guid_ind,

        case
            when duration.follow_duration_guid_ind = 1
                and follow_choice_guid_ind = 1
              then 1

            else 0 end as follow_guid_ind

        from {{ ref('pathway_infection_meds') }} as cohort_meds
            inner join duration on cohort_meds.pathway_infection_key = duration.pathway_infection_key
            left join amox_pen_allergy on duration.pathway_infection_key = amox_pen_allergy.pathway_infection_key
            left join past_visit_meds on duration.pathway_infection_key = past_visit_meds.pathway_infection_key
            left join current_amox on duration.pathway_infection_key = current_amox.pathway_infection_key
            left join conjunctivitis on duration.pathway_infection_key = conjunctivitis.pathway_infection_key

        group by
            cohort_meds.pathway_infection_key,
            duration.follow_duration_guid_ind,
            amox_pen_allergy.allergy_ind,
            past_visit_meds.amox_ind,
            current_amox.current_amox_ind,
            conjunctivitis.conjunctivitis_ind
