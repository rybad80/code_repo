--Cohort CTE
    with cohort as (

        select
            encounter_all.visit_key,
            encounter_all.mrn,
            encounter_all.pat_key,
            encounter_all.encounter_date,
            encounter_all.hospital_discharge_date,
            encounter_all.department_name,
            encounter_all.age_years,


            (case when encounter_all.age_years >= 2 then 1 else 0 end) as age_ge_2,


            -- Department Variable
            (case
                when encounter_all.primary_care_ind = 1
                    then 'Primary Care'

                when lower(encounter_all.department_name) in (
                        'kop urgent care center',
                        'buc urgent care center',
                        'bwv urgent care center',
                        'mays landing urg care',
                        'haverford urg care ctr',
                        'abington urg care ctr'
                    )
                    then 'Urgent Care'

                when encounter_all.ed_ind = 1
                    then 'Emergency Department'

                else null end) as care_location,

            -- Infection Variable
            (case

                -- AOM
                when

                    (
                        diagnosis_encounter_all.icd9_code in (
                            '382',
                            '382.0',
                            '382.00',
                            '382.01',
                            '382.4'
                        )

                        or lower(diagnosis_encounter_all.icd10_code) like 'h66.0%'
                        or lower(diagnosis_encounter_all.icd10_code) like 'h66.4%'
                        or lower(diagnosis_encounter_all.icd10_code) like 'h66.9%'

                        )

                    --Excluding concurrent infections (Pneumonia, Sinusitis, Genitourinary Infections, Skin Infections, and Streptococcal Pharyngitis)
                    and not regexp_like(
                        lower(diagnosis_encounter_all.icd10_code),
                            '(?i)j13|j14|j15|j16|j17|j18|j01|j32|n30.0|n30.00|n30.01|n39.0|n76.0|l01.0|l01.01|l01.02|l01.03|l01.09|l02|l03|l08.89|l08.90|j02.0|j03.0'
                        )

                    then 'Acute Otitis Media'

                -- CAP
                when
                    lower(diagnosis_encounter_all.icd10_code) in
                        (
                            'a37.91', 
                            'j09.x1', 
                            'j10.00', 
                            'j10.01', 
                            'j10.08',
                            'j11.00',
                            'j11.08',
                            'j12.0',
                            'j12.1',
                            'j12.2',
                            'j12.3',
                            'j12.89',
                            'j12.9',
                            'j13',
                            'j14',
                            'j15.0',
                            'j15.20',
                            'j15.211',
                            'j15.212',
                            'j15.3',
                            'j15.4',
                            'j15.7',
                            'j15.8',
                            'j15.9',
                            'j16.8',
                            'j18.0',
                            'j18.1',
                            'j18.8',
                            'j18.9',
                            'j22',
                            'jr09.1'
                        )
                    or diagnosis_encounter_all.icd9_code in
                        (
                            '480',
                            '480.1',
                            '480.2',
                            '480.8',
                            '480.9',
                            '481',
                            '482.0',
                            '482.30',
                            '482.31',
                            '482.32',
                            '482.40',
                            '482.41',
                            '482.42',
                            '482.89',
                            '482.9',
                            '483.0',
                            '483.8',
                            '484.3',
                            '485',
                            '486',
                            '487',
                            '510.0'
                        )
                    then 'Community Acquired Pneumonia'

                    else null
            end) as infection

        from {{ ref('encounter_all') }} as encounter_all
            inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
                on encounter_all.visit_key = diagnosis_encounter_all.visit_key
            left join {{ ref('encounter_ed') }} as encounter_ed
                on encounter_all.visit_key = encounter_ed.visit_key

        where
            care_location is not null
            and infection is not null
            and (encounter_ed.edecu_ind = 0 or encounter_ed.edecu_ind is null)
            and diagnosis_encounter_all.visit_diagnosis_ind = 1 --ICD codes from visit dx, not problem lists
            and encounter_all.inpatient_ind = 0 --not admitted to the hosptial
            and encounter_all.age_years < 21
            and date_trunc('day', encounter_all.encounter_date) >= '2018-10-01'

        group by
            encounter_all.visit_key,
            infection,
            encounter_all.pat_key,
            encounter_all.encounter_date,
            encounter_all.department_name,
            encounter_all.primary_care_ind,
            encounter_all.mrn,
            diagnosis_encounter_all.icd9_code,
            diagnosis_encounter_all.icd10_code,
            encounter_all.hospital_discharge_date,
            encounter_all.ed_ind,
            encounter_all.well_visit_ind,
            encounter_all.age_years
    ),

    --patients that have been prescribed immunosuppresants at most a year before CAP encounter
    immuno as (

        select
            cohort.visit_key,
            cohort.infection,
            max(case when lower(dict_pharm_subclass.dict_nm) = 'immunosuppressive agents'
                then 1 else 0 end) as immuno_ind

        from cohort
            left join
                {{ ref('medication_order_administration') }} as medication_order_administration on
                    cohort.pat_key = medication_order_administration.pat_key
            inner join {{source('cdw', 'medication')}} as medication
                on medication_order_administration.med_key = medication.med_key
            inner join
                {{source('cdw', 'cdw_dictionary')}} as dict_pharm_subclass on
                    medication.dict_pharm_subclass_key = dict_pharm_subclass.dict_key

        where
            medication_order_administration.medication_start_date > cohort.encounter_date - interval '365 days'
            and medication_order_administration.medication_start_date <= cohort.encounter_date
            and lower(medication_order_administration.order_status) not like '%cancel%'
            and lower(cohort.infection) = 'community acquired pneumonia'

        group by
            cohort.visit_key,
            cohort.infection
    ),

    --Complex Care Condition CTE, only relevant for CAP
    complex_care as (

        select
            cohort.visit_key,
            cohort.infection,
            cohort.care_location,
            diagnosis_medically_complex.complex_chronic_condition_ind

        from cohort
            left join
                {{ ref('diagnosis_medically_complex') }} as diagnosis_medically_complex on
                    cohort.visit_key = diagnosis_medically_complex.visit_key

        where
            lower(cohort.infection) = 'community acquired pneumonia'

    ),

    --Smartset CTE
    smartset as (

        select
            cohort.visit_key,
            cohort.infection,
            max(case when lower(medication_order_administration.orderset_name) in
                ('bacterial/viral - primary care - 2020',
                    'urgent care ear / om discharge ss',
                    'ed ear/om discharge ss',
                    'urgent care pneumonia discharge ss',
                    'ed pneumonia discharge ss')
            then 1 else 0 end) as smartset_ind

            from cohort
                inner join {{ ref('medication_order_administration') }} as medication_order_administration
                    on cohort.visit_key = medication_order_administration.visit_key

        group by
            cohort.visit_key,
            cohort.infection
    ),

    --After Smartset CTE: to create indicator whether encounter was before or after smartset intervention
    after_smartset as (

            select
                cohort.visit_key,
                cohort.infection,

                --smartset intervention dates:
            -- ed aom 10/17/2019 or 10/15/2019
            -- uc aom 3/19/2020
            -- pc aom 4/30/2021

            max(case when cohort.infection = 'Acute Otitis Media'
                    and (
                        (cohort.care_location = 'Primary Care'
                            and date_trunc('day', cohort.encounter_date) >= '2021-04-30'
                            )
                        or (cohort.care_location = 'Urgent Care'
                            and date_trunc('day', cohort.encounter_date) >= '2020-03-19'
                            )
                        or (cohort.care_location = 'Emergency Department'
                            and date_trunc('day', cohort.encounter_date) >= '2019-10-15'
                            )
                    )
                then 1

            --add CAP dates
                when cohort.infection = 'Community Acquired Pneumonia'
                        and (
                            (cohort.care_location = 'Primary Care'
                                and date_trunc('day', cohort.encounter_date) >= '2021-04-30'
                            )
                        or (cohort.care_location = 'Emergency Department'
                            and date_trunc('day', cohort.encounter_date) >= '2020-02-01'
                            )
                        or (cohort.care_location = 'Urgent Care'
                            and date_trunc('day', cohort.encounter_date) >= '2022-05-19')
                        )

                    then 1

                else 0 end) as after_smartset_update_ind

            from cohort

            group by
                cohort.visit_key,
                cohort.infection
    ),

    --Preference Lists CTE
    pref_list as (

        select

            cohort.visit_key,
            cohort.infection,

            max(case when ord_prflst_trk.preference_list_id = 1780068
                or ord_prflst_trk.preference_list_id = 68
                then 1 else 0 end) as pref_list_ind

        from cohort
            inner join
                {{source('cdw', 'medication_order')}} as medication_order on
                    cohort.visit_key = medication_order.visit_key
            inner join {{source('clarity_ods', 'ord_prflst_trk')}} as ord_prflst_trk
                on medication_order.med_ord_id = ord_prflst_trk.order_id

        where lower(care_location) = 'primary care'

        group by
            cohort.visit_key,
            cohort.infection
    ),

    --Revisits CTE
    revisits as (

        select

            cohort.visit_key,
            cohort.infection,

            max(case when
            revisit.care_location = 'Emergency Department'
                and (
                    revisit.encounter_date <= coalesce(
                        cohort.hospital_discharge_date, cohort.encounter_date
                    ) + interval '3 days'
                )
                then 1 else 0 end) as ed_revisit_3_days,

            max(case when
            revisit.care_location = 'Emergency Department'
                and (
                    revisit.encounter_date <= coalesce(
                        cohort.hospital_discharge_date, cohort.encounter_date
                    ) + interval '7 days'
                )
            then 1 else 0 end) as ed_revisit_7_days,

            max(case when
            revisit.care_location = 'Emergency Department'
                and (
                    revisit.encounter_date <= coalesce(
                        cohort.hospital_discharge_date, cohort.encounter_date
                    ) + interval '30 days'
                )
            then 1 else 0 end) as ed_revisit_30_days,

            max(case when
            revisit.care_location = 'Primary Care'
                and (
                    revisit.encounter_date <= coalesce(
                        cohort.hospital_discharge_date, cohort.encounter_date
                    ) + interval '3 days'
                )
            then 1 else 0 end) as pc_revisit_3_days,

            max(case when
            revisit.care_location = 'Primary Care'
                and (
                    revisit.encounter_date <= coalesce(
                        cohort.hospital_discharge_date, cohort.encounter_date
                    ) + interval '7 days'
                )
            then 1 else 0 end) as pc_revisit_7_days,

            max(case when
            revisit.care_location = 'Primary Care'
                and (
                    revisit.encounter_date <= coalesce(
                        cohort.hospital_discharge_date, cohort.encounter_date
                    ) + interval '30 days'
                )
            then 1 else 0 end) as pc_revisit_30_days

            from cohort
                inner join cohort as revisit on cohort.pat_key = revisit.pat_key

            where
                revisit.encounter_date > cohort.encounter_date
                and revisit.encounter_date <= cohort.encounter_date + interval '30 days'
                --revisit if return with same infection:
                and cohort.infection = revisit.infection

            group by
                cohort.visit_key,
                cohort.infection
    )

    -- final SFW

    select distinct
        {{
            dbt_utils.surrogate_key([
                'cohort.visit_key',
                'cohort.infection'
            ])
        }} as pathway_infection_key,
        cohort.visit_key,
        cohort.mrn,
        cohort.pat_key,
        cohort.care_location,
        cohort.department_name,
        cohort.age_years, 
        cohort.age_ge_2,
        cohort.infection,
        cohort.encounter_date,
        coalesce(revisits.ed_revisit_3_days, 0) as ed_revisit_3_days,
        coalesce(revisits.ed_revisit_7_days, 0) as ed_revisit_7_days,
        coalesce(revisits.ed_revisit_30_days, 0) as ed_revisit_30_days,
        coalesce(revisits.pc_revisit_3_days, 0) as pc_revisits_3_days,
        coalesce(revisits.pc_revisit_7_days, 0) as pc_revisits_7_days,
        coalesce(revisits.pc_revisit_30_days, 0) as pc_revisits_30_days,
        coalesce(smartset.smartset_ind, 0) as smartset_ind,
        (after_smartset.after_smartset_update_ind) as after_smartset_update_ind,
        coalesce(pref_list.pref_list_ind, 0) as pref_list_ind,
        coalesce(complex_care.complex_chronic_condition_ind, 0) as complex_chronic_condition_ind

    from cohort
        left join smartset on cohort.visit_key = smartset.visit_key
            --joining on infection because a patient could have multiple diagnoses in one encounter
            and cohort.infection = smartset.infection
        inner join after_smartset on cohort.visit_key = after_smartset.visit_key
            and cohort.infection = after_smartset.infection
        left join pref_list on cohort.visit_key = pref_list.visit_key
            and cohort.infection = pref_list.infection
        left join revisits on cohort.visit_key = revisits.visit_key
            and cohort.infection = revisits.infection
        left join immuno on cohort.visit_key = immuno.visit_key
            and cohort.infection = immuno.infection
        left join complex_care on cohort.visit_key = complex_care.visit_key
            and cohort.infection = complex_care.infection

    where immuno.immuno_ind = 0 or immuno.immuno_ind is null
