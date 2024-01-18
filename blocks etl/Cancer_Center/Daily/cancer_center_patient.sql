with reg_or_oncore_stage as (--region combine tumor registry and oncore
    select
        pat_key,
        1 as registry_ind,
        0 as oncore_ind
    from
        {{ref ('stg_cancer_center_tumor_registry')}}
    where
        tumor_registry_criteria_ind = 1
    union all
    select
        pat_key,
        0 as registry_ind,
        1 as oncore_ind
    from
        {{ref ('stg_cancer_center_oncore_registry')}}
    where oncore_criteria_ind = 1
--endregion
),

reg_or_oncore as (--region aggregate to patient level
    select
        pat_key,
        max(registry_ind) as registry_ind,
        max(oncore_ind) as oncore_ind
    from
        reg_or_oncore_stage
    group by
        pat_key
--endregion
),

over_2_comp_visits  as (--region identify patients who have 2+ completed oncology visits
select
    pat_key,
    mrn,
    count(distinct visit_key) as num_visits,
    case when num_visits >= 2 then 1 else 0 end as over_2_comp_visits_ind
from
    {{ref ('stg_cancer_center_visit')}}
group by
    pat_key,
    mrn
--endregion
),

chemo_rad_surg_biopsy_visits as (--region
/*combine chemo, radiation, surgery/biopsy, 2+ completed oncology visits, low touch criteria*/
    select
        pat_key,
        mrn
    from
        {{ref ('stg_cancer_center_chemo_rad')}}
    group by
        pat_key,
        mrn
    union
    select
        pat_key,
        mrn
    from
        {{ref ('stg_cancer_center_surgery_biopsy')}}
    group by
        pat_key,
        mrn
    union
    /*the over_2_comp_visits_ind cte brings in about 15000 records*/
    select
        pat_key,
        mrn
    from
        over_2_comp_visits
    where
        over_2_comp_visits_ind = 1
    group by
        pat_key,
        mrn
--endregion
),

high_touch as (--region high touch patients, ([criteria 1 or 2] and criteria 3)
    select
        reg_or_oncore.pat_key,
        patient.pat_mrn_id as mrn,
        coalesce(registry_ind, 0) as registry_ind,
        coalesce(oncore_ind, 0) as oncore_ind,
        --criteria for patient received long-term care at CHOP
        coalesce(visit_after_18_months_ind, 0) as visit_after_18_months_ind,
        case when death_dt < eighteen_months_after_visit then 1 else 0 end as death_before_18_months_ind,
        'high touch' as touch_category
    from
        reg_or_oncore
        inner join {{ref ('stg_cancer_center_first_visit')}} as stg_cancer_center_first_visit
            on reg_or_oncore.pat_key = stg_cancer_center_first_visit.pat_key
        inner join {{source('cdw', 'patient')}} as patient
            on reg_or_oncore.pat_key = patient.pat_key
    where
        ((registry_ind = 1 or oncore_ind = 1)
        and (visit_after_18_months_ind = 1 or death_before_18_months_ind = 1))
--endregion
),

medium_touch as (--region medium touch patients, (criteria 1 or 2)
    select
        reg_or_oncore.pat_key,
        patient.pat_mrn_id as mrn,
        coalesce(reg_or_oncore.registry_ind, 0) as registry_ind,
        coalesce(reg_or_oncore.oncore_ind, 0) as oncore_ind,
        'medium touch' as touch_category
    from
        reg_or_oncore
        left join high_touch
            on reg_or_oncore.pat_key = high_touch.pat_key
        left join {{source('cdw', 'patient')}} as patient
            on reg_or_oncore.pat_key = patient.pat_key
    where
        (reg_or_oncore.registry_ind = 1 or reg_or_oncore.oncore_ind = 1)
        --remove patients meet high touch criteria
        and high_touch.pat_key is null
--endregion
),

low_touch as (--region
/*low touch patients, any patient ever receiving chemo, radiation, surgery/biopsy
at CHOP OR ever having 2+ completed oncology visit (face to face)*/
    select
        chemo_rad_surg_biopsy_visits.pat_key,
        chemo_rad_surg_biopsy_visits.mrn,
        'low touch' as touch_category
    from
        chemo_rad_surg_biopsy_visits
        left join high_touch
            on chemo_rad_surg_biopsy_visits.pat_key = high_touch.pat_key
        left join medium_touch
            on chemo_rad_surg_biopsy_visits.pat_key = medium_touch.pat_key
    where
        --remove patients who meet medium/low touch criteria
        high_touch.pat_key is null
        and medium_touch.pat_key is null
--endregion
),

touch_category as (--region combine low medium high touch
select pat_key, mrn, touch_category from high_touch
union all
select pat_key, mrn, touch_category from medium_touch
union all
select pat_key, mrn, touch_category from low_touch
--endregion
)

select
    base_cohort.pat_key,
    base_cohort.mrn,
    base_cohort.dob,
    base_cohort.death_date,
    coalesce(touch_category.touch_category, 'base cohort') as touch_category,
    case when touch_category.touch_category = 'high touch'
    then 1 else 0 end as high_touch_ind,
    case when touch_category.touch_category = 'medium touch'
    then 1 else 0 end as medium_touch_ind,
    case when touch_category.touch_category = 'low touch'
    then 1 else 0 end as low_touch_ind
from
    {{ref ('stg_cancer_center_base_cohort')}} as base_cohort
    left join touch_category
        on base_cohort.pat_key = touch_category.pat_key
