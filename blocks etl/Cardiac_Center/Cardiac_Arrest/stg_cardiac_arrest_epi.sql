with epi_raw as (
    select
        cardiac_arrest_cohort_model.pat_key,
        cardiac_arrest_cohort_model.cicu_enc_key,
        medication_order_administration.administration_date as action_date,
        lag(medication_order_administration.administration_date) over(
            partition by cardiac_arrest_cohort_model.visit_key
            order by medication_order_administration.administration_date
        ) as last_admin,
        extract(
            epoch from (medication_order_administration.administration_date - last_admin)
        ) / 3600.0 as admin_diff_hr

    from
        {{ref('cardiac_arrest_cohort_model')}} as cardiac_arrest_cohort_model
    inner join {{ref('medication_order_administration')}} as medication_order_administration
        on medication_order_administration.visit_key = cardiac_arrest_cohort_model.visit_key

    where
        lower(medication_order_administration.medication_order_name) like '%epinephrine%'
        and lower(medication_order_administration.medication_order_name) like '%10 mcg/ml%'
        and medication_order_administration.administration_type_id in
            (105, 102, 116, 12, 119, 122.0020, 9, 6, 103, 1, 127, 7, 115, 106, 112, 117)
        and medication_order_administration.administration_date
            between cardiac_arrest_cohort_model.in_date
                and cardiac_arrest_cohort_model.out_date + cast('4 hours' as interval)
)

select *
from
    epi_raw
where
    coalesce(admin_diff_hr, 2) > 1.0
