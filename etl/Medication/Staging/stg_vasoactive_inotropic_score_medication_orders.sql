select distinct
        stg_encounter.pat_key,
        stg_encounter.visit_key,
        stg_encounter.csn,
        stg_encounter.patient_name,
        stg_encounter.mrn,
        stg_encounter.hospital_admit_date,
        stg_encounter.hospital_discharge_date,
        medication_order_administration.med_ord_key,
        --order level
        medication_order_administration.medication_name,
        medication_order_administration.medication_route,
        medication_order_administration.medication_frequency,
        medication_order_administration.medication_order_name,
        medication_order_administration.order_dose_unit,
        medication_order_administration.order_dose,
        case when
            lower(
                medication_order_administration.medication_name
            ) like '%dopamine%'
            then 'DOPAMINE'
            when
                lower(medication_order_administration.medication_name)
                like '%dobutamine%'
                then 'DOBUTAMINE'
            when
                lower(
                    medication_order_administration.medication_name
                ) like '%epinephrine%' and lower(
                    medication_order_administration.medication_name
                ) not like '%norepi%' then 'EPINEPHRINE'
            when lower(medication_order_administration.medication_name)
                       like '%milrinone%'
                       then 'MILRINONE'
            when lower(medication_order_administration.medication_name)
                       like '%vasopressin%'
                       then 'VASOPRESSIN'
            when lower(medication_order_administration.medication_name)
                       like '%norepi%'
                then 'NOREPINEPHRINE' end as med

    from {{ ref('stg_encounter') }} as stg_encounter
    inner join
        {{ ref('medication_order_administration') }}
        as medication_order_administration on
            stg_encounter.visit_key = medication_order_administration.visit_key
    where
        (
            lower(
                medication_order_administration.medication_name
            ) like '%dopamine%'
            or lower(medication_order_administration.medication_name)
            like '%dobutamine%'
            or lower(medication_order_administration.medication_name)
            like '%epinephrine%'
            or lower(medication_order_administration.medication_name)
            like '%milrinone%'
            or lower(medication_order_administration.medication_name)
            like '%vasopressin%'
            or lower(medication_order_administration.medication_name)
            like '%norepi%'
        )
        and medication_order_administration.administration_date is not null
        and stg_encounter.hospital_admit_date >= '2012-01-01'
