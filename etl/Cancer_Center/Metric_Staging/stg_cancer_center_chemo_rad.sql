with chemo_stage as (--region chemo
    select
        stg_patient.pat_key,
        stg_patient.mrn,
        date_trunc('month', medication_order_administration.administration_date) as chemo_month
    from
        {{source('cdw', 'treatment_plan_order')}} as treatment_plan_order
        inner join {{ref ('medication_order_administration')}} as medication_order_administration
            on treatment_plan_order.ord_key = medication_order_administration.med_ord_key
        inner join {{source('cdw', 'medication_order')}} as medication_order
            on medication_order.med_ord_key = medication_order_administration.med_ord_key
        inner join {{source('cdw', 'cdw_dictionary')}} as med_hist
            on med_hist.dict_key = medication_order.dict_ord_class_key
        inner join {{ref ('stg_patient')}} as stg_patient
            on stg_patient.pat_key = medication_order_administration.pat_key
        left join {{ref ('lookup_oncology_patterns')}} as lookup_oncology_patterns
            on lookup_oncology_patterns.description = 'chemo_drugs'
            and lower(medication_order_administration.medication_name) like lookup_oncology_patterns.pattern
    where
        (
        --region chemo drugs and radiation
        (medication_order_administration.therapeutic_class = 'Antineoplastic Agents'
        and medication_order_administration.pharmacy_sub_class != 'Chemotherapy Rescue/Antidote Agents')
        or lookup_oncology_patterns.pattern is not null
        --end region
        )
        -- remove historical meds
        and med_hist.src_id != 3
        /*src_id code 3 refers to historical meds*/
        -- make sure meds were administered
        and medication_order_administration.administration_type_id in (
            105,
            102,
            122.0020,
            6,
            103,
            1,
            106,
            112,
            117)
        /*src_id codes:'Given By Other','New Syringe','Started by Other','Given','Performed',
        'Pt/Caregiver Admin - Non High Alert','IV Started','Pt/Caregiver Admin - High Alert', 'New Bag'*/
        -- remove future med orders
        and medication_order_administration.administration_date < current_date
        -- remove test patients
        and stg_patient.pat_key != 0
    group by
        stg_patient.pat_key,
        stg_patient.mrn,
        chemo_month
        --end region
),
rad_stage as (-- region radiation
    select
        pat_key,
        mrn,
        date_trunc('month',
            coalesce(hospital_admit_date, encounter_date)) as radiation_month
    from
        {{ref ('stg_encounter')}}
    where
        -- visits at PCAM
        department_name like '%PCAM%'
        -- pcam visit types
        and visit_type_id in (
            '8801', --'PROTON W/O GA'
            '8800', --'PROTON W GA'
            '8818', --'RES-XRT W/O GA'
            '8817', --'XRT W/GA'
            '8850', --'XRT W/O GA'
            '8820', --'INP-XRT W/O GA'
            '8802', --'INP-PROTON W GA'
            '8803', --'INP-PROTON W/O GA'
            '8819', --'INP-XRT W/GA'
            '8832', --'INP-TBI WITHOUT GA'
            '8835', --'INP-TBI W GA AND LUNG BLOCKS'
            '8849', --'INP-TBI W/O GA AND LUNG BLOCKS'
            '8836' --'INP- TBI W CARD GA LUNG BLOCKS'
        )
        -- valid appointment status
        and appointment_status_id in (
            2,  --completed
            6 --arrived
        )
        --remove future encounters
        and encounter_date < current_date
    group by
        pat_key,
        mrn,
        radiation_month
-- end region
)

select
    pat_key,
    mrn,
    chemo_month as radiation_or_chemo_month,
    'chemo' as treatment_source
from chemo_stage
union all
select
    pat_key,
    mrn,
    radiation_month as radiation_or_chemo_month,
    'rad' as treatment_source
from rad_stage
