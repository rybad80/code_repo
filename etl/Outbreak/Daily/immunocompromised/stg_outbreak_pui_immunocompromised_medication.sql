{{ config(materialized='table', dist='pat_key') }}

with meds as (
    select
        medication.med_key,
        medication.med_nm,
        medication.generic_nm,
        medication.route
    from
        {{source('cdw', 'medication')}} as medication
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_pharm_subclass
            on dict_pharm_subclass.dict_key = medication.dict_pharm_subclass_key
    where
        (medication.route not in (
                                    'Inhalation',
                                    'Apply to affected area(s)',
                                    'Nasal',
                                    'Otic'
                                )
            or medication.route is null)
        and (
            lower(medication.med_nm) not like '%inhaler%'
            and lower(medication.med_nm) not like '%inhalation%'
            and lower(medication.med_nm) not like '%ophthalmic%'
            and lower(medication.med_nm) not like '%ointment%'
            and lower(medication.med_nm) not like '% cream %'
        )
        and (
            dict_pharm_subclass.dict_nm = 'Immunosuppressive Agents'
            or (
                lower(medication.med_nm) like '%tofacitinib%'
                or lower(medication.med_nm) like '%cyclosporine%'
                or lower(medication.med_nm) like '%tacrolimus%'
                or lower(medication.med_nm) like '%sirolimus%'
                or lower(medication.med_nm) like '%everolimus%'
                or lower(medication.med_nm) like '%azathioprine%'
                or lower(medication.med_nm) like '%leflunomide%'
                or lower(medication.med_nm) like '%mycophenolate%'
                or lower(medication.med_nm) like '%abatacept%'
                or lower(medication.med_nm) like '%adalimumab%'
                or lower(medication.med_nm) like '%anakinra%'
                or lower(medication.med_nm) like '%certolizumab%'
                or lower(medication.med_nm) like '%etanercept%'
                or lower(medication.med_nm) like '%golimumab%'
                or lower(medication.med_nm) like '%infliximab%'
                or lower(medication.med_nm) like '%ixekizumab%'
                or lower(medication.med_nm) like '%natalizumab%'
                or lower(medication.med_nm) like '%rituximab%'
                or lower(medication.med_nm) like '%secukinumab%'
                or lower(medication.med_nm) like '%tocilizumab%'
                or lower(medication.med_nm) like '%ustekinumab%'
                or lower(medication.med_nm) like '%vedolizumab%'
                or lower(medication.med_nm) like '%basiliximab %'
                or lower(medication.med_nm) like '%daclizumab%'
                or lower(medication.generic_nm) like '%tofacitinib%'
                or lower(medication.generic_nm) like '%cyclosporine%'
                or lower(medication.generic_nm) like '%tacrolimus%'
                or lower(medication.generic_nm) like '%sirolimus%'
                or lower(medication.generic_nm) like '%everolimus%'
                or lower(medication.generic_nm) like '%azathioprine%'
                or lower(medication.generic_nm) like '%leflunomide%'
                or lower(medication.generic_nm) like '%mycophenolate%'
                or lower(medication.generic_nm) like '%abatacept%'
                or lower(medication.generic_nm) like '%adalimumab%'
                or lower(medication.generic_nm) like '%anakinra%'
                or lower(medication.generic_nm) like '%certolizumab%'
                or lower(medication.generic_nm) like '%etanercept%'
                or lower(medication.generic_nm) like '%golimumab%'
                or lower(medication.generic_nm) like '%infliximab%'
                or lower(medication.generic_nm) like '%ixekizumab%'
                or lower(medication.generic_nm) like '%natalizumab%'
                or lower(medication.generic_nm) like '%rituximab%'
                or lower(medication.generic_nm) like '%secukinumab%'
                or lower(medication.generic_nm) like '%tocilizumab%'
                or lower(medication.generic_nm) like '%ustekinumab%'
                or lower(medication.generic_nm) like '%vedolizumab%'
                or lower(medication.generic_nm) like '%basiliximab %'
                or lower(medication.generic_nm) like '%daclizumab%'
            )
        )
    group by
        medication.med_key,
        medication.med_nm,
        medication.generic_nm,
        medication.route
)

select
    cohort.pat_key,
    cohort.outbreak_type,
    'Medication' as reason,
    date(medication_order_administration.medication_order_create_date) as start_date,
    date(medication_order_administration.medication_order_create_date) + cast('2 months' as interval) as end_date,
    group_concat(meds.med_nm) as reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_cohort') }} as cohort
    inner join {{ref('medication_order_administration')}} as medication_order_administration
        on cohort.pat_key = medication_order_administration.pat_key
    inner join meds
        on meds.med_key = medication_order_administration.med_key
where
    date(medication_order_administration.medication_order_create_date) >= '2020-01-01'
group by
    cohort.pat_key,
    cohort.outbreak_type,
    date(medication_order_administration.medication_order_create_date)
