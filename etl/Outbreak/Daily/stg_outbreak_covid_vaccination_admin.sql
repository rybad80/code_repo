{{ config(meta = {
    'critical': true
}) }}

/*COVID vaccinations from LPL masterfile representing vaccinations
performed by CHOP or reconciled into chart by clinician*/
with med_admin_one_row as (
    select
        medication_order_id,
        csn,
        administration_date,
        administration_department,
        case when row_number() over (
            partition by medication_order_id
            order by administration_date
        ) = 1 then 1
        end as first_admin_ind
    from {{ref('medication_order_administration')}}
    /*med was actually given*/
        where order_mode = 'Inpatient'
        and administration_type is not null
),

lpl_vaccines as (
    select
        immune.immune_id,
        immune.pat_id,
        'LPL record' as admin_source,
        coalesce(
            med_admin_one_row.administration_date,
            /*will be approx given time in OP settings*/
            procedure_order_clinical.placed_date,
            immune.immune_date)
        as received_date,
        coalesce(
            med_admin_one_row.administration_department,
            /*encounter dept is more reliable in OP setting*/
            stg_encounter.department_name,
            procedure_order_clinical.department_name)
        as administration_location,
        coalesce(zc_mfg.name, 'Not Specified') as manufacturer_name,
        lookup_covid_vaccine_visit_types.dose_description,
        coalesce(
            vaccine_clinic_type_encounter.patient_population,
            vaccine_clinic_type_order.patient_population,
            /*Treat any non-community clinic vax dept as a patient dept*/
            case
                when stg_encounter.department_id is not null
                or procedure_order_clinical.department_id is not null
                then 'CHOP Patient'
            end)
        as clinic_type,
        case
            when lower(immune.physical_site) like 'chop community clinic%'
            then 1 else 0
        end as comm_clin_abstracted_ind,
        case
            when immune.imm_historic_adm_yn = 'Y' then 1 else 0
        end as historic_ind,
        immune.order_id,
        coalesce(
            med_admin_one_row.csn,
            procedure_order_clinical.csn
        ) as order_csn,
        /*indicator for whether this was an IP administration*/
        case
            when med_admin_one_row.medication_order_id is not null then 1 else 0
        end as inpatient_administration_ind,
        /*deduplicate doses on day-patient level prioritizing CHOP admined doses*/
        case when row_number() over (
            partition by immune.pat_id, immune.immune_date
            order by historic_ind,
            immune.immune_id
        ) = 1
        then 1
    end as dedup_dose_ind
    from
        {{source('clarity_ods', 'immune')}} as immune
        inner join {{source('clarity_ods', 'grouper_compiled_rec_list')}}
            as grouper_compiled_rec_list
            on grouper_compiled_rec_list.grouper_records_numeric_id
            = immune.immunzatn_id
        /*Join to imm record to get manufacturer*/
        inner join {{source('clarity_ods', 'clarity_immunzatn')}}
            as clarity_immunzatn
            on clarity_immunzatn.immunzatn_id = immune.immunzatn_id
        left join {{source('clarity_ods', 'zc_mfg')}} as zc_mfg
            on zc_mfg.mfg_c = clarity_immunzatn.manufacturer_c
        /*clinic type and dose description from csn associated with immunization*/
        left join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.csn = immune.imm_csn
            /*Only for non-historical doses since csn could also correspond
            to when external dose was reconciled*/
            and immune.imm_historic_adm_yn is null
        left join {{ref('lookup_covid_vaccine_visit_types')}}
            as lookup_covid_vaccine_visit_types
            on lookup_covid_vaccine_visit_types.visit_type_id
            = stg_encounter.visit_type_id
        left join {{ref('lookup_covid_vaccine_clinic_type')}}
            as vaccine_clinic_type_encounter
            on vaccine_clinic_type_encounter.department_id
            = stg_encounter.department_id
            and stg_encounter.encounter_date
                between vaccine_clinic_type_encounter.align_start_date
                and coalesce(vaccine_clinic_type_encounter.align_end_date, current_date + interval '1 year')
        /*clinic type from order associated with immunization*/
        left join {{ref('procedure_order_clinical')}}
            as procedure_order_clinical
            on procedure_order_clinical.procedure_order_id = immune.order_id
        left join {{ref('lookup_covid_vaccine_clinic_type')}}
            as vaccine_clinic_type_order
            on vaccine_clinic_type_order.department_id
            = procedure_order_clinical.department_id
            and procedure_order_clinical.placed_date
                between vaccine_clinic_type_order.align_start_date
                and coalesce(vaccine_clinic_type_order.align_end_date, current_date + interval '1 year')
        left join med_admin_one_row
            on med_admin_one_row.medication_order_id = immune.order_id
            and med_admin_one_row.first_admin_ind = 1
    where
        /*COVID vax grouper*/
        grouper_compiled_rec_list.base_grouper_id = '123464'
        and immune.immnztn_status_c = 1 --Given
),
/*COVID vaccinations from DXR masterfile representing vaccinations
reported from an external source and potentially unreconciled*/
dxr_vaccines as (
    select distinct
        docs_rcvd.pat_id,
        imm_admin.imm_date as received_date
    from
        {{source('clarity_ods', 'imm_admin')}} as imm_admin
        inner join {{source('clarity_ods', 'grouper_compiled_rec_list')}}
            as grouper_compiled_rec_list
            on grouper_compiled_rec_list.grouper_records_numeric_id = imm_admin.imm_type_id
        inner join {{source('clarity_ods', 'docs_rcvd')}} as docs_rcvd
            on docs_rcvd.document_id = imm_admin.document_id
    where
        /*COVID vax grouper*/
        grouper_compiled_rec_list.base_grouper_id = '123464'
        and imm_admin.imm_status_c = 1 --Given
)

select
    immune_id,
    pat_id,
    admin_source,
    received_date,
    administration_location,
    manufacturer_name,
    dose_description,
    clinic_type,
    comm_clin_abstracted_ind,
    historic_ind,
    order_id,
    order_csn,
    inpatient_administration_ind
from
    lpl_vaccines
where
    dedup_dose_ind = 1

union all

select
    0 as immune_id,
    pat_id,
    'DXR record' as admin_source,
    received_date,
    null as administration_location,
    null as manufacturer_name,
    null as dose_description,
    null as clinic_type,
    0 as comm_clin_abstracted_ind,
    1 as historic_ind,
    null as order_id,
    null as order_csn,
    0 as inpatient_administration_ind
from
    dxr_vaccines
