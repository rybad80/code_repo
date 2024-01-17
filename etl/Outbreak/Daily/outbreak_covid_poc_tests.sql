{{ config(meta = {
    'critical': true
}) }}

with main as ( --noqa: PRS
    select
        procedure_order_clinical.proc_ord_key,
        procedure_order_clinical.procedure_order_id,
        procedure_order_clinical.pat_key,
        procedure_order_clinical.csn,
        stg_patient.dob,
        stg_patient.sex,
        stg_patient.race,
        stg_patient.ethnicity,
        procedure_order_clinical.specimen_taken_date,
        procedure_order_clinical.result_date,
        procedure_order_clinical.mrn as patient_mrn,
        patient.last_nm as patient_last_name,
        patient.first_nm as patient_first_name,
        to_char(date(patient.dob), 'yyyymmdd') as patient_dob_yyyymmdd,
        case
            when lower(stg_patient.sex) = 'f' then 'Female'
            when lower(stg_patient.sex) = 'm' then 'Male'
            else 'Unknown'
        end as patient_sex,
        case
            when lower(stg_patient.race) = 'asian' then 'Asian'
            when lower(stg_patient.race) = 'black or african american' then 'Black' --noqa: L016
            when lower(stg_patient.race) = 'white' then 'White'
            when lower(stg_patient.race) = 'native hawaiian or other pacific islander' --noqa: L016
                then 'Pacific Islander'
            when lower(stg_patient.race) = 'american indian or alaska native'
                then 'Native American'
            when lower(stg_patient.race) = 'refused' then 'Unknown'
            else 'Other'
        end as patient_race,
        case
            when lower(stg_patient.ethnicity) = 'hispanic or latino' then 'Hispanic' --noqa: L016
            when lower(stg_patient.ethnicity) = 'not hispanic or latino'
                then 'Non-Hispanic'
            else 'Unknown'
        end as patient_ethnicity,
        stg_patient.mailing_address_line1 as patient_street_addr_line1,
        stg_patient.mailing_address_line2 as patient_street_addr_line2,
        patient.city as patient_city,
        patient.state as patient_state,
        patient.zip as patient_zip,
        patient.county as patient_county,
        patient.home_ph as patient_phone_10digit,
        case when patient.home_ph is null then '' else 'home' end as patient_phone_type, --noqa: L016
        department.dept_nm as patient_contact_department,
        procedure_order_clinical.procedure_order_id as accession_order_id,
        to_char(date(procedure_order_clinical.specimen_taken_date), 'yyyymmdd') as date_specimen_collected_yyyymmdd, --noqa: L016 
        procedure_order_clinical.procedure_name as procedure, --noqa: L029 
        to_char(procedure_order_clinical.result_date, 'yyyymmddhhmiss') as result_date_yyyymmddhhmiss, --noqa: L016
        procedure_order_result_clinical.result_component_external_name as component, --noqa: L016
        procedure_order_result_clinical.result_value as test_result,
        dim_result_flag.rslt_flag_nm as result_flag,
        procedure_order_result_clinical.result_status,
        spec_type.dict_nm as specimen_type,
        procedure_order_clinical.order_specimen_source as specimen_source,
        provider.full_nm as ordering_provider,
        identity_ser_id.identity_type_id as mpi_id_type_id,
        provider.npi,
        provider_addr.addr_line1 as ordering_provider_street_addr_line1,
        provider_addr.addr_line2 as ordering_provider_street_addr_line2,
        provider_addr.city as ordering_provider_city,
        provider_addr.state as ordering_provider_state,
        provider_addr.zip as ordering_provider_zip,
        provider_addr.phone as ordering_provider_phone_10digit,
        provider.last_nm as ordering_provider_last_name,
        provider.first_nm as ordering_provider_first_name
    from
        {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
    inner join {{ref('outbreak_master_covid_tests')}} as outbreak_master_covid_tests
        on outbreak_master_covid_tests.result_component_id = procedure_order_result_clinical.result_component_id --noqa: L016
    inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
        on procedure_order_clinical.proc_ord_key = procedure_order_result_clinical.proc_ord_key --noqa: L016
    left join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = procedure_order_clinical.pat_key
    left join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = procedure_order_clinical.pat_key
    left join {{source('cdw', 'procedure_order')}} as procedure_order
        on procedure_order_clinical.proc_ord_key = procedure_order.proc_ord_key
    left join {{source('cdw', 'cdw_dictionary')}} as spec_type
        on spec_type.dict_key = procedure_order.dict_spec_type_key
    left join {{source('cdw', 'department')}} as department
        on procedure_order.pat_loc_dept_key = department.dept_key
    left join {{source('cdw', 'procedure_order_result')}} as procedure_order_result
        on procedure_order_clinical.proc_ord_key = procedure_order_result.proc_ord_key --noqa: L016
            and procedure_order_result.seq_num = procedure_order_result_clinical.result_seq_num --noqa: L016
    left join {{source('cdw', 'dim_result_flag')}} as dim_result_flag
        on procedure_order_result.dim_rslt_flag_key = dim_result_flag.dim_rslt_flag_key --noqa: L016
    left join {{source('cdw', 'provider')}} as provider
        on procedure_order.auth_prov_key = provider.prov_key
    left join {{source('cdw', 'provider_addr')}} as provider_addr
        on provider.prov_key = provider_addr.prov_key
            and provider_addr.line = 1
    left join {{source('clarity_ods', 'identity_ser_id')}} as identity_ser_id
        on provider.prov_id = identity_ser_id.prov_id
            and identity_ser_id.line = 1
    where
        outbreak_master_covid_tests.poc_ind = 1
        and lower(procedure_order_clinical.order_status) not in ('canceled', 'not applicable') --noqa: L016
        and lower(procedure_order_clinical.procedure_order_type) != 'parent order' --noqa: L016
        and procedure_order_result_clinical.result_value is not null
),

order_status as (
    select
        main.procedure_order_id,
        order_status.resulting_lab_id,
        order_status.lab_status_c,
        order_status.instant_of_entry,
        row_number() over (partition by main.procedure_order_id order by order_status.instant_of_entry desc) as result_no --noqa: L016
    from
        main
    inner join {{source('clarity_ods', 'order_status')}} as order_status
        on main.procedure_order_id = order_status.order_id
    where
        order_status.lab_status_c in (3, 4, 5)
)

select
    main.*, --noqa: L013
    clarity_llb.llb_name as resulting_lab,
    clarity_llb.llb_addr_ln1 as ordering_facility_street_addr_line1,
    clarity_llb.llb_addr_ln2 as ordering_facility_street_addr_line2,
    clarity_llb.llb_city as ordering_facility_city,
    dim_state.state_abbr as ordering_facility_state,
    clarity_llb.llb_zip as ordering_facility_zip,
    clarity_llb.llb_contact_ph as ordering_facility_phone_10digit
from
    main
    inner join order_status
        on main.procedure_order_id = order_status.procedure_order_id
    left join {{source('clarity_ods', 'clarity_llb')}} as clarity_llb
        on order_status.resulting_lab_id = clarity_llb.resulting_lab_id
        and order_status.resulting_lab_id is not null
    left join {{source('cdw', 'dim_state')}} as dim_state
        on clarity_llb.llb_state_c = dim_state.state_id
where
    order_status.result_no = 1
