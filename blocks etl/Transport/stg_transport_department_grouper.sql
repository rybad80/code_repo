select
    stg_transport_all_encounters.transport_key,

    case when lower(stg_transport_all_encounters.transport_type_raw) = 'outbound'
            then stg_transport_admit_department.last_department
         when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            then capacity_intercampus_transfers.receiving_department_group
        else stg_transport_admit_department.first_department
        end as accepting_department,

    case when lower(stg_transport_all_encounters.transport_type_raw) = 'outbound'
            then stg_transport_admit_department.last_service
         when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            then capacity_intercampus_transfers.receiving_service
        else stg_transport_admit_department.first_service
    end as accepting_service,

    case
        when lower(accepting_department) in ('ed', 'emergency')
            or lower(accepting_service) in ('ed', 'emergency')
            then 'ED'
        when lower(accepting_department) in ('ccu', 'cicu', 'nicu', 'pcu', 'picu')
            then accepting_department
        when lower(accepting_department) = 'surgery ip'
            or lower(accepting_service) like '%surgery%'
            or lower(accepting_service) like '%transplant%'
            then 'Non-ICU Surgical IP'
        when lower(accepting_service) in ('anesthesia', 'neurosurgery',
                                'oral and maxillofacial surgery', 'orthodontics',
                                'plastic surgery', 'trauma', 'trauma picu',
                                'trauma surgery', 'general surgery',
                                'ophthalmology', 'orthopedics', 'otorhinolaryngology',
                                'otolaryngology', 'urology', 'obstetrics', 'dentistry')
            then 'Non-ICU Surgical IP'
        when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            and lower(receiving_care_level) like '%med/surg%'
            then 'Non-ICU Medical IP'
        when lower(accepting_service) = 'neonatology'
            then 'NICU'
        when lower(accepting_service) = 'critical care'
            then 'PICU'
        when lower(accepting_service) = 'cardiac critical care'
            then 'CICU'
        when lower(accepting_service) in ('unknown', 'other', 'not applicable')
            then 'TBD'
        when accepting_service is null and accepting_department is null
            then null
        else 'Non-ICU Medical IP'
    end as accepting_service_grouped,

    case
        when lower(stg_transport_all_encounters.initial_service)   like '%emergency%'
            then 'ED'
        when lower(stg_transport_all_encounters.initial_service)   like '%cardiac icu%'
            or lower(stg_transport_all_encounters.initial_service) like '%cicu%'
            then 'CICU'
        when lower(stg_transport_all_encounters.initial_service)   like '%progressive care%'
            then 'PCU'
        when lower(stg_transport_all_encounters.initial_service)   like '%surgery%'
            or lower(stg_transport_all_encounters.initial_service) like '%transplant%'
            or lower(stg_transport_all_encounters.initial_service) like '%anesthesia%'
            or lower(stg_transport_all_encounters.initial_service) like '%ophthal%'
            or lower(stg_transport_all_encounters.initial_service) like '%ortho%'
            or lower(stg_transport_all_encounters.initial_service) like '%otolaryngology%'
            or (lower(stg_transport_all_encounters.initial_service) like '%urology%'
                and lower(stg_transport_all_encounters.initial_service) not like '%neurology%')
            or lower(stg_transport_all_encounters.initial_service) like '%trauma%'
            then 'Non-ICU Surgical IP'
        when lower(stg_transport_all_encounters.initial_service)   like '%general%'
            or lower(stg_transport_all_encounters.initial_service) like '%adolescent%'
            or lower(stg_transport_all_encounters.initial_service) like '%allergy%'
            or lower(stg_transport_all_encounters.initial_service) like '%onco%'
            or lower(stg_transport_all_encounters.initial_service) like '%gastro%'
            or lower(stg_transport_all_encounters.initial_service) like '%metabol%'
            or lower(stg_transport_all_encounters.initial_service) like '%rheum%'
            or lower(stg_transport_all_encounters.initial_service) like '%endocrin%'
            or lower(stg_transport_all_encounters.initial_service) like '%hematol%'
            or lower(stg_transport_all_encounters.initial_service) like '%nephrol%'
            or lower(stg_transport_all_encounters.initial_service) like '%pulm%'
            or lower(stg_transport_all_encounters.initial_service) like '%psych%'
            or lower(stg_transport_all_encounters.initial_service) like '%immun%'
            or lower(stg_transport_all_encounters.initial_service) like '%infectious%'
            or lower(stg_transport_all_encounters.initial_service) like '%neuro%'
            or lower(stg_transport_all_encounters.initial_service) like '%cardio%'
            then 'Non-ICU Medical IP'
        when lower(stg_transport_all_encounters.initial_service)   like '%neonatology%'
            then 'NICU'
        when lower(stg_transport_all_encounters.initial_service)   like '%critical care%'
            or lower(stg_transport_all_encounters.initial_service) like '%complex care%'
                then 'PICU'
        when lower(stg_transport_all_encounters.initial_service) = 'na'
            then 'TBD'
    end as initial_service_grouped,

    case
        when lower(stg_transport_all_encounters.transfer_evaluation_service)   like '%emergency%'
            then 'ED'
        when lower(stg_transport_all_encounters.transfer_evaluation_service)   like '%cardiac icu%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%cicu%'
            then 'CICU'
        when lower(stg_transport_all_encounters.transfer_evaluation_service)   like '%progressive care%'
            then 'PCU'
        when lower(stg_transport_all_encounters.transfer_evaluation_service)   like '%surgery%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%transplant%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%anesthesia%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%ophthal%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%ortho%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%otolaryngology%'
            or (lower(stg_transport_all_encounters.transfer_evaluation_service) like '%urology%'
                and lower(stg_transport_all_encounters.transfer_evaluation_service) not like '%neurology%')
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%trauma%'
            then 'Non-ICU Surgical IP'
        when lower(stg_transport_all_encounters.transfer_evaluation_service)   like '%general%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%adolescent%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%allergy%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%onco%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%gastro%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%metabol%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%rheum%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%endocrin%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%hematol%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%nephrol%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%pulm%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%psych%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%immun%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%infectious%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%neuro%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%cardio%'
            then 'Non-ICU Medical IP'
        when lower(stg_transport_all_encounters.transfer_evaluation_service)   like '%neonatology%'
            then 'NICU'
        when lower(stg_transport_all_encounters.transfer_evaluation_service)   like '%critical care%'
            or lower(stg_transport_all_encounters.transfer_evaluation_service) like '%complex care%'
                then 'PICU'
        when lower(stg_transport_all_encounters.transfer_evaluation_service) = 'na'
            then 'TBD'
    end as transfer_evaluation_service_grouped

from
    {{ ref('stg_transport_all_encounters') }} as stg_transport_all_encounters
    left join {{ ref('stg_transport_admit_department') }} as stg_transport_admit_department on
            stg_transport_admit_department.admit_visit_key = stg_transport_all_encounters.admit_visit_key
    left join {{ref('capacity_intercampus_transfers')}} as capacity_intercampus_transfers
        on capacity_intercampus_transfers.transport_key = stg_transport_all_encounters.transport_key
