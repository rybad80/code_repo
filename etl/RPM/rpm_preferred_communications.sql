with patient_proxy_wpr as ( --region this CTE pulls proxy_ids for the patient and the guardian/parents
    select
        stg_rpm_patient.pat_id,
        patient_myc.mypt_id as proxy_wpr_id,
        patient_myc.mypt_id
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'patient_myc') }} as patient_myc
            on patient_myc.pat_id = stg_rpm_patient.pat_id
                and patient_myc.mychart_status_c = 1
                and (patient_myc.code_for_proxy_yn != 'Y'
                    or patient_myc.code_for_proxy_yn is null)
        inner join {{ source('clarity_ods', 'myc_patient') }} as myc_patient
            on myc_patient.pat_id = stg_rpm_patient.pat_id
                and myc_patient.mypt_id = patient_myc.mypt_id
                and (myc_patient.proxy_account_yn != 'Y'
                    or myc_patient.proxy_account_yn is null) -- not a proxy account, patient's wpr id
),

non_pat_proxy_wpr as (
	select
		stg_rpm_proxy_raw.pat_id,
		stg_rpm_proxy_raw.proxy_wpr_id,
		patient_myc.mypt_id,
        stg_rpm_proxy_raw.access_ecl_id
	from
        {{ref('stg_rpm_proxy_raw') }} as stg_rpm_proxy_raw
		inner join {{ source('clarity_ods', 'patient_myc') }} as patient_myc
			on stg_rpm_proxy_raw.pat_id = patient_myc.pat_id
			and stg_rpm_proxy_raw.rn = 1
--end region
),

security_class as (
	select
        non_pat_proxy_wpr.pat_id,
        non_pat_proxy_wpr.proxy_wpr_id,
        clarity_ecl.classifctn_name as security_class
	from non_pat_proxy_wpr
        inner join {{ source('clarity_ods', 'clarity_ecl') }} as clarity_ecl
            on non_pat_proxy_wpr.access_ecl_id = clarity_ecl.ecl_id
),

relationships as (
    select
        stg_rpm_contacts.pat_id,
        stg_rpm_contacts.patient_over_16_phone_confirmed,
        stg_rpm_contacts.patient_over_16_phone_probable,
        stg_rpm_contacts.non_patient_phone,
        stg_rpm_contacts.patient_over_16_email,
        stg_rpm_contacts.non_patient_email,
        pat_relationships.pat_rel_mobile_phne,
        pat_relationships.pat_rel_name,
        zc_pat_relation.name,
        case
            when lower(zc_pat_relation.name) like 'self%'
            then 1 else 0
        end as patient_proxy_ind,
        case
            when lower(zc_pat_relation.name) not like 'self%'
            then 1 else 0
        end as parent_guardian_proxy_ind
    from
        {{ref('stg_rpm_contacts') }} as stg_rpm_contacts
        inner join {{ source('clarity_ods', 'pat_relationships') }} as pat_relationships
            on pat_relationships.pat_id = stg_rpm_contacts.pat_id
                and pat_relationships.line = '1'
        inner join {{ source('clarity_ods', 'zc_pat_relation') }} as zc_pat_relation
            on pat_relationships.pat_rel_relation_c = zc_pat_relation.pat_relation_c
)

select
    stg_rpm_patient.patient_key,
    stg_rpm_patient.pat_id,
    stg_rpm_patient.patient_name,
    stg_rpm_patient.current_age,
    relationships.patient_over_16_email,
    relationships.non_patient_email,
    replace(stg_rpm_patient.home_phone, '-', '') as home_phone,
    relationships.patient_over_16_phone_confirmed,
    relationships.patient_over_16_phone_probable,
    relationships.non_patient_phone as non_patient_mobile_phone,
    relationships.name as non_patient_relationship_type,
    stg_rpm_address.delivery_address as rx_delivery_address,
    stg_rpm_address.temporary_address,
    stg_rpm_address.home_address,
    patient_proxy_wpr.proxy_wpr_id as mypt_id,
    non_pat_proxy_wpr.proxy_wpr_id as proxy_wpr_id,
    security_class.security_class,
    stg_rpm_patient.pt_13_and_over_ind,
    stg_rpm_patient.pt_16_and_over_ind,
    stg_rpm_patient.pt_18_and_over_ind,
    case
        when patient_proxy_wpr.proxy_wpr_id is not null
        then coalesce(relationships.patient_proxy_ind, '1') else 0
    end as patient_proxy_ind,
    case
        when non_pat_proxy_wpr.proxy_wpr_id is not null
        then coalesce(relationships.parent_guardian_proxy_ind, '1') else 0
    end as non_patient_proxy_ind
from
    {{ref('stg_rpm_patient') }} as stg_rpm_patient
    left join {{ref('stg_rpm_address') }} as stg_rpm_address
        on stg_rpm_patient.pat_id = stg_rpm_address.pat_id
    left join patient_proxy_wpr
        on stg_rpm_patient.pat_id = patient_proxy_wpr.pat_id
    left join non_pat_proxy_wpr
        on stg_rpm_patient.pat_id = non_pat_proxy_wpr.pat_id
    left join relationships
        on stg_rpm_patient.pat_id = relationships.pat_id
    left join security_class
        on patient_proxy_wpr.pat_id = security_class.pat_id
        and patient_proxy_wpr.proxy_wpr_id = security_class.proxy_wpr_id
