with setup as(
    select
        accountmrn__c as mrn,
        diagnosis_code__c as diagnosis_code,
        date(strleft(old_created_date__c,10)) as old_create_date,
        stagename,
        diagnosiscodedescription__c,
        id,
        row_number() over (partition by id order by id) as id_row
    from
        {{ source('salesforce_ods', 'salesforce_opportunity') }} as salesforce_opportunity
        inner join {{ ref('lookup_cancer_center_gps_referral_dx')}} as lookup_cancer_center_gps_referral_dx
            /* Requires a clinical diagnosis on lookup list */
            on
                (
                    lower(
                        salesforce_opportunity.diagnosiscodedescription__c
                    ) like lookup_cancer_center_gps_referral_dx.dx
                and lower(salesforce_opportunity.diagnosiscodedescription__c)  not like '%bmt donor%')
            /* Requires a referral diagnosis on lookup list */
            or (
                lower(
                    salesforce_opportunity.referral_diagnosis_description__c
                ) like lookup_cancer_center_gps_referral_dx.dx
                and lower(salesforce_opportunity.referral_diagnosis_description__c)  not like '%bmt donor%')
            or  (lower(department__c) = 'oncology'
                and accountmrn__c is not null
                and (
                    (
                        lower(diagnosiscodedescription__c) not like '%bmt donor%'
                        or diagnosiscodedescription__c is null
                    )
                and lower(referral_diagnosis_description__c)  not like '%bmt donor%')
                and (other_diagnosis__c != 'test' or other_diagnosis__c is null) -- excludes test patients
                )
    where
        (source__c in ('IM', 'IM2') or source__c is null)
        and accountid not in ('001f100001Qnb61AAB', '001j000000doeLlAAI', '0013Z00001jjoycQAA')
)

select
    setup.mrn,
    patient_name,
    old_create_date,
    stagename,
    id,
    case when lower(stagename) like ('closed successful%') then 1
      when lower(stagename) = 'current patient' then 1
      else 0 end
      as converted_ind,
    'Global Patient Services' as drill_down,
    {{
    dbt_utils.surrogate_key([
        'id',
        'drill_down'
        ])
    }} as primary_key
from
    setup
    left join {{ ref('stg_patient') }} as stg_patient
            on setup.mrn = stg_patient.mrn
where id_row = 1
