with sites as (
    select
        kaps_feedback_target.target_id,
        kaps_feedback_target.issue_id,
        case
            when kaps_feedback_target.program = '4 West ? CSH- Adolescent'
                then '4 West CSH - Adolescent'
            when kaps_feedback_target.program = 'Children?s Intensive Emotional Behavioral Program (CIEBP)'
                then 'Childrens Intensive Emotional Behavioral Program (CIEBP)'
            when kaps_feedback_target.program = 'Mount Laurel NJ'
                then 'Moorestown NJ'
            when kaps_feedback_target.program = 'n/a'
                then kaps_feedback_user_defined_field.primary_care_area
            else kaps_feedback_target.program end as care_service_area,
        case
            when care_service_area like '%3550%'
                or care_service_area in (
                'Child & Adolescent Psychiatry & Behavioral Sciences (DCAPBS)', 'Division of Child Development',
                'Healthy Weight Program', 'Gender & Sexuality Development Clinic', 'DEVELOPMENTAL PEDS',
                'Autism Integrated Care Program', 'Adolescent Clinic', 'Developmental and Behavioral Pediatrics',
                'Child Development Center', 'Childrens Intensive Emotional Behavioral Program (CIEBP)')
                then '3550 Market'
            when care_service_area like '%3440%' then '3440 Market'
            when care_service_area like 'Abington%' then 'Abington'
            when care_service_area like 'Atlantic County%' then 'Atlantic County NJ'
            when care_service_area like '%BWV%' or care_service_area like '%Brandywine%'
                then 'Brandywine'
            when care_service_area like '%BGR%' or care_service_area like '%BCC%'
                or care_service_area like '%Buerger%'
                or care_service_area in ('Allergy Clinic', 'Anesthesia', 'Audiology', 'Behavioral Health',
                                    'Cerebral Palsy', 'Day Hospital', 'Day Medicine', 'Dermatology Clinic',
                                    'Endocrinology Clinic', 'ENT', 'ENT Clinic', 'General Pediatrics',
                                    'Genetics Clinic', 'Hematology Clinic', 'Immunology Clinic',
                                    'Metabolic Clinic', 'Neonatal Follow-up Program', 'Neurology',
                                    'Neurology Clinic', 'Neurosurgery Clinic', 'Occupational Therapy',
                                    'Oncology Clinic', 'Oncology Day Hospital', 'Ophthalmology',
                                    'Ophthalmology Clinic', 'Ortho Clinic', 'Orthopedic Clinic',
                                    'Physical Therapy', 'Plastic Surgery Clinic', 'Pulmonary Clinic',
                                    'Radiology/Diagnostic', 'Rehab Day Hospital', 'Rehab Medicine Outpatient',
                                    'Rheumatology Clinic', 'Speech', 'Speech Therapy', 'Surgical Clinic',
                                    'Urology Clinic')
                then 'Buerger'
            when care_service_area like '%Bryn Mawr%' then 'Bryn Mawr'
            when (care_service_area like '%Bucks%' or care_service_area like '%BUC%')
                and care_service_area != 'Central Bucks'
                then 'Bucks County'
            when care_service_area like '%Exton%' then 'Exton'
            when care_service_area like '%LGH%' then 'Lancaster'
            when care_service_area like '%KOP%' or care_service_area like '%King of Prussia%'
                then 'King of Prussia'
            when care_service_area like 'Princeton%' then 'Princeton NJ'
            when care_service_area like 'Virtua%' then 'Virtua'
            when care_service_area like 'Voorhees%' or care_service_area like 'VPF%'
                then 'Voorhees'
            when care_service_area like '%Wood%' or care_service_area = 'Spina Bifida Clinic'
                then 'Wood'
            when care_service_area = 'Cardiac Center at St Peters'
                then 'St Peters'
            when care_service_area = 'Home Care' then 'Home Care'
            when care_service_area like '%PCC%' then care_service_area
            when regexp_like(
                    care_service_area,
                    '\d|'
                    || 'Access Services|Atrium|Anesthesia|Bed Management|Case Management|Cardiac ECHO Lab|'
                    || 'Cardiac EKG Lab|Cardiac Intake Center|Cardiology Clinic|Chaplaincy|Child Life|CICU|'
                    || 'Connelly|CPRU|Dental|ECMO|EDECU|Environmental|Emergency|Experience|Facilities|'
                    || 'Fetal OB|^Feeding|Fetal Diagnosis|GI Endoscopy Suite|Infectious Disease Clinic|'
                    || 'International|Interventional Radiology|IV Team|LAB|Main|Materials|'
                    || 'Day Hospital Feeding Program|Nuclear Med|Nephrology Clinic|NIC|Nutrition|'
                    || 'PACU|Global Patient Services|CT Scan|Asplundh Welcome Center|'
                    || 'Anesthesia Resource Center (ARC)|^OR$|^OR |Pathology|Pharmacy|'
                    || 'Progressive Care|PICU|SDU|PICU|Security|Sedation|Sleep|Social Work|'
                    || 'Transplant|TELEHEALTH URGENT CARE'
                 ) then 'Main'
            when regexp_like(
                    care_service_area,
                    '^(After Hours|After Hours Program|Broomall|Cape May|Central Bucks|Chadds Ford|Chestnut Hill|'
                    || 'Coatesville|Drexel Hill|Flourtown|Gibbsboro Care Network|'
                    || 'Haverford|High Point|Indian Valley|Moorestown NJ|'
                    || 'Kennett Square|Media|Mount Laurel NJ|Newtown|Norristown|'
                    || 'North Hills|Paoli|Pottstown|Roxborough|Salem Road NJ|Smithville NJ|'
                    || 'Somers Point NJ|Springfield|West Chester|'
                    || 'CHOP Campus / Faculty Practice|West Grove)$'
                    /*append PCC so you can group these for service*/
                    ) then care_service_area || ' (PCC)'
            end as building,
        case
            when building = 'Home Care' then 'Home Care'
            when care_service_area in ('Emergency Department', 'EDECU')
                then 'Emergency Department'
            when lower(care_service_area) = 'telehealth urgent care'  or care_service_area like '%Urgent Care%'
                then 'Urgent Care'
            when regexp_like(
                    care_service_area,
                    '\d|'
                    || '12NW|1E Observation Unit|3 East - CSH|3 West - CSH|3C / 3E - Oncology|3ECSH - MHT|'
                    || '3ECSH - SSU|3S - Oncology|4 East CSH - Medical Behavioral Unit|4 West - CSH|'
                    || '4 West CSH - Adolescent|4E|4S|5E|5S|5w-a|5w-b|6E-CCU|7W MHT|8S|9S|'
                    || 'Progressive Care Unit|NIC-C|NIC-NE|SDU|PACU|CPRU|NICU|CICU|NIC-East|'
                    || 'PICU - 7  West/7 Central|PICU'
                    ) then 'Philadelphia Campus - Inpatient'
            when (building = 'Wood' or building like '%Seashore%')
                then 'Philadelphia Campus - Inpatient'
            when lower(care_service_area) in ('atrium', 'access services (admissions)', 'bed management',
                        'cardiac echo lab', 'cardiac ekg lab', 'case management', 'chaplaincy', 'child life',
                        'connelly center', 'emergency transport', 'environmental', 'environmental services',
                        'facilities management', 'garage - main', 'global patient services',
                        'international medicine/international patient services', 'iv team - venous access service',
                        'lab - central laboratory services', 'lab - clinical hematology', 'lab - phlebotomy',
                        'main cafeteria', 'materials distribution', 'nutrition', 'nutrition services', 'pathology',
                        'patient & family experience', 'pharmacy - outpatient', 'security', 'social work')
                then 'Other'
            when care_service_area in ('Child & Adolescent Psychiatry & Behavioral Sciences (DCAPBS)',
                        'Childrens Intensive Emotional Behavioral Program (CIEBP)')
                then 'Behavioral Health'
            when regexp_like(
                    care_service_area,
                    '\d|'
                    || 'Feeding Program|Main BHIP|Sleep Center|Triosomy 21|Nephrology Clinic|'
                    || 'GI/Nutrition Clinic|Sleep Lab|Center for Fetal Diagnosis/Treatment|'
                    || 'Dental Clinic|OR Cardiothoracic|OR|Interventional Radiology|MRI-Wood|'
                    || 'Division of Child Development|Speech Therapy|Healthy Weight Program|'
                    || 'Allergy Clinic|Hematology Clinic|Ophthalmology Clinic|Behavioral Health|'
                    || 'Dermatology Clinic|Spina Bifida Clinic|Ortho Clinic|Rehab Day Hospital|'
                    || 'Orthopedic Clinic|Occupational Therapy|ENT Clinic|ehealth|Endocrinology Clinic|'
                    || 'Gender & Sexuality Development Clinic|Urology Clinic|Cardiac Intake Center|'
                    || 'Fetal OB Ultrasound|Day Hospital Feeding Program|Neurology Clinic|Oncology Clinic|'
                    || 'Plastic Surgery Clinic|Ophthalmology|Oncology Day Hospital|DEVELOPMENTAL PEDS|'
                    || 'Neurosurgery Clinic|Metabolic Clinic|Nuclear Med|CT Scan|Day Hospital|'
                    || 'Immunology Clinic|Autism Integrated Care Program|Cardiac Center at St Peters|'
                    || 'Speech|Radiology/Diagnostic|Cardiology Clinic|Physical Therapy|Cerebral Palsy|'
                    || 'Neonatal Follow-up Program|Adolescent Clinic|Infectious Disease Clinic|'
                    || 'Rehab Medicine Outpatient|Neurology|Surgical Clinic|Audiology|Rheumatology Clinic|'
                    || 'ENT|Developmental and Behavioral Pediatrics|Child Development Center|Anesthesia|'
                    || 'Genetics Clinic|Day Medicine|Pulmonary Clinic|Cardiac Intake Center|Fetal OB Ultrasound|'
                    || 'Day Hospital Feeding Program|Nuclear Med|CT Scan|Cardiology Clinic|GI Endoscopy Suite|'
                    || 'Main Palliative Care|Sedation Center|ECMO Program|Transplant Center|'
                    || 'Asplundh Welcome Center'
                    ) then 'Specialty Care Center'
            when care_service_area like ('PCC') or building like '%PCC%'
                then 'Care Network'
            when building in ('3440 Market', '3550 Market', 'Abington', 'Atlantic County NJ',
                        'Bucks County', 'Buerger', 'Brandywine', 'Bryn Mawr', 'Exton', 'Lancaster',
                        'King of Prussia', 'Princeton NJ', 'Virtua', 'Voorhees', 'Wood', 'St Peters'
                    ) then 'Specialty Care Center'
            when building = 'n/a' or building is null then 'Other'
            else 'n/a'
            end as service
    from
        {{source('cdw', 'kaps_feedback_cases')}} as kaps_feedback_cases
        left join {{source('cdw', 'kaps_feedback_user_defined_field')}} as kaps_feedback_user_defined_field
            on kaps_feedback_cases.feedback_id = kaps_feedback_user_defined_field.feedback_id
        left join {{source('cdw', 'kaps_feedback_issue')}} as kaps_feedback_issue
            on kaps_feedback_cases.feedback_id = kaps_feedback_issue.feedback_id
        left join {{source('cdw', 'kaps_feedback_target')}} as kaps_feedback_target
            on kaps_feedback_issue.issue_id = kaps_feedback_target.issue_id
    where
        kaps_feedback_cases.submission_dt >= '2019-01-01'
--end region
)

select distinct
    kaps_claim_file.file_id,
    kaps_feedback_cases.feedback_id,
    coalesce(stg_patient.patient_name,
        initcap(kaps_feedback_subject.last_name || ', ' || kaps_feedback_subject.first_name))
        as patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_patient.preferred_name,
    stg_patient.race_ethnicity,
    initcap(stg_patient.preferred_language) as preferred_language,
    kaps_feedback_cases.submission_dt as feedback_date_received,
    kaps_claim_file.enter_dt as feedback_date_entered,
    kaps_feedback_user_defined_field.write_off_close_dt as feedback_date_closed,
    date(kaps_feedback_user_defined_field.write_off_close_dt)
        - date(kaps_claim_file.enter_dt) as days_to_close_entered,
    date(kaps_feedback_user_defined_field.write_off_close_dt)
        - date(kaps_feedback_cases.submission_dt ) as days_to_close_received,
    kaps_feedback_cases.method as feedback_method,
    kaps_feedback_issue.description as feedback_description,
    kaps_feedback_user_defined_field.primary_issue_classification as primary_classification,
    sites.care_service_area as feedback_department,
    sites.building as feedback_building,
    sites.service as care_service_area,
    /*enterprise leader/site where case is handled*/
    kaps_feedback_user_defined_field.primary_file_owner,
    kaps_feedback_cases.referral_source,
    kaps_claim_file.file_stat as file_status,
    kaps_claim_file.file_state,
    /*office of feedback staff member that created KAPS file*/
    initcap(kaps_master_user.user_full_nm) as feedback_owner_name,
--    stg_patient.pat_id,
    stg_patient.pat_key,
    'KAPS' as create_by
from
    {{source('cdw', 'kaps_claim_file')}} as kaps_claim_file
        left join {{source('cdw', 'kaps_master_module')}} as kaps_master_module
            on kaps_master_module.mstr_module_key = kaps_claim_file.mstr_module_key
        left join {{source('cdw', 'kaps_file_state')}} as kaps_file_state
            on kaps_file_state.file_id = kaps_claim_file.file_id
        left join {{source('cdw', 'kaps_master_user')}} as kaps_master_user
            on kaps_master_user.mstr_user_key = kaps_claim_file.mstr_owner_user_key
        left join {{source('cdw', 'kaps_feedback_cases')}} as kaps_feedback_cases
            on kaps_feedback_cases.file_id = kaps_claim_file.file_id
        left join {{source('cdw', 'kaps_feedback_issue')}} as kaps_feedback_issue
            on kaps_feedback_issue.feedback_id = kaps_feedback_cases.feedback_id
        left join sites
            on sites.issue_id = kaps_feedback_issue.issue_id
        left join {{source('cdw', 'kaps_feedback_subject')}} as kaps_feedback_subject
            on kaps_feedback_cases.feedback_id = kaps_feedback_subject.feedback_id
        left join {{source('cdw', 'kaps_feedback_user_defined_field')}} as kaps_feedback_user_defined_field
            on kaps_feedback_user_defined_field.feedback_id = kaps_feedback_cases.feedback_id
        left join {{ref('stg_patient')}} as stg_patient
            on stg_patient.mrn = kaps_feedback_subject.file_number
        left join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.pat_key = stg_patient.pat_key
where
    kaps_master_module.module_id = 2
    and lower(kaps_file_state.file_state) not in ('deleted-inc', 'deleted', 'incomplete')
    /*blank test file*/
    and kaps_claim_file.file_id != '86933'
    /*mickey mouse test file*/
    and kaps_feedback_cases.feedback_id not in ('1', '393')
    and feedback_date_received >= '2019-01-01'
