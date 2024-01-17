select
  worker_id,
  preferred_first_name,
	display_name,
	manager_name,
	lower(ad_login) || '@chop.edu' as email_address,
	date(hire_date) as hire_day,
	termination_date as end_day,
	job_title,
	job_family,
  job_family_group,
  management_level,
  provider.prov_type as epic_provider_type,
  provider.title as epic_provider_title,
	job_category,
    -- Classify BH Jobs based on job title, job category, job family, and title
  case when
    job_category in (
    '12 - Clinical Directors',
    '14 - Clinical Mgrs/Supvs',
    '16 - Nurse MgrsSupvs',
    '20 - Physician Leader',
    '202 - Clinical Nurse II',
    '204 - Clinical Nurse Profls',
    '21 - Physicians',
    '22 - Doctors in Training',
    '23 - Clinical Professionals',
    '31 - Clinical/Technicians'
    )
    or (worker_role = 'Penn_Faculty' and job_category != '25 - Research Profls')
    or (job_family_group = 'Behavioral Health' and epic_provider_title is not null)
    or job_title in ('Scoring Clerk', 'Psychometrist')
    or epic_provider_title in ('LCSW', 'LPC')
    or epic_provider_type = 'Behavioral Health Clinician'
    then 1 else 0 end as clinical_ind,
  case
    when
      (clinical_ind = 1 and job_family = 'Students & Interns')
      or job_title = 'Psychology Extern' then 'Student/Extern/Intern'
    when job_title = 'Resident' then 'Resident'
    when regexp_like(job_title, 'Fellow(?!s)') then 'Fellow'
    else null end as training_level,
  case
    when
        (
          (job_title like '%Dir%' or job_title like '%Chief%')
          and job_title not like '%Secretary%'
        )
        or (
          job_category in (
            '12 - Clinical Directors',
            '16 - Nurse MgrsSupvs',
            '20 - Physician Leader',
            '14 - Clinical Mgrs/Supvs'
            )
          )
        or (
            clinical_ind = 1 and management_level in (
              'Department Chair',
              'Division Chief / Assoc Chair',
              'MedDir / PgmDir / Assoc Chief',
              'Mid-Management'
            )
        )
     then 1 else 0 end as leadership_ind,
  case when
    job_category in (
    '25 - Research Profls',
    '33 - Research Techs'
    )
    or job_title like '%Research%'
    then 1 else 0 end as research_ind,
  case when job_title = 'Behavioral Health LCSW'
      or epic_provider_title = 'LCSW'
     then 'LCSW'
    when
      job_title like '%Board Certified Behavioral Analyst%'
        or job_title like '%BCBA%'
        or epic_provider_type = 'Behavior Analyst'
      then 'BCBA'
    when
      job_title like '%Neuropsych%'
      or (worker_role = 'Penn_Faculty' and cost_center_id = '14535') -- Behavioral Health Neuropsychology
      then 'Neuropsychologist'
    when
        job_title like '%Psycholog%'
        or epic_provider_type = 'Psychologist'
      then 'Psychologist'
    when
        job_title like '%Psychiatrist%'
        or epic_provider_type = 'Physician'
        or epic_provider_title in ('MD', 'DO', 'MBBS')
        or job_title like '%Attending%'
        or (job_title like '%Physician%' and clinical_ind = 1)
        or (
            clinical_ind = 1 and cost_center_id = '13270' -- Resident and Fellow Training Psychiatry
        )
      then 'Physician'
    when
      job_family_group = 'Social Work & Family Services'
      or job_family = 'Case Management'
      or job_title like 'Social Work%'
     then 'Social Worker/Case Manager'
    when
      job_family_group = 'Nursing'
      or job_title like '%Nursing%'
      or epic_provider_type in ('Nurse Practitioner', 'Registered Nurse')
      or cost_center_id = '10970' -- Nursing Administration
      then 'Nurse'
    when job_title in ('Psychometrist', 'Scoring Clerk')
      then 'Psychometrist/Scoring Clerk'
    when
    job_title like '%Therapist%'
    or job_family = 'Behavioral Health Counselor'
    or epic_provider_title = 'LPC'
     then 'Counselor/Therapist'
    when job_title like '%Learning%' and epic_provider_type = 'Behavioral Health Clinician'
      then 'Learning Specialist'
    when
      job_title = 'Psychiatric Technician'
      or job_title like 'Behavioral Health Clinician%'
      or job_title like '%Behavior%Spec%'
      or epic_provider_type = 'Behavioral Health Clinician'
      then 'Psychiatric Technician/BHC/Behavioral Specialist'
    when job_title like '%Certified Medical%'
      then 'CMA'
    when research_ind = 1
      then 'Research'
    else null end as bh_role,
	worker_type,
  worker_role,
	cost_center_name,
	cost_center_id,
	cost_center_site_name,
	-- DCAPBS specific information
	case when
  cost_center_id in (
	'10295', -- Behavioral Health Unit (Cedar Ave. BHCs)
  '10910', -- Centralized Behavioral Health Staff (BHCs)
  '10970', -- Nursing Administration
  '41295'  -- Cedar Behavioral Health Unit
  ) then 0 else 1 end as dcapbs_ind,
	-- Group Cost Centers into DCAPBS Divisions and Locations
	case
  when cost_center_id in
  (
  '14545', -- Behavioral Health Pediatric Psychology
  '34545', -- KOP Behavioral Health Pediatric Psychology
  '14505', -- Behavioral Health Inpatient Consultations
  '34505', -- KOP Behavioral Health Inpatient Consultations
  '41505', -- Cedar Behavioral Health Inpatient Consults
  '14535', -- Behavioral Health Neuropsychology
  '34535'  -- KOP Behavioral Health Neuropsychology
    )
   then 'Integrated'
 when cost_center_id in
  (
  '12590', -- Intensive Emotional and Behavioral Program
  '14540'  -- Behavioral Health Healthy Mind and Kids
  )
  then 'Community'
 when cost_center_id in
  (
  '14520', -- Behavioral Health Training
  '13270'  -- Resident and Fellow Training Psychiatry
  )
  then 'DCAPBS Training'
  when dcapbs_ind = 1
  then 'Outpatient'
  end as dcapbs_cost_center_division,
	active_ind,
  provider.prov_key,
  provider.epic_prov_id
from
    {{ref('worker')}} as worker
    left join {{source('cdw', 'provider')}} as provider
      on provider.prov_key = worker.prov_key
      and provider.prov_key is not null
where
      cost_center_id in
      (
      '12590', -- Intensive Emotional and Behavioral Program
      '13455', -- Gender and Sexuality Clinic
      '14500', -- Behavioral Health Operations
      '14505', -- Behavioral Health Inpatient Consultations
      '14510', -- Behavioral Health Outpatient Consultations
      '14511', -- Behavioral Health Eating Disorder IOP
      '14512', -- Behavioral Health Mood PHP
      '14515', -- Behavioral Health ADHD
      '14520', -- Behavioral Health Training
      '14535', -- Behavioral Health Neuropsychology
      '14540', -- Behavioral Health Healthy Mind and Kids
      '14545', -- Behavioral Health Pediatric Psychology
      '14550', -- Behavioral Health Autism Program
      '14555', -- Behavioral Health Eating Disorder PHP
      '34505', -- KOP Behavioral Health Inpatient Consultations
      '34525', -- KOP Behavioral Health Feeding
      '34535', -- KOP Behavioral Health Neuropsychology
      '34545', -- KOP Behavioral Health Pediatric Psychology
      '41500', -- Cedar Behavioral Health Operations (DCAPBS managers)
      '41505', -- Cedar Behavioral Health Inpatient Consults
      '41560', -- Cedar Behavioral Health Crisis Center Outpatient Consults
      '13270'  -- Resident and Fellow Training Psychiatry
      )
      or (
        cost_center_id in (
        '10295', -- Behavioral Health Unit (Cedar Ave. BHCs)
        '41295', -- Cedar Behavioral Health Unit 
        '10910'  -- Centralized Behavioral Health Staff (BHCs)
        )
        and job_category in(
        '12 - Clinical Directors',
        '14 - Clinical Mgrs/Supvs',
        '16 - Nurse MgrsSupvs',
        '202 - Clinical Nurse II',
        '31 - Clinical/Technicians',
        '23 - Clinical Professionals'
        )
      )
      or (
        job_title like '%Behav%Health%'
        and cost_center_id = '10970' -- Nursing Administration
      )
