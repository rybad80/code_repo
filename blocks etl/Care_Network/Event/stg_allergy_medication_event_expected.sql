{{ config(materialized='table', dist='csn') }}

with event_inclusion as (
    select
    csn,
    case when visit_type_id  in ('1794', --new single ventricle
                                '1795')--fol up single ventricle
               and department_care_network.department_id in ('89273011', --wood gastroenterology
                                                            '101012122')--bgr gastroenterology
        then 1 else 0 end as additional_drop
    from
         {{ ref('stg_encounter') }} as stg_encounter
        inner join {{ ref('department_care_network') }} as department_care_network
          on department_care_network.dept_key = stg_encounter.dept_key
        inner join {{source('cdw','provider')}} as provider
          on provider.prov_key = stg_encounter.prov_key
        inner join {{ source('clarity_ods', 'clarity_ser') }} as clarity_ser
          on clarity_ser.prov_id = provider.prov_id
    where
        encounter_type_id in (101)
        and appointment_status_id in (2, 6)--completed, arrived
        and record_status_active_ind = 1  -- department is active in records
        and professional_billing_ind = 0 --not a billing department
        and lower(visit_type) not like '%research%'
        and visit_type_id not in ('1614', -- NEW EATING DISORDER
                                  '1706', -- Fetal Echo
                                  '2102', -- XTRAC LASER
                                  '2103', -- UVB THERAPY
                                  '2210', -- AHM T1Y1 CLASS
                                  '2237', -- Pump Class
                                  '2238', -- Advanced Pump Class
                                  '2248', -- DIABETES EDUCATION
                                  '2259', -- DIABETES EDUCATION less than 30 mins
                                  '2292', -- DIABETES EDUCATION T1Y1
                                  '2369', -- GROUP BARIATRIC
                                  '2446', -- EATING DISORDER FOLLOW UP
                                  '2481', -- TMS PROCEDURE
                                  '2826', -- AADP New
                                  '2827', -- AADP Fol
                                  '3226', -- Keto Class
                                  '3250', -- RNS FOLLOW UP
                                  '9400', -- NEW EATING DISORDER
                                  '600227', -- PET
                                  '6001227', -- PET SED
                                  '6002227' -- PET ANES
                                   )
        and (provider.prov_type in ('Resident',
                            'Physician',
                            'Registered Nurse',
                            'FELLOW',
                            'Physician Assistant',
                            'Nurse Practitioner')--provider types that we care about
            or (provider.prov_type not in ('Social Worker',
                                  'Physical Activity Specialist',
                                  'DIETICIAN',
                                  'Psychologist')
                  or (lower(department_care_network.specialty_name) = 'behavioral health services'))
                  --behavioral health exception
             )
)
 select
   csn,
   1 as event_expected
 from
    event_inclusion
 where
    additional_drop = 0
