{{ config(meta = {
    'critical': true
}) }}

with office_visit_ind as (
    select
        stg_office_visit_grouper.department_id,
        max(stg_office_visit_grouper.physician_app_psych_visit_ind) as physician_app_psych_visit_ind
    from
        {{ref('stg_office_visit_grouper')}} as stg_office_visit_grouper
    group by
        stg_office_visit_grouper.department_id
),

fill_rate as (
select
    case
        when
            (
            lower(provider.prov_type) in ('physician',
                                            'midwife',
                                            'nurse practitioner',
                                            'physician assistant')
            or lower(provider.prov_type) like '%psycholog%'
            )
            and office_visit_ind.physician_app_psych_visit_ind = 1
        then 1
        /*
        * Licensed social workers for Behavioral Health are included in this grouper
        */
        when
            lower(stg_department_all.specialty_name) = 'behavioral health services'
            and lower(provider.prov_type) = 'social worker'
            and provider.title = 'LCSW'
            and office_visit_ind.physician_app_psych_visit_ind = 1
        then 1
        else 0
    end as physician_app_psych_ind,
    stg_department_all.specialty_name,
    stg_department_all.dept_key,
    stg_department_all.department_name,
    stg_department_all.department_id,
    stg_department_all.intended_use_name,
    provider.prov_key,
    provider.prov_type as provider_type,
    max(employee.active_ind) as active_ind,
    dim_appt_block.dim_appt_block_key,
    dim_appt_block.appt_block_id,
    dim_appt_block.appt_block_nm
from
    {{source('cdw', 'provider_availability')}} as provider_availability
      inner join {{ref('stg_department_all')}} as stg_department_all
            on provider_availability.dept_key = stg_department_all.dept_key
      inner join {{source('cdw', 'provider')}} as provider
            on provider_availability.prov_key = provider.prov_key
      left join {{source('cdw', 'employee')}} as employee
            on provider.prov_key = employee.prov_key
      left join {{source('cdw', 'available_block')}} as available_block
            on provider_availability.dept_key = available_block.dept_key
            and provider_availability.prov_key = available_block.prov_key
            and provider_availability.slot_start_tm = available_block.slot_begin_dt
            and available_block.seq_num = 1
      left join {{source('cdw', 'dim_appt_block')}} as dim_appt_block
            on available_block.dim_appt_block_key = dim_appt_block.dim_appt_block_key
      left join office_visit_ind as office_visit_ind
            on stg_department_all.department_id = office_visit_ind.department_id
group by
    physician_app_psych_ind,
    stg_department_all.specialty_name,
    stg_department_all.dept_key,
    stg_department_all.department_name,
    stg_department_all.department_id,
    stg_department_all.intended_use_name,
    provider.prov_key,
    provider_type,
    dim_appt_block.dim_appt_block_key,
    dim_appt_block.appt_block_id,
    dim_appt_block.appt_block_nm
),

tna_fill_rate_ind as (
    select distinct
        fill_rate.dept_key as dept_key,
        fill_rate.prov_key as prov_key,
        fill_rate.dim_appt_block_key,
        fill_rate.intended_use_name,
        fill_rate.specialty_name,
        fill_rate.physician_app_psych_ind,
        case
              when lower(fill_rate.intended_use_name) = 'primary care'
              and lower(fill_rate.provider_type) in (
                  'physician',
                  'nurse practitioner'
                ) then 1
              when lower(fill_rate.specialty_name) not in (
                      'adolescent initiative',
                      'behavioral health services',
                      'cardiology',
                      'cardiovascular surgery',
                      'critical care',
                      'family planning',
                      'general pediatrics',
                      'general surgery',
                      'gi/nutrition',
                      'home care',
                      'multidisciplinary',
                      'neonatology',
                      'obstetrics',
                      'other',
                      'pulmonary',
                      'plastic surgery',
                      'toxicology',
                      'urgent care',
                      'wood anes pain mgmt',
                      /*
                      * Ancillary services
                      */
                      'audiology',
                      'clinical nutrition',
                      'occupational therapy',
                      'physical therapy',
                      'speech'
                    ) then 1
          /*
          * Ancillary services (not limited to phys/app/psych visits)
          */
              when lower(fill_rate.provider_type) in (
                      'audiologist',
                      'fellow',
                      'nurse practitioner',
                      'occupational therapist',
                      'physical therapist',
                      'physician',
                      'physician assistant',
                      'speech therapist')
                  and lower(fill_rate.specialty_name) in (
                      'audiology',
                      'occupational therapy',
                      'physical therapy'
                    ) then 1
              when lower(fill_rate.specialty_name) = 'clinical nutrition'
                  and lower(fill_rate.provider_type) in (
                    'dietician',
                    'nutritionist',
                    'physician'
                ) then 1
              when lower(fill_rate.specialty_name) = 'speech'
                  and lower(fill_rate.provider_type) in (
                    'speech therapist'
                ) then 1
          --end ancillary services
          when lower(fill_rate.specialty_name) = 'general surgery'
              and fill_rate.department_id not in (
                '101001161', --bgr gen surg clinic
                '101013020', --mkt 3550 ped gen thor
                '101013011', --main cfdt
                '101001030' --wood gen surg clinic
            ) then 1
              when
                  lower(fill_rate.specialty_name) = 'behavioral health services'
                  and (fill_rate.department_id != '92103543' --atl bh day hospital
                  and fill_rate.active_ind = 1
                ) then 1
              when
                  lower(fill_rate.specialty_name) = 'plastic surgery'
                  and (lower(fill_rate.provider_type) != '%psycholog%'
                ) then 1
              when lower(fill_rate.specialty_name) = 'neonatology'
                  and fill_rate.department_id not in (
                    '101012046', --main neonatology consult
                    '10801021' --mkt 3550 special babies
                ) then 1
              when lower(fill_rate.specialty_name) = 'cardiology'
                  and fill_rate.department_id not in (
                    '101012017', --hup cardiology
                    '101012033', --lady of lourdes
                    '101022019' --princeton med ctr card
                  ) then 1
              when lower(fill_rate.specialty_name) = 'pulmonary'
                  and fill_rate.appt_block_id in (
                    '215', --new transplant patient visit
                    '216', --new transplant visit with spirometry
                    '217', --follow up transplant visit with spirometry
                    '236', --follow up transplant visit
                    '316', --php new
                    '317', --PHP Follow up
                    '513', --follow-up airway
                    '514', --follow-up airway with spirometry
                    '515', --new airway
                    '516', --new airway spirometry
                    '834' --neuro muscular multi discp
                   ) then 0
              when lower(fill_rate.specialty_name) = 'pulmonary'
                  and lower(fill_rate.provider_type) = 'nurse practitioner'
                  then 0
              when lower(fill_rate.specialty_name) = 'pulmonary'
                  then 1
                  else 0
            end as tna_fill_rate_incl_ind
    from fill_rate
)
select
    tna_fill_rate_ind.dept_key as dept_key,
    tna_fill_rate_ind.prov_key as prov_key,
    tna_fill_rate_ind.dim_appt_block_key as appt_block_key,
    case when tna_fill_rate_ind.tna_fill_rate_incl_ind = 1
       and (lower(tna_fill_rate_ind.intended_use_name) = 'primary care'
            or lower(tna_fill_rate_ind.specialty_name) in ('audiology',
            'clinical nutrition', 'occupational therapy', 'physical therapy',
            'speech'))
        then 1
        when tna_fill_rate_ind.tna_fill_rate_incl_ind = 1 and tna_fill_rate_ind.physician_app_psych_ind = 1
        then 1
        else 0
        end as fill_rate_incl_ind,
    tna_fill_rate_ind.tna_fill_rate_incl_ind
from tna_fill_rate_ind
