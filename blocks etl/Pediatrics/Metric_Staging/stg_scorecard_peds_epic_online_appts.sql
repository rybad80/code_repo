select
    visit.visit_key as primary_key,
    coalesce(date(pat_enc_es_aud_act.es_audit_time), date(visit.appt_made_dt)) as appt_made_date,
    department.specialty,
    department.rev_loc_key,
    case
        when pat_enc_es_aud_act.es_audit_user_id in('382', '483')
            then 'mychop' else 'non mychop'
        end as sched_type,
    case
        when
            lower(department.rev_loc_key) in (688, 685)
            then 1 else 0
        end as denominator,
    case
        when
            pat_enc_es_aud_act.es_audit_user_id in('382', '483')
            and department.rev_loc_key in (688, 685) --'chca nj rl', 'chca pa rl'
            then 1 else 0
        end as mychop_appts,
    case
        when
            ((department.rev_loc_key != 681 -- 'children's hospital of philadelphia rl'
            and lower(department.specialty) in (
                        'adolescent',
                        'allergy',
                        'audiology',
                        'behavioral health services',
                        'cardiology',
                        'clinical nutrition',
                        'dermatology',
                        'developmental pediatrics',
                        'endocrinology',
                        'gastroenterology',
                        'genetics',
                        'healthy weight',
                        'hematology',
                        'hematology oncology',
                        'immunology',
                        'infectious disease',
                        'integrative health',
                        'metabolism',
                        'neonatal followup',
                        'neonatology',
                        'nephrology',
                        'neurology',
                        'neurosurgery',
                        'occupational therapy',
                        'ophthalmology',
                        'orthopedics',
                        'otolaryngology',
                        'pediatric general thoracic surgery',
                        'physical therapy',
                        'plastic surgery',
                        'pulmonary',
                        'rehab medicine',
                        'rheumatology',
                        'speech',
                        'urology'
                        )
                )
                or  lower(department.specialty) in (
                        'general pediatrics',
                        'lactation',
                        'oncology',
                        'radiology'
                        )
            )
            then 1 else 0
        end as denom_enterprise_wide,
    case
        when
            pat_enc_es_aud_act.es_audit_user_id in('382', '483')
            and denom_enterprise_wide = 1
            then 1 else 0
        end as mychop_appts_enterprise_wide
from {{ source('cdw', 'visit')}} as visit
    inner join {{ source('cdw', 'department')}} as department
        on visit.dept_key = department.dept_key
    inner join {{ source('cdw', 'cdw_dictionary')}} as cdw_dictionary
        on visit.dict_appt_stat_key = cdw_dictionary.dict_key
    left join {{ source('cdw', 'employee')}} as employee
        on employee.emp_key = visit.appt_entry_emp_key
    left join {{ source('clarity_ods', 'pat_enc_es_aud_act')}} as pat_enc_es_aud_act
        on pat_enc_es_aud_act.pat_enc_csn_id = visit.enc_id
        and pat_enc_es_aud_act.es_audit_action_c in (1, 8) -- 1-Made On, 8-Rescheduled
where
    appt_made_date >= '01/01/2019'
    and lower(cdw_dictionary.dict_nm) not in ('not applicable', 'invalid')
