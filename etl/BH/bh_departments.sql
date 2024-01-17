select
    stg_department_all.dept_key,
    stg_department_all.department_id,
    stg_department_all.department_name,
   clarity_dep.rev_loc_id,
    -- Could replace 'Epic Suspense' with/ 'Generic Cost' center
    cost_center.cost_cntr_nm,
    clarity_dep.rpt_grp_three as cost_center_site_id,
    stg_department_all.department_center as dept_location,
    case
        when
            clarity_dep.gl_prefix = '14540' then 'HMHK'
        when
            stg_department_all.department_id = 101016129 then 'School-based Mental Health'
        when lower(stg_department_all.department_name) like '%aadp%' then 'AADP'
        when lower(stg_department_all.department_name) like '%anxiety%' then 'ABC'
        when lower(stg_department_all.department_name) like '%adhd%' then 'ADHD'
        when clarity_dep.gl_prefix = '14550'
            or lower(stg_department_all.department_name) like '%autism%' then 'AIC'
        when
            lower(stg_department_all.department_name) like '%bhip%'
            or lower(stg_department_all.department_name) like '%consult%'
            or lower(stg_department_all.department_name) = 'inp mental health' then 'BHIP'
        when lower(stg_department_all.department_name) like '%cafe%' then 'Cafe Clinic'
        when lower(stg_department_all.department_name) like '%champ%' then 'CHAMP'
        when lower(stg_department_all.department_name) like '%atl behavioral%' then 'CIEBP'
        when lower(stg_department_all.department_name) like '%atl bh day%' then 'CIEBP'
		when lower(stg_department_all.department_name) like '%bh ciebp%' then 'CIEBP'
        when
            lower(
                stg_department_all.department_name
            ) like '%npsych%' then 'Developmental Neuropsych'
        when lower(stg_department_all.department_name) like '%eat%' then 'Eating Disorder'
        when lower(stg_department_all.department_name) like '%coping%' then 'FSIP'
        when lower(stg_department_all.department_name) like '%gender%' then 'Gender Clinic'
        when lower(stg_department_all.department_name) like '%healthy%' then 'Healthy Weight'
        when
            lower(stg_department_all.department_name) like '%neuropsych%'
            or lower(stg_department_all.department_name) like '%concussion%'
            or lower(stg_department_all.department_name) like '%testing%' then 'Neuropsych'
        when lower(stg_department_all.department_name) like '%pcit%' then 'Disruptive Behavior'
        when
            lower(
                stg_department_all.department_name
            ) in ('wood bh outpatient', 'bgr bh phab') then 'PHAB/CSP'
        when
            lower(stg_department_all.department_name) like '%sud%' then 'Substance Use Disorder'
        when lower(stg_department_all.department_name) like '%tips%' then 'TiPS'
        when lower(stg_department_all.department_name) like '%ycc%' then 'Young Child Clinic'
        when lower(stg_department_all.department_name) like '%adoption%' then 'Peds Adoption'
        when lower(stg_department_all.department_name) like '%allergy%' then 'Peds Allergy'
        when lower(stg_department_all.department_name) like '%audiology%' then 'Peds Audiology'
        when lower(stg_department_all.department_name) like '%cardiac%' then 'Peds Cardiac Kids'
        when lower(stg_department_all.department_name) like '%cardio%' then 'Peds Cardiology'
        when lower(stg_department_all.department_name) like '%cfdt%' then 'Peds CFDT'
        --- should ATL BH day Hospital be included with this or CIEBP?
        when
            lower(
                stg_department_all.department_name
            ) = 'csh bh day hospital' then 'Peds Day Hospital'
        when lower(stg_department_all.department_name) like '%dialysis%' then 'Peds Dialysis'
        when lower(stg_department_all.department_name) like '%endocr%' then 'Peds Endocrinology'
        when lower(stg_department_all.department_name) like '%feed%' then 'Peds Feeding'
        when lower(stg_department_all.department_name) like '%gastro%' then 'Peds Gastro'
        when lower(stg_department_all.department_name) like '%hemat%' then 'Peds Hematology'
        when lower(stg_department_all.department_name) like '%metab%' then 'Peds Metabolism'
        when lower(stg_department_all.department_name) like '%neonat%' then 'Peds Neonatology'
        when lower(stg_department_all.department_name) like '%nephr%' then 'Peds Nephrology'
        when lower(stg_department_all.department_name) like '%oncol%' then 'Peds Oncology'
        when lower(stg_department_all.department_name) like '%pulm%' then 'Peds Pulmonary'
        when
            lower(
                stg_department_all.department_name
            ) like '%si family%' then 'Peds Special Immunology'
        when lower(stg_department_all.department_name) like '%rehab%' then 'Peds Rehabilitation'
        when lower(stg_department_all.department_name) like '%rheum%' then 'Peds Rheumatology'
        when lower(stg_department_all.department_name) like '%neurology%' then 'Peds Neurology'
        when lower(stg_department_all.department_name) like '%proton%' then 'Peds Proton'
        when lower(stg_department_all.department_name) like '%pain%' then 'Peds Pain'
        when lower(stg_department_all.department_name) like '%plastic%' then 'Peds Plastics'
        when
            lower(stg_department_all.department_name) like '%safe place%' then 'Peds Safe Place'
        when
            lower(
                stg_department_all.department_name
            ) like '%special%' then 'Peds Special Babies'
        when
            lower(stg_department_all.department_name) like '%transplant%' then 'Peds Transplant'
        when lower(stg_department_all.department_name) like '%urology%' then 'Peds Urology'
		when stg_department_all.department_name = 'MKT 4601 BH MOOD PHP' then 'Mood'
        end as program,
    case
        when
            program in (
                'CIEBP', 'HMHK', 'School-based Mental Health', 'TiPS'
            ) then 'Community Care and Wellness'
        when program in (
            'ABC',
            'AIC',
            'ADHD',
            'Cafe Clinic',
            'CHAMP',
            'Developmental Neuropsych',
            'Eating Disorder',
            'FSIP',
            'Gender Clinic',
            'Disruptive Behavior',
            'Substance Use Disorder',
            'Young Child Clinic',
            'Mood'
        ) or lower(stg_department_all.department_name) like '%behavioral%' then 'Outpatient Behavioral Health'
        when
            (
                program in (
                    'AADP', 'BHIP', 'Healthy Weight', 'Neuropsych', 'PHAB/CSP'
                ) or program like 'Peds%'
            ) or stg_department_all.department_name in ('ABINGTON BH PED PSYCH', 'VIRTUA BH PEDS PSYCH',
            'KOP BH PED PSYCHOLOGY', 'BUC BH PEDIATRIC PSYCH', 'MKT 3550 BH PED PSYCH')
        then 'Integrated Psychiatry, Psychology & Behavioral Health'
    end as division,
    clarity_dep.phone_number,
    clarity_dep_3.fax_num,
    department_geographical_spatial_info.locator_nm,
    department_geographical_spatial_info.src_address,
    department_geographical_spatial_info.street_long_deg_x,
    department_geographical_spatial_info.street_lat_deg_y,
      stg_department_all.record_status_active_ind
from
   {{ref('stg_department_all')}} as stg_department_all
left join {{source('clarity_ods', 'clarity_dep')}} as clarity_dep
    on clarity_dep.department_id = stg_department_all.department_id
left join {{source('workday', 'workday_cost_center')}} as cost_center
    on cost_center.cost_cntr_cd = clarity_dep.gl_prefix
left join {{source('clarity_ods', 'clarity_dep_3')}} as clarity_dep_3
    on clarity_dep_3.department_id = stg_department_all.department_id
left join {{source('cdw', 'department_geographical_spatial_info')}}
    as department_geographical_spatial_info
    on department_geographical_spatial_info.dept_key = stg_department_all.dept_key
where stg_department_all.specialty_name = 'BEHAVIORAL HEALTH SERVICES'
