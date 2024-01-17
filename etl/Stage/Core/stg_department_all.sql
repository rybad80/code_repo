{{ config(meta = {
    'critical': true
}) }}

with all_departments as (
    select
        dept_id as department_id,
        dept_nm as department_name,
        dept_abbr as department_abbr,
        null as specialty,
        'idx_department' as source
    from {{ source( 'manual_ods', 'idx_department') }}

    union all

    select
        dept_id as department_id,
        dept_nm as department_name,
        dept_abbr as department_abbr,
        null as specialty,
        'scm_department' as source
    from {{ source( 'manual_ods', 'scm_department') }}

    union all

    select
        dept_id as department_id,
        dept_nm as department_name,
        dept_abbr as department_abbr,
        null as specialty,
        'admin_department' as source
    from {{ source( 'manual_ods', 'admin_department') }}

    union all

    select
        department_id,
        department_name,
        dept_abbreviation as department_abbr,
        specialty,
        'clarity_dep' as source
    from
        {{ source('clarity_ods','clarity_dep') }}
),

specialty as (
    select
        department_id,
        case
            when lower(department_name) = 'wood anes pain mgmt' then 'WOOD ANES PAIN MGMT'
            when lower(department_name) = 'mkt 3550 spc imm fam care' then 'GENERAL PEDIATRICS'
            when lower(specialty) = 'hematology oncology' then 'ONCOLOGY'
            when lower(specialty) = 'radiation oncology' then 'ONCOLOGY'
            when lower(specialty) = 'neonatal followup' then 'NEONATOLOGY'
            when lower(specialty) = 'leukodystropy' then 'NEUROLOGY'
            when lower(specialty) = 'dentistry' then 'PLASTIC SURGERY'
            when lower(specialty) = 'pediatric general thoracic surgery' then 'GENERAL SURGERY'
            when lower(department_name) like '%feeding%'
                and lower(department_name) not like '%bh%' then 'FEEDING'
            else upper(specialty)
        end as specialty_name
    from
        all_departments
    where
        lower(specialty) in (
            'adolescent', 'allergy', 'behavioral health services', 'cardiology', 'cardiovascular surgery',
            'critical care', 'dermatology', 'dentistry',
            'developmental pediatric rehab', 'developmental pediatrics',
            'endocrinology', 'gastroenterology', 'general pediatrics', 'general surgery',
            'genetics', 'gi/nutrition', 'hematology', 'hematology oncology', 'immunology',
            'infectious disease', 'leukodystropy', 'metabolism', 'neonatal followup', 'nephrology',
            'neurology', 'neurosurgery', 'ophthalmology', 'orthopedics', 'otorhinolaryngology',
            'plastic surgery', 'pulmonary', 'rheumatology', 'urology',
            'pediatric general thoracic surgery', 'rehab medicine', 'oncology', 'neonatology'
        )
        or lower(department_name) = 'wood anes pain mgmt'
        or lower(department_name) like '%feeding%'
)

select
    {{
        dbt_utils.surrogate_key([
            'all_departments.department_id',
            'all_departments.source'
        ])
    }} as department_key,
    department.dept_key,
    all_departments.department_name,
    department_cost_center_sites.department_display_name,
    all_departments.department_abbr,
    all_departments.department_id,
    upper(coalesce(specialty.specialty_name, all_departments.specialty, 'UNKNOWN')) as specialty_name,
    zc_loc_rpt_grp_6.name as revenue_location_group,
    case
        when clarity_dep.department_id is not null
        then coalesce(clarity_loc.loc_name, 'INVALID')
        when all_departments.department_id in (-1, 73)
        then 'INVALID'
        when all_departments.department_id in (0, 1.003)
        then 'DEFAULT'
    end as location_name,
    case
        when clarity_dep.department_id is not null
        then coalesce(clarity_loc.loc_id, -1)
        when all_departments.department_id in (-1, 73)
        then -1
        when all_departments.department_id in (0, 1.003)
        then 0
    end as location_id,
    -- to include these specialty encounters in `encounter_specialty_care` until their `intended_use_id` is fixed
    coalesce(zc_dep_rpt_grp_31.internal_id, department_intended_use.intended_use_id::varchar(254))
        as intended_use_id,
    zc_dep_rpt_grp_31.abbr as intended_use_abbr,
    -- to include these specialty encounters in `encounter_specialty_care` until their `intended_use_name` is fixed
    coalesce(zc_dep_rpt_grp_31.name, department_intended_use.intended_use_name) as intended_use_name,
    zc_dep_rpt_grp_33.internal_id as care_area_id,
    zc_dep_rpt_grp_33.abbr as care_area_abbr,
    zc_dep_rpt_grp_33.name as care_area_name,
    zc_dep_rpt_grp_34.internal_id as addl_care_area_1_id,
    zc_dep_rpt_grp_34.abbr as addl_care_area_1_abbr,
    zc_dep_rpt_grp_34.name as addl_care_area_1_name,
    zc_dep_rpt_grp_35.internal_id as addl_care_area_2_id,
    zc_dep_rpt_grp_35.abbr as addl_care_area_2_abbr,
    zc_dep_rpt_grp_35.name as addl_care_area_2_name,
    zc_dep_rpt_grp_36.internal_id as addl_care_area_3_id,
    zc_dep_rpt_grp_36.abbr as addl_care_area_3_abbr,
    zc_dep_rpt_grp_36.name as addl_care_area_3_name,
    upper(zc_center.name) as department_center,
    zc_center.center_c as department_center_id,
    zc_center.abbr as department_center_abbr,
    zc_center.name as department_center_name,
    upper(clarity_dep_2.address_city) as mailing_city,
    upper(zc_state.name) as mailing_state,
    clarity_dep_2.address_zip_code as mailing_zip,
    -- to include these specialty care departments in `department_care_network`
    -- until department fixed is implemented
    case department_intended_use.department_id
        when '101012189' then 'BGR'
        else coalesce(specialty_care_center.scc_abbr, specialty_care_location.scc_abbr)
    end as scc_abbreviation,
    case when scc_abbreviation is not null then 1 else 0 end as scc_ind,
    case
        when scc_ind = 1
        or lower(intended_use_name) in ('primary care', 'urgent care')
        or lower(all_departments.department_name) like '%triage%'
        then 1 else 0 end as care_network_ind,
    case
        when upper(strleft(all_departments.department_name, 3)) = 'PB ' then 1 else 0
    end as professional_billing_ind,
    case when rec_stat is null then 1 else 0 end as record_status_active_ind
from
    all_departments
    left join {{ source('clarity_ods','clarity_dep') }} as clarity_dep
        on clarity_dep.department_id = all_departments.department_id
    left join {{ source('clarity_ods','clarity_dep_2') }} as clarity_dep_2
        on clarity_dep_2.department_id = clarity_dep.department_id
    left join {{ source('clarity_ods', 'clarity_loc') }} as clarity_loc
        on clarity_dep.rev_loc_id = clarity_loc.loc_id
    left join specialty
        on specialty.department_id = clarity_dep.department_id
    left join {{ source('clarity_ods', 'zc_center') }} as zc_center
        on zc_center.center_c = clarity_dep.center_c
    -- join to be removed once `intended_use` department fix is implemented
    left join {{ ref('lookup_department_intended_use_temp_override') }} as department_intended_use
        on department_intended_use.department_id = clarity_dep.department_id
    left join {{ ref('lookup_specialty_care_center') }} as specialty_care_center
        on lower(specialty_care_center.specialty_care_center_name) = lower(zc_center.name)
    left join {{ ref('lookup_specialty_care_center') }} as specialty_care_location
        on specialty_care_location.specialty_care_center_name = clarity_loc.loc_name
    left join {{ ref('lookup_care_network_department_cost_center_sites') }} as department_cost_center_sites
        on department_cost_center_sites.department_id = clarity_dep.department_id
    left join {{ source('clarity_ods','zc_dep_rpt_grp_31') }} as zc_dep_rpt_grp_31
        on zc_dep_rpt_grp_31.internal_id = clarity_dep.rpt_grp_thirtyone_c
    left join {{ source('clarity_ods','zc_dep_rpt_grp_33') }} as zc_dep_rpt_grp_33
        on zc_dep_rpt_grp_33.internal_id = clarity_dep.rpt_grp_trtythree_c
    left join {{ source('clarity_ods','zc_dep_rpt_grp_33') }} as zc_dep_rpt_grp_34
        on zc_dep_rpt_grp_34.internal_id = clarity_dep.rpt_grp_trtyfour_c
    left join {{ source('clarity_ods','zc_dep_rpt_grp_33') }} as zc_dep_rpt_grp_35
        on zc_dep_rpt_grp_35.internal_id = clarity_dep.rpt_grp_trtyfive_c
    left join {{ source('clarity_ods','zc_dep_rpt_grp_33') }} as zc_dep_rpt_grp_36
        on zc_dep_rpt_grp_36.internal_id = clarity_dep.rpt_grp_thirtysix_c
    left join {{ source('clarity_ods','zc_state') }} as zc_state
        on zc_state.state_c = clarity_dep_2.address_state_c
    left join {{ source('clarity_ods','zc_loc_rpt_grp_6') }} as zc_loc_rpt_grp_6
        on zc_loc_rpt_grp_6.rpt_grp_six = clarity_loc.rpt_grp_six
    -- only use for dept_key
    left join {{ source('cdw','department') }} as department
        on department.dept_id = all_departments.department_id
