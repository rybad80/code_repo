with tna_prov_lvl as (
    select
        next_available_slots.specialty_name,
        next_available_slots.department_name,
        next_available_slots.department_id,
        as_of_date as index_date,
        next_available_slots.prov_id,
        case
            when department_care_network.primary_care_ind = '1'
            then 'Primary Care'
            when department_care_network.specialty_care_ind = '1'
                    and lower(department_care_network.specialty_name) in (
                    'physical therapy', 'speech', 'audiology', 'occupational therapy',
                    'clinical nutrition')
            then 'Ancillary Services'
            when department_care_network.specialty_care_ind = '1'
                and next_available_slots.specialty_name not in ('cardiovascular surgery', 'obstetrics',
                'multidisciplinary', 'gi/nutrition', 'family planning')
            then 'Specialty Care'
        end   as care_setting,
        group_by,
        min(next_available_slots.days_to_slot) as days_to_slot
    from
        {{ref('tna_phys_app_psych_available_slot_history')}} as next_available_slots
    left join
        {{ref('department_care_network')}} as department_care_network
    on
        department_care_network.department_id = next_available_slots.department_id
    where
        next_available_slots.next_available = '3rd Next Available'
    and group_by in ('SPECIALTY', 'DEPARTMENT')
    group by
        next_available_slots.specialty_name,
        next_available_slots.department_name,
        next_available_slots.department_id,
        as_of_date,
        group_by,
        next_available_slots.prov_id,
        care_setting
),
tna_specialty_lvl as (
       select
        care_setting,
        specialty_name,
        department_name,
        department_id,
        min(days_to_slot) over (partition by care_setting, specialty_name,
                                                index_date, group_by) as days_to_slot,
        index_date,
        group_by
    from
    tna_prov_lvl
    where
        group_by = 'SPECIALTY'
),
tna_department_lvl as (
    select
        care_setting,
        specialty_name,
        department_name,
        department_id,
        min(days_to_slot) as days_to_slot,
        index_date,
        group_by
    from
        tna_prov_lvl
    where
        group_by = 'DEPARTMENT'
    group by
        care_setting,
        specialty_name,
        department_name,
        department_id,
        index_date,
        group_by
)
select
    care_setting,
    specialty_name,
    department_name,
    department_id,
    min(days_to_slot) over (partition by care_setting, specialty_name,
                                        index_date, group_by) as days_to_slot,
    index_date,
    group_by
from
tna_specialty_lvl
union all
select
    care_setting,
    specialty_name,
    department_name,
    department_id,
    min(days_to_slot) as days_to_slot,
    index_date,
    group_by
from
tna_department_lvl
group by
    care_setting,
    department_name,
    department_id,
    specialty_name,
    index_date,
    group_by
