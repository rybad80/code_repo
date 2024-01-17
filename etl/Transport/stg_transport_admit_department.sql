with
    department_info as (--department and service info
        select
            stg_transport_all_encounters.admit_visit_key,
            adt_department_group.enter_date,
            adt_department_group.department_group_name as department_name,
            adt_department_group.initial_service,
            extract(epoch from adt_department_group.enter_date
                - stg_transport_all_encounters.hospital_admit_date) / 3600.0 as admit_to_ent_hrs,
            adt_department_group.all_department_group_order,
            adt_department_group.last_department_group_ind,
            case
                when adt_department_group.all_department_group_order = 1
                    and adt_department_group.department_group_name = 'ED'
                    then 'Emergency'
                when adt_department_group.all_department_group_order = 1
                    then adt_department_group.initial_service
                else null
            end as first_service,

            case
                when adt_department_group.last_department_group_ind = 1
                    and adt_department_group.department_group_name = 'ED'
                    then 'Emergency'
                when adt_department_group.last_department_group_ind = 1
                    then adt_department_group.initial_service
                else null
            end as last_service,

            case
                when adt_department_group.inpatient_department_group_order = 1
                    and (adt_department_group.bed_care_group = 'ICU'
                        or adt_department_group.department_group_name = 'CCU')
                then 1
                else 0
            end as icu_first_ip,

            case
                when adt_department_group.all_department_group_order = 1
                    and (adt_department_group.bed_care_group = 'ICU'
                        or adt_department_group.department_group_name = 'CCU')
                then 1
                else 0
            end as icu_first_all,

            case
                when admit_to_ent_hrs <= 6
                    and (adt_department_group.bed_care_group = 'ICU'
                        or adt_department_group.department_group_name = 'CCU')
                then 1
                else 0
            end as icu_6_hrs
        from
            {{ ref('stg_transport_all_encounters') }} as stg_transport_all_encounters
            inner join {{ ref('adt_department_group') }} as  adt_department_group on
                    adt_department_group.visit_key = stg_transport_all_encounters.admit_visit_key
        where
            adt_department_group.department_group_name != 'MAIN TRANSPORT' -- do not want this as a dischrg dept
)

select
    department_info.admit_visit_key,
    max(department_info.first_service) as first_service,
	max(department_info.last_service) as last_service,
    max(
        case
            when department_info.all_department_group_order = 1
            then department_info.department_name
            else null
        end
    ) as first_department,
    max(
        case
            when department_info.last_department_group_ind = 1
            then department_info.department_name
            else null
        end
    ) as last_department,
    max(department_info.icu_first_ip) as icu_first_ip_ind,
    max(department_info.icu_first_all) as icu_first_all_ind,
    max(department_info.icu_6_hrs) as icu_6_hrs_ind
from
    department_info
group by
    department_info.admit_visit_key
