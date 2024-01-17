with
sdu_admits as (
    select
        encounter_inpatient.visit_key,
        date(encounter_inpatient.hospital_admit_date) as census_date,
        adt_department_group.department_group_name as next_department_group
    from
        {{ ref('encounter_inpatient') }} as encounter_inpatient
        /* pull in row 2 from adt_department_group to see where patient went after SDU */
        left join {{ ref('adt_department_group') }} as adt_department_group
            on adt_department_group.visit_key = encounter_inpatient.visit_key
            and adt_department_group.inpatient_department_group_order = 2
    where
        encounter_inpatient.admission_source = 'SDU Neonate'
        and encounter_inpatient.hospital_admit_date >= date('2015-01-01')
)
select
    census_date,
    count(*) as sdu_births,
    sum(case when lower(next_department_group) = 'nicu' then 1 end) as sdu_to_nicu,
    sum(case when lower(next_department_group) = 'cicu' then 1 end) as sdu_to_cicu,
    sum(case when next_department_group is null then 1 end) as sdu_only,
    sum(
        case
            when lower(next_department_group) not in ('cicu', 'nicu') and next_department_group is not null then 1
        end
    ) as sdu_to_other
from
    sdu_admits
group by
    census_date
