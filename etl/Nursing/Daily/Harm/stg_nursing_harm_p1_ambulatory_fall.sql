/* stg_nursing_harm_p1_ambulatory_fall
capture the "ambulatory fall data" field values pivoted into one row per fall
only for records marked "Complete" in REDCap
*/
with
falls_rec_data as (
    select
        record_id,
        field_name,
        field_value
    from
         {{ ref('nursing_redcap_detail') }}
    where
        redcap_project_id = 174 --'Ambulatory Fall Data'
),

match_rec_data as (
    select
        falls_rec.record_id,
        fall_date.field_value as amb_fall_date_text,
        loc.field_value as location_name,
        kaps.field_value as kaps_file_id,
        fall_severity.field_value as fall_severity,
        fall_type.field_value as fall_type,
        fall_category.field_value as fall_category,
        included.field_value as included,
        prevented.field_value as prevented_type,
        age_group.field_value as age_group,
        case
            when prevented.field_value is null
            then 0 else 1
            end as prevented_fall_ind,
        case
            when included.field_value = 'Yes include on dashboard'
            then 1 else 0
            end as ambulatory_patient_fall_ind,
        case
            when fall_category.field_value = 'Syncope Post Immunizations'
            and ambulatory_patient_fall_ind = 1
            then 1 else 0
            end as post_immunization_fall_ind,
        case ambulatory_patient_fall_ind
            when 1
            then case fall_severity.field_value
                when '1- no reported injury'
                then 0 else 1
                end
            else 0
            end as with_injury_ind
    from
        falls_rec_data as falls_rec
        inner join falls_rec_data as completed
            on falls_rec.record_id = completed.record_id
            and completed.field_name = 'ambulatory_fall_data_complete'
            and completed.field_value = 'Complete'
        inner join falls_rec_data as included
            on falls_rec.record_id = included.record_id
            and included.field_name = 'included'
        inner join falls_rec_data as kaps
            on falls_rec.record_id = kaps.record_id
            and kaps.field_name = 'file_id'
        inner join falls_rec_data as fall_date
            on falls_rec.record_id = fall_date.record_id
            and fall_date.field_name = 'fall_date'
        inner join falls_rec_data as loc
            on falls_rec.record_id = loc.record_id
            and loc.field_name = 'location'
        inner join falls_rec_data as fall_severity
            on falls_rec.record_id = fall_severity.record_id
            and fall_severity.field_name = 'fall_severity'
        left join falls_rec_data as fall_type
            on falls_rec.record_id = fall_type.record_id
            and fall_type.field_name = 'fall_type'
        left join falls_rec_data as fall_category
            on falls_rec.record_id = fall_category.record_id
            and fall_category.field_name = 'fall_category'
        left join falls_rec_data as prevented
            on falls_rec.record_id = prevented.record_id
            and prevented.field_name = 'prevented_detail'
        left join falls_rec_data as age_group
            on falls_rec.record_id = age_group.record_id
            and age_group.field_name = 'age_group'
    where
        falls_rec.field_name = 'record_id'
)

select
    cast(amb_fall_date_text as date) as amb_fall_date,
    location_name,
    case ambulatory_patient_fall_ind
        when 1
        then count(distinct record_id)
        end as ambulatory_patient_fall_count,
    case prevented_fall_ind
        when 1
        then count(distinct record_id)
        end as prevented_fall_count,
    fall_severity,
    fall_category,
    fall_type,
    age_group,
    prevented_type,
    prevented_fall_ind,
    ambulatory_patient_fall_ind,
    post_immunization_fall_ind,
    with_injury_ind
from
    match_rec_data
where
    prevented_fall_ind = 1
    or ambulatory_patient_fall_ind = 1
    /* Only keeping records indicated as prevented
        or confirmed patient actual falls */
group by
    cast(amb_fall_date_text as date),
    location_name,
    fall_severity,
    fall_category,
    fall_type,
    age_group,
    prevented_type,
    prevented_fall_ind,
    ambulatory_patient_fall_ind,
    post_immunization_fall_ind,
    with_injury_ind
