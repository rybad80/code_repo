/* stg_nursing_education_language_certification
assemebles data, one row per worker, and
sets INDs for metrics use in the NCCS Platform
Qlik app for nursing to see regular and temporary
RN's education, language, and certification KPIs
*/
with

nursing_certifications as (
    select
    match_certification_name as match_cert_nm,
    case use_for_advanced_practice_ind
        when 1 then 'Yes'
        else 'No' end as use_for_advanced_practice,
    case use_for_magnet_ind
        when 1 then 'Yes'
        else 'No' end as use_for_magnet,
    workday_reference_id,
    case workday_reference_id
        when 'RN' then 1 else 0 end as rn_cert_ind
    from
        {{ ref('lookup_nursing_certification') }}
),

get_certs as (
    select
        worker_id,
        certification_name, --VERIFIED_IND,
        certification_start_date as start_date,
        coalesce(certification_end_date::date, current_date + 1) as expiration_date,
        case when expiration_date > current_date then 1 else 0 end as not_expired_ind
    from
        {{ ref('worker_certification') }}
),

set_worker_certification_magnet as (
    select
        get_certs.worker_id,
        1 as magnet_cert_ind,
        count(get_certs.certification_name) as count_magnet_certification
    from
        get_certs
        inner join nursing_certifications
            on get_certs.certification_name
            = nursing_certifications.match_cert_nm
            and nursing_certifications.use_for_magnet = 'Yes'
    where
        get_certs.not_expired_ind = 1
     group by
        get_certs.worker_id
),

set_worker_certification_advanced_practice as (
    select
        get_certs.worker_id,
        1 as advanced_practice_cert_ind,
        count(get_certs.certification_name) as count_ap_certification
    from
        get_certs
        inner join nursing_certifications
            on get_certs.certification_name
            = nursing_certifications.match_cert_nm
            and nursing_certifications.use_for_advanced_practice = 'Yes'
    where
        get_certs.not_expired_ind = 1
    group by
        get_certs.worker_id
),

set_worker_certification_registered_nurse as (
    select
        get_certs.worker_id,
        1 as registered_nurse_cert_ind
    from
        get_certs
        inner join nursing_certifications
            on get_certs.certification_name
            = nursing_certifications.match_cert_nm
            and nursing_certifications.rn_cert_ind = 1
    where
        get_certs.not_expired_ind = 1
        /* and verified_ind=1 dropped to 131 with verififed restriction, 2,216 otherwise */
    group by
        get_certs.worker_id
),

get_language_data as (  /* one row per worker - language combination  */
    select
        worker_id,
        language_name,
        reading_proficiency,
        speaking_proficiency,
        writing_proficiency,
        describe_proficiency--, order_position, last_modified_date
    from
        {{ ref('worker_language') }}
),

count_non_english as (
    select
        worker_id,
        count(*) as non_english_language_cnt
    from
        get_language_data
    where
        language_name != 'English'
    group by
        worker_id
),

worker_language_counts as (
    select
        worker_id,
        non_english_language_cnt,
        1 as non_english_language_ind
    from
        count_non_english
)

select
    current_date as as_of_dt,
    stg_nursing_educ_p3_degree_profile.worker_id,
    stg_nursing_educ_p3_degree_profile.worker_name,
    stg_nursing_educ_p3_degree_profile.worker_active_ind,
    stg_nursing_educ_p3_degree_profile.worker_nursing_category,
    coalesce(worker_language_counts.non_english_language_cnt, 0) as non_english_language_cnt,
    coalesce(worker_language_counts.non_english_language_ind, 0) as non_english_language_ind,

    coalesce(set_worker_certification_advanced_practice.advanced_practice_cert_ind, 0)
        as advanced_practice_cert_ind,
    set_worker_certification_advanced_practice.count_ap_certification,
    coalesce(set_worker_certification_magnet.magnet_cert_ind, 0)
        as magnet_cert_ind,
    set_worker_certification_magnet.count_magnet_certification,
    coalesce(set_worker_certification_registered_nurse.registered_nurse_cert_ind, 0)
        as registered_nurse_cert_ind,

    stg_nursing_educ_p3_degree_profile.worker_nursing_bachelors_ind,
    stg_nursing_educ_p3_degree_profile.worker_nursing_advanced_degree_ind,
    stg_nursing_educ_p3_degree_profile.entry_nursing_degree_sort,
    stg_nursing_educ_p3_degree_profile.entry_nursing_degree,
    stg_nursing_educ_p3_degree_profile.highest_nursing_degree_sort,
    stg_nursing_educ_p3_degree_profile.highest_nursing_degree,
    stg_nursing_educ_p3_degree_profile.highest_any_degree,
    stg_nursing_educ_p3_degree_profile.attending_advanced_nursing_degree_ind,
    stg_nursing_educ_p3_degree_profile.advancing_degree_sort_num,
    stg_nursing_educ_p3_degree_profile.highest_attending_advanced_nursing_degree

from
    {{ ref('stg_nursing_educ_p3_degree_profile') }}
        as stg_nursing_educ_p3_degree_profile
    left join worker_language_counts
        on stg_nursing_educ_p3_degree_profile.worker_id
        = worker_language_counts.worker_id
    left join set_worker_certification_advanced_practice
        on stg_nursing_educ_p3_degree_profile.worker_id
        = set_worker_certification_advanced_practice.worker_id
    left join set_worker_certification_magnet
        on stg_nursing_educ_p3_degree_profile.worker_id
        = set_worker_certification_magnet.worker_id
    left join set_worker_certification_registered_nurse
        on stg_nursing_educ_p3_degree_profile.worker_id
        = set_worker_certification_registered_nurse.worker_id
