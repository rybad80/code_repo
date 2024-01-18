/* stg_nursing_profile_w2_name
 for workers associated to an RN job (per stg_nursing_educ_p3_degree_profile)
write metric rows for the degree levels
for entry, highest, and advancing degree for nursing
and name the languages: any non-English
and name the certifications: RN licensed, Magnet certification,
advanced certification,
Later: nursing-specific certification
*/
with
education_rows as (
    select
        'nEducDegreeEntry' as metric_abbreviation,
        worker_id,
        entry_nursing_degree as profile_name,
        1 as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}

    union all
    select
        'nEducDegreeHighest' as metric_abbreviation,
        worker_id,
        highest_nursing_degree as profile_name,
        1 as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}

    union all
    select
       'nEducDegreeAdvancing' as metric_abbreviation,
        worker_id,
        highest_attending_advanced_nursing_degree as profile_name,
        attending_advanced_nursing_degree_ind as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        attending_advanced_nursing_degree_ind = 1

    union all
    select
        'EducDegreeHighestAny' as metric_abbreviation,
        worker_id,
        highest_any_degree as profile_name,
        1 as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        highest_any_degree is not null
),

list_degree_and_major as (
    select
        'rnEducDegree'
        || case stg_nursing_educ_p1_worker_degree.include_major_for_nursing_degree_level
            when 0 then 'Other' else ''
            end	as metric_abbreviation,
        master_date.dt_key as metric_dt_key,
        stg_nursing_educ_p1_worker_degree.worker_id,
        stg_nursing_educ_p1_worker_degree.degree as profile_name,
        stg_nursing_educ_p1_worker_degree.major as metric_grouper,
        1 as numerator -- select *
    from
        {{ ref('stg_nursing_educ_p1_worker_degree') }} as stg_nursing_educ_p1_worker_degree
        inner join {{ source('cdw', 'master_date') }} as master_date
        on stg_nursing_educ_p1_worker_degree.year_degree_received = master_date.c_yyyy
        and master_date.c_mm = 12
        and master_date.last_day_month_ind = 1
    where
        stg_nursing_educ_p1_worker_degree.graduated  = 'Yes'
        and stg_nursing_educ_p1_worker_degree.include_for_degree_achieved_ind = 1
),
language_rows as (
    select
        'rnLangSkill' as metric_abbreviation,
        rn_data.worker_id,
        worker_language.language_name as profile_name,
        worker_language.describe_proficiency as metric_grouper,
        1 as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }} as rn_data
        inner join {{ ref('worker_language') }} as worker_language
        on rn_data.worker_id = worker_language.worker_id
    where
        rn_data.non_english_language_ind = 1
),

nursing_cert as (
    select
        match_certification_name as match_cert_nm,
        use_for_advanced_practice_ind,
        use_for_magnet_ind,
        workday_reference_id,
        case workday_reference_id
        when 'RN' then 1 else 0 end as rn_cert_ind
    from
        {{ ref('lookup_nursing_certification') }}
),

certification_rows as (
    select
        'rnCertName' as metric_abbreviation,
        rn_data.worker_id,
        worker_certification.certification_name as profile_name,
        case
            when nursing_cert.use_for_magnet_ind = 1
            and nursing_cert.use_for_advanced_practice_ind = 1 then 'Magnet & advanced practice'
            when nursing_cert.use_for_magnet_ind = 1 then 'Magnet-recognized'
            when nursing_cert.use_for_advanced_practice_ind = 1 then 'advanced practice'
            when rn_cert_ind = 1 then 'RN license'
        end as metric_grouper,
	1 as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }} as rn_data
        inner join {{ ref('worker_certification') }} as worker_certification
        on rn_data.worker_id = worker_certification.worker_id
            and case
                when coalesce(certification_end_date::date, current_date + 1)  > current_date
                then 1 else 0
                end = 1 /* certification record is not expired */
        left join nursing_cert
        on worker_certification.certification_name = nursing_cert.match_cert_nm
    where
        rn_data.worker_active_ind = 1
--        rn_data.magnet_cert_ind = 1 -- Awaiting client confirmation
--        or rn_data.advanced_practice_cert_ind = 1 -- Awaiting client confirmation
        and worker_certification.certification_name not in (
            /* HR does not want these in NCCS apps */
            'Act 31 Recognizing Child Abuse and Mandated Reporting - Pennsylvania Department of State',
            'FBI Fingerprint Status - IdentoGo by IDEMIA',
            'FBI Report Received - Childrens Hospital of Philadelphia',
            'PA Child Abuse - Pennsylvania Department of Human Services',
            'PA State Police - Pennslyvania State Police',
            'ACT 153 Renewal - Childrens Hospital of Philadelphia'
            )
),

metric_row_set as (
    select
        metric_abbreviation,
        0 as metric_dt_key,
        worker_id,
        profile_name,
		null as metric_grouper,
		numerator
    from
        education_rows

    union all
	select
        metric_abbreviation,
        metric_dt_key,
        worker_id,
        profile_name,
		metric_grouper,
        numerator
    from
       list_degree_and_major

	union all
	select
        metric_abbreviation,
        0 as metric_dt_key,
        worker_id,
        profile_name,
		metric_grouper,
        numerator
    from
        language_rows

    union all
	select
        metric_abbreviation,
        0 as metric_dt_key,
        worker_id,
        profile_name,
		metric_grouper,
        numerator
    from
        certification_rows
)

select
    metric_row_set.metric_abbreviation,
    case metric_row_set.metric_dt_key
        when 0 then nursing_pay_period.pp_end_dt_key
        else metric_row_set.metric_dt_key
    end as metric_dt_key,
    metric_row_set.worker_id,
    metric_row_set.profile_name,
    metric_row_set.metric_grouper,
    metric_row_set.numerator
from
    metric_row_set
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on metric_row_set.metric_dt_key < nursing_pay_period.pp_end_dt_key
        and nursing_pay_period.latest_pay_period_ind = 1
