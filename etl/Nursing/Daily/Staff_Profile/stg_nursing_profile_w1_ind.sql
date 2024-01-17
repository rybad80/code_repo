/* stg_nursing_profile_w1_ind
write metric rows for the indicators & counts for
education: bachelors in nursing, advanced degree,
advancing degree
language: any non-English
certification: RN licensed, Magnet certification,
advanced certification,
Later: nursing-specific certification
*/
with
education_rows as (
    select
        'nEducBSNElement' as metric_abbreviation,
        worker_id,
        worker_nursing_bachelors_ind as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        worker_nursing_bachelors_ind = 1

    union all
    select
        'nEducAdvDegreeElement' as metric_abbreviation,
        worker_id,
        worker_nursing_advanced_degree_ind as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        worker_nursing_advanced_degree_ind = 1

    union all
    select
        'nEducAdvancingElement' as metric_abbreviation,
        worker_id,
        attending_advanced_nursing_degree_ind as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        attending_advanced_nursing_degree_ind = 1
),

language_rows as (
    select
        'HasLangSkillElement' as metric_abbreviation,
        worker_id,
        non_english_language_ind as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        non_english_language_ind = 1

    /* add the count when one or more non-English rows */
    union all
    select
        'LangCnt' as metric_abbreviation,
        worker_id,
        non_english_language_cnt as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        non_english_language_ind = 1
),

certification_rows as (
    select
        'HasCertMagnetElement' as metric_abbreviation,
        worker_id,
        magnet_cert_ind as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        magnet_cert_ind = 1

    union all
    select
        'HasCertAdvPracElement' as metric_abbreviation,
        worker_id,
        advanced_practice_cert_ind as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        advanced_practice_cert_ind = 1

	union all
    select
        'RNlicenseCnt' as metric_abbreviation,
        worker_id,
        registered_nurse_cert_ind as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        registered_nurse_cert_ind = 1

    /* get the counts when Magnet or Advandced Practice certs are present */
    union all
    select
        'CertMagnetCnt' as metric_abbreviation,
        worker_id,
        count_magnet_certification as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        magnet_cert_ind = 1

    union all
    select
        'CertAdvPracCnt' as metric_abbreviation,
        worker_id,
        count_ap_certification as numerator
    from
        {{ ref('stg_nursing_education_language_certification') }}
    where
        advanced_practice_cert_ind = 1
),

metric_row_set as (
    select
        metric_abbreviation,
        worker_id,
        numerator
    from
        education_rows

    union all
	select
        metric_abbreviation,
        worker_id,
        numerator
    from
        language_rows

    union all
	select
        metric_abbreviation,
        worker_id,
        numerator
    from
        certification_rows
)

select
    metric_abbreviation,
    nursing_pay_period.pp_end_dt_key as metric_dt_key,
    worker_id,
    null as profile_name,
    null as metric_grouper,
    numerator
from
    metric_row_set
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on nursing_pay_period.latest_pay_period_ind = 1
