with usnews_max_row as (
    select
        usnews_detail_review.primary_key,
        usnews_detail_review.metric_id,
        usnews_detail_review.submission_year,
        max(usnews_detail_review.division) as division,
        max(usnews_detail_review.question_number) as question_number,
        max(usnews_detail_review.metric_name) as metric_name,
        max(usnews_detail_review.mrn) as mrn,
        max(usnews_detail_review.patient_name) as patient_name,
        max(usnews_detail_review.dob) as dob,
        max(usnews_detail_review.index_date) as index_date,
        max(usnews_detail_review.num) as num,
        max(usnews_detail_review.cpt_code) as cpt_code,
        max(usnews_detail_review.icd10_code) as icd10_code,
        max(usnews_detail_review.domain) as domain, --noqa: L029
        max(usnews_detail_review.subdomain) as subdomain,
        max(usnews_detail_review.metric_date) as metric_date,
        max(usnews_detail_review.included_ind) as included_ind,
        max(usnews_detail_review.excluded_ind) as excluded_ind,
        max(usnews_detail_review.last_updated_by_clinician) as last_updated_by_clinician,
        max(usnews_detail_review.usnews_last_updated_date) as usnews_last_updated_date
    from
        {{ref('usnews_detail_review')}} as usnews_detail_review
    group by
        usnews_detail_review.primary_key,
        usnews_detail_review.metric_id,
        usnews_detail_review.submission_year
)

select distinct
    usnews_detail_review.primary_key,
    usnews_max_row.submission_year,
    usnews_max_row.division,
    usnews_max_row.question_number,
    usnews_detail_review.metric_id,
    usnews_max_row.metric_name,
    usnews_max_row.mrn,
    usnews_max_row.patient_name,
    usnews_max_row.dob,
    usnews_max_row.index_date,
    usnews_max_row.num,
    usnews_detail_review.denom,
    usnews_max_row.cpt_code,
    usnews_max_row.icd10_code,
    usnews_max_row.domain, --noqa: L029
    usnews_max_row.subdomain,
    usnews_max_row.metric_date,
    usnews_detail_review.num_calculation,
    usnews_detail_review.denom_calculation,
    usnews_detail_review.desired_direction,
    usnews_detail_review.metric_type,
    usnews_detail_review.review_ind,
    usnews_detail_review.submitted_ind,
    usnews_detail_review.scored_ind,
    usnews_max_row.included_ind,
    usnews_max_row.excluded_ind,
    usnews_max_row.last_updated_by_clinician,
    usnews_max_row.usnews_last_updated_date
from
    {{ref('usnews_detail_review')}} as usnews_detail_review
    left join usnews_max_row
        on usnews_max_row.primary_key = usnews_detail_review.primary_key
        and usnews_max_row.metric_id = usnews_detail_review.metric_id
where
    (usnews_max_row.included_ind = 1 or usnews_max_row.included_ind is null)
    and (usnews_max_row.excluded_ind != 1 or usnews_max_row.excluded_ind is null)
