/* rn_profile_reference
for nursing reference usage, summarizes the education degrees
and indicators, and language, and certifcation INDs and counts
but only for active CHOP people in RN jobs
*/
with
worker_cert_count as (
    select
        worker_id
    from
        {{ ref('worker_certification') }}
	where
        certification_end_date is null
        or certification_end_date >= current_date
	group by
        worker_id
)
select
    worker.worker_id,
    worker.legal_reporting_name as rn_name,
    profile_data.non_english_language_cnt,
    profile_data.non_english_language_ind,
	profile_data.worker_nursing_bachelors_ind as has_nursing_bachelors_ind,
    profile_data.worker_nursing_advanced_degree_ind as has_nursing_advanced_degree_ind,
    profile_data.attending_advanced_nursing_degree_ind,
    coalesce(second_degree.numerator, 0) as nursing_second_degree_ind,
    profile_data.entry_nursing_degree,
    profile_data.highest_nursing_degree,
    profile_data.highest_attending_advanced_nursing_degree,
    second_degree.profile_name as nursing_second_degree_after,
    profile_data.highest_any_degree,
    profile_data.registered_nurse_cert_ind,
    profile_data.advanced_practice_cert_ind,
    count_ap_certification as ap_certification_count,
    profile_data.magnet_cert_ind,
    count_magnet_certification as magnet_certification_count,
    case
        when worker_cert_count.worker_id is null
        then worker.worker_id
        end as worker_id_no_certs,
    case
        when worker_id_no_certs > ''
        then 1 else 0
        end as rn_has_no_certs_ind,
    worker.hire_date,
	job_ref.job_group_id,
    case
        when rn_has_no_certs_ind = 1
        then case lower(worker.worker_role)
            when 'regular' then 1
            when 'temporary' then 1
            else 0 end
        end as  show_person_zero_certs_ind,
    worker.active_ind,
    worker.job_title_display,
    worker.reporting_chain,
    job_ref.nursing_category,
    case
		when profile_data.highest_any_degree = 'no Workday degree'
        then 1 else 0
        end as worker_no_degree_ind,
    case
		when profile_data.entry_nursing_degree  = 'Missing!'
        then 1 else 0
        end as rn_no_nursing_degree_ind,
    nursing_cost_center_attributes.cost_center_display,
    nursing_cost_center_attributes.cost_center_parent,
    nursing_cost_center_attributes.cost_center_group,
    nursing_cost_center_attributes.cost_center_type
from
    {{ ref('worker') }} as worker
    left join {{ ref('worker_job_reference') }} as job_ref
        on  worker.worker_id = job_ref.worker_id
    left join worker_cert_count
        on  worker.worker_id = worker_cert_count.worker_id
    left join {{ ref('stg_nursing_education_language_certification') }} as profile_data
        on worker.worker_id = profile_data.worker_id
    left join {{ ref('stg_nursing_educ_p4_time') }} as second_degree
        on worker.worker_id = second_degree.worker_id
	and second_degree.metric_abbreviation = 'Nursing2ndDeg'
    left join {{ ref('nursing_cost_center_attributes') }} as  nursing_cost_center_attributes
        on worker.cost_center_id = nursing_cost_center_attributes.cost_center_id
where
    worker.active_ind = 1
    and worker.rn_job_ind = 1
