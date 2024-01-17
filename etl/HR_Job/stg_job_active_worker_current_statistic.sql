{{ config(meta = {
    'critical': true
}) }}

/* stg_job_active_worker_current_statistic
how many people active at CHOP are aligned to the job currently
and of them which are job requiring Registered Nurse cert
*/
select
        job_code,
        sum(active_ind) as job_active_worker_count,
        rn_job_ind,
        sum(active_ind * rn_job_ind) as job_active_rn_count
    from
        {{ ref('worker') }}
    where
        active_ind = 1
	group by
        job_code,
        rn_job_ind
