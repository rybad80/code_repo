-- find the date of initial ENGIN visit for each patient
select
    a.pat_key,
    min(a.encounter_date) as engin_start_date
from (
        select
            pat_key,
            encounter_date
        from {{ ref('stg_frontier_engin_op_enc_engin') }}
        where appointment_status_id != 4 -- exclude 'no show'
        union
        select
            pat_key,
            encounter_date
        from {{ ref('stg_frontier_engin_op_enc_generic') }}
        where appointment_status_id != 4 -- exclude 'no show'
        union
        select
            pat_key,
            encounter_date
        from {{ ref('stg_frontier_engin_inpat_encounter') }}
    ) as a
group by a.pat_key
