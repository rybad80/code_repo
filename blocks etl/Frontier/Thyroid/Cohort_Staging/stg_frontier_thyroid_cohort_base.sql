select a.pat_key,
    min(a.encounter_date) as initial_date
from (
        select pat_key,
            encounter_date
        from {{ ref('stg_frontier_thyroid_enc_ov_endo') }}
        union
        select pat_key,
            encounter_date
        from {{ ref('stg_frontier_thyroid_enc_dx') }}
        union
        select pat_key,
            encounter_date
        from {{ ref('stg_frontier_thyroid_enc_cpt') }}
        union
        select pat_key,
            encounter_date
        from {{ ref('stg_frontier_thyroid_enc_other_prov_e') }}
        union
        select pat_key,
            encounter_date
        from {{ ref('stg_frontier_thyroid_enc_other_prov_d') }}
) as a
group by a.pat_key
