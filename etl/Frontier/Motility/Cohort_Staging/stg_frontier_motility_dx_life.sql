select
    pat_key,
    mrn,
    max(case when general_motility_blu_ind = 1
        then 1 else 0 end) as general_motility_blu_ind,
    max(case when general_motility_yel_ind + general_motility_blk_ind + general_motility_grn_ind > 0
        then 1 else 0 end) as general_motility_non_blu_ind,
    max(case when general_motility_yel_ind = 1
        then 1 else 0 end) as general_motility_yel_ind,
    max(case when general_motility_blk_ind = 1
        then 1 else 0 end) as general_motility_blk_ind,
    max(case when general_motility_grn_ind = 1
        then 1 else 0 end) as general_motility_grn_ind,
    max(case when defecation_disorder_blu_ind = 1
        then 1 else 0 end) as defecation_disorder_blu_ind,
    max(case when defecation_disorder_yel_ind + defecation_disorder_blk_ind + defecation_disorder_grn_ind > 0
        then 1 else 0 end) as defecation_disorder_non_blu_ind,
    max(case when defecation_disorder_yel_ind = 1
        then 1 else 0 end) as defecation_disorder_yel_ind,
    max(case when defecation_disorder_blk_ind = 1
        then 1 else 0 end) as defecation_disorder_blk_ind,
    max(case when defecation_disorder_grn_ind = 1
        then 1 else 0 end) as defecation_disorder_grn_ind,
    max(case when multi_disciplinary_blu_ind = 1
        then 1 else 0 end) as multi_disciplinary_blu_ind,
    max(case when multi_disciplinary_grn_ind = 1
        then 1 else 0 end) as multi_disciplinary_non_blu_ind,
    max(case when multi_disciplinary_grn_ind = 1
        then 1 else 0 end) as multi_disciplinary_grn_ind,
    max(case when life_bowel_dysmotility_blu_ind = 1
        then 1 else 0 end) as life_bowel_dysmotility_blu_ind,
    max(case when life_bowel_dysmotility_yel_ind = 1
        then 1 else 0 end) as life_bowel_dysmotility_yel_ind,
    max(case when life_bowel_dysmotility_yel_ind = 1
        then 1 else 0 end) as life_bowel_dysmotility_non_blu_ind,
    max(case when neuromodulation_blu_ind = 1
        then 1 else 0 end) as neuromodulation_blu_ind
from
    {{ ref('stg_frontier_motility_dx_hx') }}
group by
    pat_key,
    mrn
