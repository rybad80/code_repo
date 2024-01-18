select
  upper(lookup_ed_cohort_metadata.cohort) as cohort,
  lookup_ed_cohort_metadata.ed_qi_ind,
  lookup_ed_cohort_metadata.ed_bh_qi_ind,
  lookup_ed_cohort_metadata.gene_article_id,
  stg_ed_cohort_metadata_acuity_dist.acuity_one_dist,
  stg_ed_cohort_metadata_acuity_dist.acuity_two_dist,
  stg_ed_cohort_metadata_acuity_dist.acuity_three_dist,
  stg_ed_cohort_metadata_acuity_dist.acuity_four_dist,
  stg_ed_cohort_metadata_acuity_dist.acuity_five_dist
from
  {{ref('lookup_ed_cohort_metadata')}} as lookup_ed_cohort_metadata
  left join {{ref('stg_ed_cohort_metadata_acuity_dist')}} as stg_ed_cohort_metadata_acuity_dist
    on lower(lookup_ed_cohort_metadata.cohort) = lower(stg_ed_cohort_metadata_acuity_dist.cohort)
