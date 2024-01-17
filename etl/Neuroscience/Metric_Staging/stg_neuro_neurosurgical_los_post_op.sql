select
    *
from
    {{ ref('stg_neuro_neurosurgical_los') }}
where
    post_op_los_days is not null
