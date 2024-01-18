select
    cardiac_unit_encounter.enc_key,
    cardiac_unit_encounter.hsp_vst_key,
    {{
        dbt_utils.surrogate_key([
            'cardiac_unit_encounter.enc_key',
            'cardiac_unit_encounter.hsp_vst_key'
        ])
    }} as primary_key,
    cardiac_unit_encounter.department_admit_date as ccu_admit_date,
    cardiac_unit_encounter.department_discharge_date as ccu_discharge_date,

    /* we'll have three entries in the _data table -- one for each bounceback metric.
    the 72 hour bounceback will pull bounceback_72_ind for the numerator, and metric_id_72 for the metric_id*/

    cardiac_unit_encounter.ccu_bounceback_72_ind,
    cardiac_unit_encounter.ccu_bounceback_48_ind,
    cardiac_unit_encounter.ccu_bounceback_24_ind,
    'cardiac_bounceback_72' as metric_id_72,
    'cardiac_bounceback_48' as metric_id_48,
    'cardiac_bounceback_24' as metric_id_24

from
    {{ref('cardiac_unit_encounter')}} as cardiac_unit_encounter

where
    cardiac_unit_encounter.ccu_bounceback_72_ind
    + cardiac_unit_encounter.ccu_bounceback_48_ind
    + cardiac_unit_encounter.ccu_bounceback_24_ind >= 1
