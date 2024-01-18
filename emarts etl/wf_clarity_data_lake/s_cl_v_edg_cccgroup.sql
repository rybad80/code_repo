/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_v_edg_cccgroup",
  "MAPPING_NAME": "m_stg_load_cl_v_edg_cccgroup",
  "MAPPING_ID": 7896,
  "TARGET_ID": 7633,
  "TARGET_NAME": "s_cl_v_edg_cccgroup"
}
*/

with
sq_v_edg_cccgroup as (
    select
        v_edg_cccgroup.dx_id,
        v_edg_cccgroup.dx_name,
        v_edg_cccgroup.icd9_code,
        v_edg_cccgroup.dx,
        v_edg_cccgroup.malignancy_ccc,
        v_edg_cccgroup.hemaimmuno_ccc,
        v_edg_cccgroup.respiratory_ccc,
        v_edg_cccgroup.gastro_ccc,
        v_edg_cccgroup.metabolic_ccc,
        v_edg_cccgroup.neuromusc_ccc,
        v_edg_cccgroup.cardiovasc_ccc,
        v_edg_cccgroup.renal_ccc,
        v_edg_cccgroup.othercongen_ccc,
        v_edg_cccgroup.neonatal_ccc,
        v_edg_cccgroup.tech_dep_ccc,
        v_edg_cccgroup.transplant_ccc
    from
        {{ source('clarity_ods', 'v_edg_cccgroup') }} as v_edg_cccgroup
)
select
    cast(sq_v_edg_cccgroup.dx_id as bigint) as dx_id,
    cast(sq_v_edg_cccgroup.dx_name as varchar(200)) as dx_name,
    cast(sq_v_edg_cccgroup.icd9_code as varchar(254)) as icd9_code,
    cast(sq_v_edg_cccgroup.dx as varchar(254)) as dx,
    cast(sq_v_edg_cccgroup.malignancy_ccc as smallint) as malignancy_ccc,
    cast(sq_v_edg_cccgroup.hemaimmuno_ccc as smallint) as hemaimmuno_ccc,
    cast(sq_v_edg_cccgroup.respiratory_ccc as smallint) as respiratory_ccc,
    cast(sq_v_edg_cccgroup.gastro_ccc as smallint) as gastro_ccc,
    cast(sq_v_edg_cccgroup.metabolic_ccc as smallint) as metabolic_ccc,
    cast(sq_v_edg_cccgroup.neuromusc_ccc as smallint) as neuromusc_ccc,
    cast(sq_v_edg_cccgroup.cardiovasc_ccc as smallint) as cardiovasc_ccc,
    cast(sq_v_edg_cccgroup.renal_ccc as smallint) as renal_ccc,
    cast(sq_v_edg_cccgroup.othercongen_ccc as smallint) as othercongen_ccc,
    cast(sq_v_edg_cccgroup.neonatal_ccc as smallint) as neonatal_ccc,
    cast(sq_v_edg_cccgroup.tech_dep_ccc as smallint) as tech_dep_ccc,
    cast(sq_v_edg_cccgroup.transplant_ccc as smallint) as transplant_ccc
from sq_v_edg_cccgroup
