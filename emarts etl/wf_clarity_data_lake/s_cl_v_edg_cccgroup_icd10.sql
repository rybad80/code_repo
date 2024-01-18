/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_v_edg_cccgroup_icd10",
  "MAPPING_NAME": "m_stg_load_cl_v_edg_cccgroup_icd10",
  "MAPPING_ID": 7906,
  "TARGET_ID": 7607,
  "TARGET_NAME": "s_cl_v_edg_cccgroup_icd10"
}
*/

with
sq_v_edg_cccgroup_icd10 as (
    select
        v_edg_cccgroup_icd10.dx_id,
        v_edg_cccgroup_icd10.dx_name,
        v_edg_cccgroup_icd10.icd10_code,
        v_edg_cccgroup_icd10.dx,
        v_edg_cccgroup_icd10.malignancy_ccc,
        v_edg_cccgroup_icd10.hemaimmuno_ccc,
        v_edg_cccgroup_icd10.respiratory_ccc,
        v_edg_cccgroup_icd10.gastro_ccc,
        v_edg_cccgroup_icd10.metabolic_ccc,
        v_edg_cccgroup_icd10.neuromusc_ccc,
        v_edg_cccgroup_icd10.cardiovasc_ccc,
        v_edg_cccgroup_icd10.renal_ccc,
        v_edg_cccgroup_icd10.othercongen_ccc,
        v_edg_cccgroup_icd10.neonatal_ccc,
        v_edg_cccgroup_icd10.tech_dep_ccc,
        v_edg_cccgroup_icd10.transplant_ccc
    from
        {{ source('clarity_ods', 'v_edg_cccgroup_icd10') }} as v_edg_cccgroup_icd10
)
select
    cast(sq_v_edg_cccgroup_icd10.dx_id as bigint) as dx_id,
    cast(sq_v_edg_cccgroup_icd10.dx_name as varchar(200)) as dx_name,
    cast(sq_v_edg_cccgroup_icd10.icd10_code as varchar(254)) as icd10_code,
    cast(sq_v_edg_cccgroup_icd10.dx as varchar(254)) as dx,
    cast(sq_v_edg_cccgroup_icd10.malignancy_ccc as smallint) as malignancy_ccc,
    cast(sq_v_edg_cccgroup_icd10.hemaimmuno_ccc as smallint) as hemaimmuno_ccc,
    cast(sq_v_edg_cccgroup_icd10.respiratory_ccc as smallint) as respiratory_ccc,
    cast(sq_v_edg_cccgroup_icd10.gastro_ccc as smallint) as gastro_ccc,
    cast(sq_v_edg_cccgroup_icd10.metabolic_ccc as smallint) as metabolic_ccc,
    cast(sq_v_edg_cccgroup_icd10.neuromusc_ccc as smallint) as neuromusc_ccc,
    cast(sq_v_edg_cccgroup_icd10.cardiovasc_ccc as smallint) as cardiovasc_ccc,
    cast(sq_v_edg_cccgroup_icd10.renal_ccc as smallint) as renal_ccc,
    cast(sq_v_edg_cccgroup_icd10.othercongen_ccc as smallint) as othercongen_ccc,
    cast(sq_v_edg_cccgroup_icd10.neonatal_ccc as smallint) as neonatal_ccc,
    cast(sq_v_edg_cccgroup_icd10.tech_dep_ccc as smallint) as tech_dep_ccc,
    cast(sq_v_edg_cccgroup_icd10.transplant_ccc as smallint) as transplant_ccc
from sq_v_edg_cccgroup_icd10
