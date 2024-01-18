{{ config(meta = {
    'critical': true
}) }}

-- identify department to which encounter was billed for AHP from "quick question" (pre Call Hub go-live)
select
    stg_encounter.encounter_key,
    x_dept.primary_dept_id_c_id as ahp_contract_dept_id,
    stg_department_all.department_name as ahp_contract_dept_name,
    pc_dept.department_display_name as ahp_contract_dept_display,
    case
        when ((pc_dept.department_id is not null
            -- exclude calls contracted to AHP
            and ahp_contract_dept_id != 37)
            or ahp_contract_dept_id = 66315012 -- North Hills Care Network (now inactive)
            )
    then 1 else 0 end as primary_care_triage_ind,
    case when ahp_contract_dept_id in (
            2,         -- UNKNOWN DEPARTMENT
            777700102, -- CHILDRENS MED GROUP
            777700103, -- BMS COMMUNITY CLINIC
            777700104, -- MONTEREY PED GRP
            777700105  -- SOUTH BASCOM PEDS
        ) then 1 else 0
    end as external_encounter_ind
from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{ source('clarity_ods', 'x_primary_dept_id_c') }} as x_dept
        on x_dept.pat_enc_csn_id = stg_encounter.csn
    inner join {{ ref('stg_department_all') }} as stg_department_all
        on stg_department_all.department_id = x_dept.primary_dept_id_c_id
    left join {{ ref('lookup_care_network_department_cost_center_sites') }} as pc_dept
        on pc_dept.department_id = x_dept.primary_dept_id_c_id
where stg_encounter.department_id in (37, 777700101)
