{{
  config(
    meta = {
      'critical': false
    }
  )
}}

/* stg_nursing_up_p1_enc_diagnosis
capturing marked primary diagnoses and problem list (only currently admitted) rows
for inpatient encounters in the Unit Profiles time frame window
*/
    select
        dx_all.visit_key,
        dx_all.mrn,
        dx_all.encounter_date,
        dx_all.source_summary,  /*visit_diagnosis or problem_list*/
        dx_all.diagnosis_name,
        dx_all.visit_primary_ind,  /*must be 1 or be primary visit dx metric*/
        dx_all.visit_diagnosis_ind, /*always 1 for visit diagnosis rows*/
        dx_all.visit_diagnosis_seq_num,  /*usually 1 for visit diagnosis, for problem_list is null*/
        dx_all.diagnosis_id,
        dx_all.external_diagnosis_id,
        dx_all.hsp_acct_key,
        dx_all.problem_list_ind,  /* always 1 for problem list rows*/
        dx_all.ip_admit_primary_ind,
        dx_all.ip_admit_other_ind,
        ip.currently_admitted_ind,
        ip.admission_dept_key,
        ip.discharge_dept_key,
        dx_all.marked_primary_ind
    from
        {{ ref('diagnosis_encounter_all') }} as dx_all
        inner join {{ ref('encounter_inpatient') }} as ip
            on dx_all.visit_key = ip.visit_key   -- only inpatient
    where
        dx_all.encounter_date between '2019-12-15' and current_date
        and (
            ( /*primary visit diagnoses*/
            dx_all.source_summary like '%visit_diagnosis%'
            and dx_all.visit_diagnosis_ind = 1
            /*capturing for visit_diagnosis, 'visit_diagnosis, pb_transaction' & 'visit_diagnosis, problem_list'*/
            /* visit/diagnosis combination can be found in the CDW table - VISIT_DIAGNOSIS */
            /* whether any of the visit diagnosis status types which include the keyword 'PRIMARY' are set to 1 */
--	        and dx_all.visit_primary_ind = 1 
            /* if the diagnosis had the 'VISIT - PRIMARY' visit_diagnosis status type  */
--          and dx_all.visit_diagnosis_seq_num = 1 
            /* only grabbing the first visit_diagnosis entered for each encounter  */
            )
        or ( /*only current problem lists*/
            dx_all.source_summary like '%problem_list%'
            and dx_all.problem_list_ind = 1
            and ip.currently_admitted_ind = 1
            )
        )
