with fetal_heart as (
   select distinct
         rcq.field_nm,
         rcq.element_label,
         rcd.record as rec,
         rcd.value as rcd_value,
         substr(coalesce(rcea.element_desc, rcd.value), 1, 250) as val,
         dense_rank() over (partition by rcd.record, rcd.mstr_redcap_quest_key order by rcea.element_id) as row_num
   from
         {{source('cdw', 'redcap_detail')}} as rcd
         inner join
             {{source('cdw', 'master_redcap_project')}} as rcp on rcp.mstr_project_key = rcd.mstr_project_key
         left join {{source('cdw', 'master_redcap_question')}} as rcq
           on rcq.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key and rcq.cur_rec_ind = 1
         left join {{source('cdw', 'master_redcap_element_answr')}} as rcea
           on rcea.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key and rcd.value = rcea.element_id
   where
     app_title = 'Fetal Heart Program'
),

cat_diag as (
 select
      rec,
      val as prenatal_cardiac_dx
  from
     fetal_heart
where
     field_nm = 'prenatal_cardiac_dx'
),

cat_diag_grp_concat as (
      select
            rec,
            group_concat(prenatal_cardiac_dx, '|') as prenatal_cardiac_dx
  from
       cat_diag
group by
        rec
),

followup_card_pick as (
 select
      rec,
      val as followup_cardiologist
  from
      fetal_heart
where
     field_nm = 'followup_card_pick'
),

followup_card as (
  select
        rec,
        group_concat(followup_cardiologist, '|') as followup_cardiologist
    from
        followup_card_pick
 group by
        rec
),

fhp_pivot as (

select
      fetal_heart.rec,
      case when field_nm = 'study_id' then val end as study_id,
      case when field_nm = 'mother_mrn' then val end as mother_mrn,
      case when field_nm = 'mother_first_nm' then val end as mother_first_nm,
      case when field_nm = 'mother_last_nm' then val end as mother_last_nm,
      case when field_nm = 'outside_ref_date' then val end as outside_ref_date,
      case when field_nm = 'due_date' then val end as due_date,
      case when field_nm = 'init_fhp_date' then val end as  init_fhp_date,
      case when field_nm = 'gest_age' then val end as gestational_age_weeks,
      case when field_nm = 'chop_fetal_cardiologist' then val end as chop_fetal_cardiologist,
      case when field_nm = 'chop_fetal_card_other' then val end as chop_fetal_card_other,
      case when field_nm = 'postnatal_cardiologist' then val end as postnatal_cardiologist,
      case when field_nm = 'postnatal_card_other' then val end as postnatal_card_other,
      case when field_nm = 'chop_ct_surgeon' then val end as chop_ct_surgeon,
      case when field_nm = 'chop_ct_surg_oth' then val end as chop_ct_surg_oth,
      followup_cardiologist,
      case when field_nm = 'followup_card' then val end as followup_cardiologist_other,
      case when field_nm = 'referral_src_cardiac' then val end as referral_src_cardiac,
      case when field_nm = 'referral_src_ob_mfm' then val end as   referral_src_ob_mfm,
      case when field_nm = 'gravida' then val end as gravida,
      case when field_nm = 'para' then val end as para,
      case when field_nm = 'normal_dx_ind' then val end as normal_diagnosis,
      case when field_nm = 'prenatal_extra_card_dx' then val end as prenatal_extracardiac_diagnosis,
      prenatal_cardiac_dx as prenatal_cardiac_diagnosis,
      case when field_nm = 'prenatal_card_dx_oth' then val end as prenatal_cardiac_diagnosis_other,
      case when field_nm = 'prenatal_dx' then val end as prenatal_clinical_diagnosis,
      case when field_nm = 'art_flag' then val end as art_flag,
      case when field_nm = 'art_type' then val end as art_type,
      case when field_nm = 'severity_scale' then val end as severity_scale,
      case when field_nm = 'genetic_testing' then val end as genetic_testing,
      case when field_nm = 'genetic_testing_normal' then val end as genetic_testing_normal,
      case when field_nm = 'genetic_testing_type' then val end as genetic_testing_type,
      case when field_nm = 'genetic_test_results' then val end as genetic_test_results,
      case when field_nm = 'baby_gender' then val end as gender,
      case when field_nm = 'nuchal_trans'  then val end as nuchal_trans,
      case when field_nm = 'nuchal_trans_val' then val end as nuchal_trans_val,
      case when field_nm = 'sched_cerv_ripe_dt' then val end as sched_cerv_ripe_dt,
      case when field_nm = 'sched_c_section_dt' then val end as sched_c_section_dt,
      case when field_nm = 'sched_induction_dt' then val end as sched_induction_dt,
      case when field_nm = 'del_plan_comments' then val end as del_plan_comments,
      case when field_nm = 'baby_mrn' then val end as baby_mrn,
      case when field_nm = 'baby_first_name' then val end as     baby_first_name,
      case when field_nm = 'baby_last_name' then val end as baby_last_name,
      case when field_nm = 'baby_last_name_alias' then val end as baby_last_name_alias,
      case when field_nm = 'baby_dob' then val end as baby_dob,
      case when field_nm = 'postnatal_dx' then val end as  postnatal_dx,
      case when field_nm = 'pt_state_residence'  then val end as pt_state_residence,
      case when field_nm = 'pt_state_other' then val end as pt_state_other,
      case when field_nm = 'pt_zipcode'  then val end as pt_zipcode,
      case when field_nm = 'place_of_delivery' then val end as place_of_delivery,
      case when field_nm = 'place_of_delivery_oth' then val end as place_of_delivery_oth,
      case when field_nm = 'delivery_class' then val end as delivery_class,
      case when field_nm = 'delivery_method' then val end as delivery_method,
      case when field_nm = 'post_natal_comment'  then val end as post_natal_comment,
      case when field_nm = 'admit_unit' then val end as admit_unit,
      case when field_nm = 'chop_fhp_fu' then val end as chop_fhp_fu,
      case when field_nm = 'postnatal_card_eval' then val end as postnatal_card_eval,
      case when field_nm = 'admit_date' then val end as admit_date,
      case when field_nm = 'or_date' then val end as or_date,
      case when field_nm = 'cath_date' then val end as cath_date,
      case when field_nm = 'disch_date' then val end as disch_date,
      case when field_nm = 'expired_date' then val end as expired_date,
      case when field_nm = 'iufd_date' then val end as iufd_date,
      case when field_nm = 'tab_date' then val end as tab_date
from
     fetal_heart left join cat_diag_grp_concat on fetal_heart.rec = cat_diag_grp_concat.rec
                  left join followup_card on followup_card.rec = fetal_heart.rec
),


grp_concat1 as (
select
     rec,
      study_id,
      mother_mrn,
      mother_first_nm,
      mother_last_nm,
      outside_ref_date,
      due_date,
      init_fhp_date,
      gestational_age_weeks,
      cast(group_concat(chop_fetal_cardiologist, '|') as varchar(100)) as chop_fetal_cardiologist,
      chop_fetal_card_other,
      cast(group_concat(postnatal_cardiologist, '|') as varchar(100)) as postnatal_cardiologist,
      postnatal_card_other,
      followup_cardiologist,
      followup_cardiologist_other,
      cast(group_concat(chop_ct_surgeon, '|') as varchar(100)) as chop_ct_surgeon,
      chop_ct_surg_oth,
      referral_src_cardiac,
      referral_src_ob_mfm,
      gravida,
      para,
      normal_diagnosis,
      prenatal_cardiac_diagnosis,
      prenatal_cardiac_diagnosis_other,
      prenatal_extracardiac_diagnosis,
      prenatal_clinical_diagnosis,
      art_flag
from
     fhp_pivot
 group by
      rec,
      study_id,
      mother_mrn,
      mother_first_nm,
      mother_last_nm,
      outside_ref_date,
      due_date,
      init_fhp_date,
      gestational_age_weeks,
      chop_fetal_card_other,
      postnatal_card_other,
      followup_cardiologist,
      followup_cardiologist_other,
      chop_ct_surg_oth,
      referral_src_cardiac,
      referral_src_ob_mfm,
      gravida,
      para,
      normal_diagnosis,
      prenatal_cardiac_diagnosis,
      prenatal_cardiac_diagnosis_other,
      prenatal_extracardiac_diagnosis,
      prenatal_clinical_diagnosis,
      art_flag
),


grp_concat2 as (
select
      rec,
      cast(group_concat(art_type, '|') as varchar(100)) as art_type,
      severity_scale,
      genetic_testing,
      genetic_testing_normal,
      genetic_testing_type,
      genetic_test_results,
      gender,
      nuchal_trans,
      nuchal_trans_val,
      sched_cerv_ripe_dt,
      sched_c_section_dt,
      sched_induction_dt,
      del_plan_comments,
      baby_mrn,
      baby_first_name,
      baby_last_name,
      baby_last_name_alias,
      baby_dob,
      postnatal_dx,
      pt_state_residence,
      pt_state_other,
      pt_zipcode,
      place_of_delivery,
      place_of_delivery_oth,
      delivery_class,
      cast(group_concat(delivery_method, '|') as varchar(100)) as delivery_method,
      post_natal_comment,
      cast(group_concat(admit_unit, '|')as varchar(100)) as admit_unit,
      chop_fhp_fu,
      postnatal_card_eval,
      admit_date,
      or_date,
      cath_date,
      disch_date,
      expired_date,
      iufd_date,
      tab_date

from
     fhp_pivot
group by
      rec,
      severity_scale,
      genetic_testing,
      genetic_testing_normal,
      genetic_testing_type,
      genetic_test_results,
      gender,
      nuchal_trans,
      nuchal_trans_val,
      sched_cerv_ripe_dt,
      sched_c_section_dt,
      sched_induction_dt,
      del_plan_comments,
      baby_mrn,
      baby_first_name,
      baby_last_name,
      baby_last_name_alias,
      baby_dob,
      postnatal_dx,
      pt_state_residence,
      pt_state_other,
      pt_zipcode,
      place_of_delivery,
      place_of_delivery_oth,
      delivery_class,
      post_natal_comment,
      chop_fhp_fu,
      postnatal_card_eval,
      admit_date,
      or_date,
      cath_date,
      disch_date,
      expired_date,
      iufd_date,
      tab_date

),

all_data as (

select
      grp_concat1.rec,
      max(study_id) as study_id,
      max(mother_mrn) as mother_mrn,
      max(mother_first_nm) as mother_first_nm,
      max(mother_last_nm) as mother_last_nm,
      max(outside_ref_date) as outside_ref_date,
      max(due_date) as due_date,
      cast(max(init_fhp_date) as date) as initial_fhp_date,
      max(case
            when extract(
                  epoch from date(due_date) - date(now())) <= 0 then 0
            else cast(40 - (date(due_date) - date(now())) / 7.0 as integer) end
      ) as gestational_age_weeks,
      case
            when max(chop_fetal_cardiologist) = 'Other' then max(chop_fetal_card_other)
            else max(chop_fetal_cardiologist)
      end as chop_fetal_cardiologist,
      case
            when max(postnatal_cardiologist) = 'Other' then max(postnatal_card_other)
            else max(postnatal_cardiologist)
      end as postnatal_cardiologist,
      max(followup_cardiologist) as followup_cardiologist,
      max(followup_cardiologist_other) as followup_cardiologist_other,
      case
            when max(chop_ct_surgeon) = 'Other' then max(chop_ct_surg_oth)
      else max(chop_ct_surgeon) end as chop_ct_surgeon,
      max(referral_src_cardiac) as referral_source_cardiac,
      max(referral_src_ob_mfm) as referral_source_ob_mfm,
      max(gravida) as gravida,
      max(para) as para,
      max(normal_diagnosis) as normal_diagnosis,
      max(prenatal_cardiac_diagnosis) as prenatal_cardiac_diagnosis,
      max(prenatal_cardiac_diagnosis_other) as prenatal_cardiac_diagnosis_other,
      max(prenatal_clinical_diagnosis) as prenatal_clinical_diagnosis,
      max(prenatal_extracardiac_diagnosis) as prenatal_extracardiac_diagnosis,
      max(art_flag) as art_flag,
      max(art_type) as art_type,
      max(severity_scale) as severity_scale,
      max(genetic_testing) as genetic_testing,
      max(genetic_testing_normal) as genetic_testing_normal,
      max(genetic_testing_type) as genetic_testing_type,
      max(genetic_test_results) as genetic_test_results,
      max(gender) as gender,
      max(nuchal_trans) as nuchal_trans,
      max(nuchal_trans_val) as nuchal_trans_val,
      max(sched_cerv_ripe_dt) as sched_cerv_ripe_dt,
      max(sched_c_section_dt) as sched_c_section_dt,
      max(sched_induction_dt) as sched_induction_dt,
      max(del_plan_comments) as delivery_plan_comments,
      max(baby_mrn) as baby_mrn,
      max(baby_first_name) as baby_first_name,
      max(baby_last_name) as baby_last_name,
      max(baby_last_name_alias) as baby_last_name_alias,
      max(baby_dob) as baby_dob,
      max(postnatal_dx) as postnatal_dx,
      coalesce(max(pt_state_residence), max(pt_state_other)) as pt_state_residence,
      max(pt_zipcode) as pt_zipcode,
      coalesce(max(place_of_delivery), max(place_of_delivery_oth)) as place_of_delivery,
      max(delivery_class) as delivery_class,
      max(delivery_method) as delivery_method,
      max(post_natal_comment) as post_natal_comment,
      max(admit_unit) as admit_unit,
      max(chop_fhp_fu) as chop_fhp_fu,
      max(postnatal_card_eval) as postnatal_card_eval,
      max(admit_date) as admit_date,
      max(or_date) as or_date,
      max(cath_date) as cath_date,
      max(disch_date) as disch_date,
      max(expired_date) as expired_date,
      max(iufd_date) as iufd_date,
      max(tab_date) as  tab_date
from
     grp_concat1
     inner join grp_concat2 on grp_concat2.rec = grp_concat1.rec
group by grp_concat1.rec
)

select study_id,
       case when not(expired_date is null)
                 or not(iufd_date is null)
                 or not(tab_date is null)
                 or not(disch_date is null)
                 or not(chop_fhp_fu is null)
                 or not(postnatal_card_eval is null)
              then 'Inactive'
              when (baby_dob <= cast(now() as date)
                 or due_date <= cast(now() - 60 as date))
                 and (expired_date is null
                 and iufd_date is null
                 and tab_date is null)
              then 'Inactive-Followup'
              else 'Active' end as active_flag,
      mother_mrn,
      mother_first_nm as mother_first_name,
      mother_last_nm as mother_last_name,
      cast(outside_ref_date as date) as outside_ref_date,
      cast(due_date as date) as due_date,
      cast(initial_fhp_date as date) as initial_fhp_date,
      case when initial_fhp_date is not null then 1 else 0 end as initial_fhp_ind,
      gestational_age_weeks,
      chop_fetal_cardiologist,
      postnatal_cardiologist,
      followup_cardiologist,
      followup_cardiologist_other,
      chop_ct_surgeon,
      referral_source_cardiac,
      referral_source_ob_mfm,
      gravida,
      para,
      normal_diagnosis,
      prenatal_cardiac_diagnosis,
      prenatal_cardiac_diagnosis_other,
      prenatal_clinical_diagnosis,
      case when prenatal_extracardiac_diagnosis = 1 then 'Yes'
           when prenatal_extracardiac_diagnosis = 0 then 'No' end as prenatal_extracardiac_diagnosis,
      case when art_flag = 1 then 'Yes'
           when art_flag = 0 then 'No' end as art_flag,
      art_type,
      severity_scale,
      case when genetic_testing = 1 then 'Yes'
           when genetic_testing = 0 then 'No' end as genetic_testing,
      genetic_testing_normal,
      genetic_testing_type,
      genetic_test_results,
      gender,
      nuchal_trans,
      nuchal_trans_val,
      sched_cerv_ripe_dt as scheduled_cervical_ripening_date,
      sched_c_section_dt as scheduled_c_section_date,
      sched_induction_dt as scheduled_induction_date,
      delivery_plan_comments,
      baby_mrn,
      baby_first_name,
      baby_last_name,
      baby_last_name_alias,
      cast(baby_dob as date) as baby_dob,
      postnatal_dx,
      pt_state_residence,
      pt_zipcode,
      place_of_delivery,
      delivery_class,
      delivery_method,
      post_natal_comment,
      admit_unit,
      case when chop_fhp_fu = 1 then 'No'
              else 'Yes' end as chop_fhp_fu,
      postnatal_card_eval,
      cast(admit_date as date) as admit_date,
      cast(or_date as date) as or_date,
      cast(cath_date as date) as cath_date,
      cast(disch_date as date) as discharge_date,
      cast(expired_date as date) as expired_date,
      cast(iufd_date as date) as iufd_date,
      cast(tab_date as date) as  abort_date

from all_data
