with backup_copy as (
SELECT 
    lpad(MRN,8,'0') as "mrn", 
    redcap_repeat_instrument as "redcap_repeat_instrument", 
    redcap_repeat_instance as "redcap_repeat_instance", 
    lab_date as "lab_date", 
    age_labs as "age_labs", 
    tc as "tc", 
    hdl as "hdl", 
    ldl as "ldl", 
    tg as "tg", 
    nonhdl as "nonhdl", 
    t4 as "t4", 
    tsh as "tsh", 
    alt as "alt", 
    ast as "ast", 
    ck as "ck", 
    glucose as "glucose", 
    insulin as "insulin", 
    hba1c as "hba1c", 
    vitamin_d as "vitamin_d", 
    lpa as "lpa", 
    lpb as "lpb", 
    hgb as "hgb", 
    vit_a as "vit_a", 
    vit_e as "vit_e", 
    vit_k as "vit_k"
FROM 
    cdwdev..LIPIDSV2_DATA_2023_02_01_1333
where
    redcap_repeat_instrument =  'lipid_labs'
    and lab_date <> ''

),

redcap as (
select 
   MRN,
    REDCAP_REPEAT_INSTANCE, 
    LAB_DATE, 
    AGE_LABS, 
    TC, 
    HDL, 
    LDL, 
    TG, 
    NONHDL, 
    T4, 
    TSH, 
    ALT, 
    AST, 
    CK, 
    GLUCOSE, 
    INSULIN, 
    HBA1C, 
    VITAMIN_D, 
    LPA, 
    LPB, 
    HGB, 
    VIT_A, 
    VIT_E, 
    VIT_K --select *
from cdw_ods_uat..redcap_lipids_v2
where
     lipid_labs_complete is not null
     and lab_date is not null
),

max_inst as (
select
      mrn,
      max(redcap_repeat_instance) as max_inst
from cdw_ods_uat..redcap_lipids_v2
group by
      mrn
)

select
    "mrn", 
   "redcap_repeat_instrument", 
    row_number() over (partition by "mrn" order by "lab_date")+coalesce(max_inst,0) as "redcap_repeat_instance", 
    "lab_date", 
   -- "age_labs", 
    "tc", 
    "hdl", 
    "ldl", 
    "tg", 
    --"nonhdl", 
    "t4", 
     "tsh", 
    "alt", 
    "ast", 
    "ck", 
    "glucose", 
    "insulin", 
    "hba1c", 
    "vitamin_d", 
    "lpa", 
    "lpb", 
    "hgb", 
    "vit_a", 
    "vit_e", 
   "vit_k"
from       
    backup_copy
     full outer join redcap
      on backup_copy."mrn" = redcap.mrn
         and (date(backup_copy."lab_date") = date(redcap.lab_date) or backup_copy."age_labs" = redcap.age_labs)
     left join max_inst
      on max_inst.mrn = backup_copy."mrn"
where
    redcap.mrn is null         
