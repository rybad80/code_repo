/*
Summary: identify ALL NEW diabetes patients at CHOP and flag new onset (within 1 year) patients for all types of
    diabetes. T1Y1_ind and T2Y1_ind are located in diabetes_patient_all, because MD/NP confirmed their
    diabetes types in OP diab_type flowsheet.
Source: identify new onset diabetes by inpatient flowsheet 9386 (documented as T1Y1 PROGRAM)
    + outpatient visit type as 'DIABETES NEW/TRANSFER'
Note:
    Inclusion:
        1. all inpatients (hospital) who newly diagnosed with any type of diabetes, even though some of them
        were confirmed to be type 2 by antibody testing in their follow-up OP visits, flagged as diag_ind.
        Patients were flagged as T1Y1 or T2Y1 program in diabetes_patient_all
        2. transferred diabetes: for patients who had at least one NEW DIABETES visit type in OP but haven't been
            diagnosed at CHOP.
Exclusion: patients who documented in T1Y1 IP FLO row when they admitted but refused to join in CHOP's T1Y1 program
Granularity level: patient level + monthly reporting level
Time Span: all new onset admitted diabetes patients who had IP t1y1 ICR Flowsheets
    in the last 15 months from reporting point (1st day of month)
Last updated: 10/6/22
*/

with flowsheet_visit_keys as (
    select
        visit_key,
        meas_val
    from
        {{ ref('flowsheet_all')}}
    where
        flowsheet_id = 9386
        and (lower(meas_val) = 't1y1' or lower(meas_val) like '%t1y1 modified (coaching in satellites)')
    group by
        visit_key,
        meas_val
),

stg_ip_t1y1 as (

/* The first data source to identify newly diagnosed diabetes is
using an inpatient flowsheet 9386 -- Disposition (newly diagnosed only) a few patients have multi admissions
documented as T1Y1 onset, should select the ealiest one as the new onset ip.
*/
    select
        encounter_inpatient.pat_key,
        encounter_inpatient.mrn,
        encounter_inpatient.encounter_date as ip_t1y1_dt,
        encounter_inpatient.hospital_admit_date,
        encounter_inpatient.hospital_discharge_date,
        encounter_inpatient.primary_dx,
        flowsheet_visit_keys.meas_val,
        row_number() over (partition by encounter_inpatient.pat_key order by ip_t1y1_dt) as ip_rn
    from
        {{ ref('encounter_inpatient')}} as encounter_inpatient
        inner join flowsheet_visit_keys as flowsheet_visit_keys
            on flowsheet_visit_keys.visit_key = encounter_inpatient.visit_key
),

ip_t1y1 as (--select the ealiest ip as the new onset t1
    select
        *
    from
        stg_ip_t1y1
    where
        ip_rn = 1
),

specialty_care_flowsheet_visit_keys as (
    select
        visit_key
    from
        {{ ref('flowsheet_all')}}
    where
        flowsheet_id = 7261    --type of diabetes
        and meas_val is not null --include all types of diabetes and have seen at chop dcc
    group by visit_key
),

op_t1y1 as (
/* the second data source to identify newly diagnosed diabetes is using the outpatient visit type.
included new or transfer for all types of diabetes
*/
select
    spc.pat_key,
    spc.mrn,
    spc.visit_type,
    spc.encounter_date as op_t1y1_dt,
    row_number() over (partition by spc.pat_key order by op_t1y1_dt) as op_rn
from
    {{ ref('encounter_specialty_care')}} as spc
    inner join specialty_care_flowsheet_visit_keys
        on specialty_care_flowsheet_visit_keys.visit_key = spc.visit_key
where
    lower(spc.visit_type) in (
        'diabetes t1y1 new',
        'new diabetes type 1 transfer',
        'new diabetes type 2 transfer',
        'new diabetes patient',
        'new possible diabetes')
    and spc.encounter_date <= current_date
)
--combine the two logics above and determine new diabetes cohort at chop:
--including diagnosed at chop and transfered to chop
select
    stg_patient.pat_key,
    stg_patient.patient_key,
    stg_patient.mrn,
    stg_patient.patient_name,
    coalesce(ip_t1y1.ip_t1y1_dt, op_t1y1.op_t1y1_dt) as new_diabetes_dt,
        --the ealiest date (ip enc date) at chop as newly diagnosed diabetes patients
    case when ip_t1y1.ip_t1y1_dt is not null then 1 else 0 end as ip_diag_ind,
        --new onset diabetes patients who admitted and diagnosed at chop
    ip_t1y1.hospital_admit_date,
    ip_t1y1.hospital_discharge_date,
    ip_t1y1.primary_dx as ip_primary_dx,
    case when ip_t1y1.ip_t1y1_dt is null then 1 else 0 end as new_transfer_ind,
        --diabetes patients newly transferred to chop
    op_t1y1.visit_type as op_visit_type, --earliest op visit
    op_t1y1.op_t1y1_dt as op_visit_date  --earliest op visit
from
    {{ ref('stg_patient')}} as stg_patient
    left join ip_t1y1 on ip_t1y1.pat_key = stg_patient.pat_key
    left join op_t1y1 on op_t1y1.pat_key = stg_patient.pat_key
                        and op_t1y1.op_rn = 1    --select the ealiest op visit as the new transfer to chop
where
    date(new_diabetes_dt) <= current_date
    and coalesce(ip_t1y1.pat_key, op_t1y1.pat_key) is not null
