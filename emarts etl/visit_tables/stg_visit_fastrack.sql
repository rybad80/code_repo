with clean_pat_disc as (
    select
        reasoncode,
        trim(regexp_replace(patient_number, '[^0-9]+', '')) as patient_number,
        admdate,
        socdate,
        actiontype,
        ctr,
        discdate
    from
        {{source('fastrack_ods','pat_disc')}} as pat_disc
    where
        pat_disc.actiontype = 'A'
        and pat_disc.ctr
        = (
            select max(ctr)
            from
                 {{source('fastrack_ods','pat_disc')}} as pat_disc2
            where
                pat_disc2.actiontype = 'A'
                and pat_disc2.patient_number = pat_disc.patient_number
                and pat_disc2.socdate = pat_disc.socdate
        )
),

max_ctr as (
    select
        max(ctr) as maxctr,
        patient_number,
        cast(socdate as varchar(112)) as socdatestr
    from
        {{source('fastrack_ods','pat_disc')}}
    where
        actiontype = 'A'
    group by
        patient_number,
        socdate

),

discharges as (
    select
        patient_number,
        ctr,
        actiontype,
        cast(socdate as varchar(112)) as socdatestr,
        discdate,
        round(cast(reasoncode as decimal) + 0.005, 3) as v_reasoncode
    from
        {{source('fastrack_ods','pat_disc')}}
    where
        actiontype = 'I'
        and pat_disc.ctr
        = (
            select max(ctr)
            from
                 {{source('fastrack_ods','pat_disc')}} as pat_disc2
            where
                pat_disc2.actiontype = 'I'
                and pat_disc2.patient_number = pat_disc.patient_number
                and pat_disc2.socdate = pat_disc.socdate
        )
),

pending as (
    select
        patient_number,
        ctr,
        actiontype,
        penddate,
        cast(socdate as varchar(112)) as socdatestr,
        'ENCOUNTER APPOINTMENT STATUS' as dict_cat_nm,
        3 as appt_stat_src_id
    from
        {{source('fastrack_ods','pat_disc')}}
    where
        actiontype = 'P'
        and pat_disc.ctr
        = (
            select max(ctr)
            from
                 {{source('fastrack_ods','pat_disc')}} as pat_disc2
            where
                pat_disc2.actiontype = 'P'
                and pat_disc2.patient_number = pat_disc.patient_number
                and pat_disc2.socdate = pat_disc.socdate
        )
),

financial_class as (
    -- currently how Fastrack handles duplicate fc_abbr of which there are 3
    -- The correct way to do it would be to join on the financial_class table
    -- by fc_abbr and where discharge date is between create_dt and upd_dt
    select
        fc_abbr,
        fc_key,
        row_number() over (partition by fc_abbr order by create_dt) as row_num
    from
        {{source('cdw','financial_class')}}

),

stage as (
    select
        pat_disc.reasoncode as reasoncode_a,
        max_ctr.maxctr,
        pat_disc.patient_number,
        pat_disc.admdate,
        pat_disc.socdate,
        pat_disc.actiontype,
        pat_disc.ctr,
        patient.pat_key,
        patient.pat_id,
        patient.prov_key,
        pat_disc.discdate,
        p.medical_record_number,
        p.patient_birth_date,
        pat_disc.patient_number || lpad(max_ctr.maxctr, 3, '0') || '.005' as v_i_lkp_enc_id,
        pat_disc.patient_number || lpad(pat_disc.ctr, 3, '0') || '.005' as v_enc_id,
        cast(
        case
            when actiontype = 'A' then v_enc_id
            else v_i_lkp_enc_id
        end as decimal) as enc_id,
        coalesce(p.patient_carrier_1, p.patient_carrier_a, p.carrier1_hha) as carrier_coalesce,
        case when carrier_coalesce > 0 then carrier_coalesce + 0.005 end as v_bp_id,
        cast(p.patient_doctor_number + 0.005 as varchar(20)) as v_prov_id,
        cast(p.referral_source_code + 0.005 as varchar(20)) as in_referral_source_code,
        p.patient_carrier_1,
p.patient_carrier_a,
p.carrier1_hha
    from
        clean_pat_disc as pat_disc
        left join {{source('fastrack_ods','patientm')}} as p
            on p.patient_number = pat_disc.patient_number
        left join {{source('cdw','patient')}} as patient
            on patient.pat_mrn_id = p.medical_record_number
        left join max_ctr
            on max_ctr.patient_number = pat_disc.patient_number
            and max_ctr.socdatestr = pat_disc.socdate
)

select
    stg_visit_key_lookup.visit_key,
    case
        when stage.medical_record_number is not null and length(stage.medical_record_number) != 8
        then -1
        else coalesce(stage.pat_key, 0)
    end as pat_key,
    case
        when benefit_plan.bp_id is not null
            and financial_class.fc_key is null
        then -1
        else coalesce(financial_class.fc_key, -1)
    end as fc_key,
    coalesce(stage.prov_key, -1) as pc_prov_key,
    coalesce(provider.prov_key, 0) as visit_prov_key,
    department.dept_key,
    0 as eff_dept_key,
    0 as proc_key,
    0 as acct_key,
    location.loc_key,
    coalesce(bp_key, 0) as bp_key,
    case
        when benefit_plan.record_stat_epp is null
        then 0
        else coalesce(payor.payor_key, -1)
    end as payor_key,
    0 as prgm_key,
    coalesce(service_area.svc_area_key, 0) as svc_area_key,
    master_date.dt_key as contact_dt_key,
    coalesce(ref_src_key, 0) as ref_src_key,
    0 as rfl_key,
    0 as appt_visit_type_key,
    0 as hsp_acct_key,
    0 as cvg_key,
    0 as avs_emp_key,
    0 as checkin_emp_key,
    0 as appt_entry_emp_key,
    cast(0 as bigint) as less_72hr_visit_key,
    0 as res_stdy_key,
    0 as cosign_emp_key,
    0 as supervisor_prov_key,
    cast(-1 as bigint) as ip_documented_visit_key,
    enc_type.dict_key as dict_enc_type_key,
    coalesce(appointment_status.dict_key, 0) as dict_appt_stat_key,
    -2 as dict_appt_lag_bkt_key,
    -2 as dict_copay_type_key,
    -2 as dict_visit_last_stay_cls_key,
    0 as dim_phone_reminder_stat_key,
    dim_reason_for_discharge.dim_rsn_disch_key,
    0 as dim_adt_pat_class_key,
    -2 as dim_visit_cncl_rsn_key,
    0 as dim_supervisor_prov_type_key,
    0 as dim_routed_msg_priority_key,
    0 as level_svc_mod1_key,
    0 as level_svc_mod2_key,
    0 as level_svc_mod3_key,
    0 as level_svc_mod4_key,
    round(stage.enc_id, 3) as enc_id,
    NULL as claim_id,
    cast(extract(epoch from stage.socdate - stage.patient_birth_date) / 60 / 60 / 24 / 365.25 as numeric(9, 4)) as age,
    cast(NULL as int) as bp_sys,
    cast(NULL as int) as bp_dias,
    cast(NULL as int) as temp,
    cast(NULL as int) as pulse,
    cast(NULL as int) as wt_oz,
    cast(NULL as int) as wt_kg,
    cast(NULL as varchar(7)) as ht_raw,
    cast(NULL as int) as ht_cm,
    cast(NULL as int) as respirations,
    cast(NULL as int) as head_circ,
    cast(NULL as varchar(7)) as appt_stat,
    cast(NULL as varchar(7)) as appt_block,
    cast(NULL as varchar(7)) as hosp_admit_type,
    cast(NULL as varchar(7)) as entity,
    cast(NULL as int) as los_hours,
    cast(NULL as varchar(7)) as age_display,
    cast(NULL as int) as appt_lag_days,
    extract(epoch from stage.socdate - stage.patient_birth_date) / 60 / 60 / 24 as age_days,
    cast(NULL as int) as copay_due,
    cast(NULL as int) as copay_coll,
    cast(NULL as int) as self_pay_amt,
    cast(NULL as varchar(7)) as chrg_slip_num,
    cast(NULL as varchar(7)) as copay_ref_num,
    round(stage.enc_id) as appt_sn,
    cast(NULL as varchar(7)) as contact_cmt,
    cast(NULL as int) as enc_dt_real,
    cast(NULL as int) as appt_lgth_min,
    cast(NULL as int) as bmi,
    cast(NULL as varchar(7)) as bill_nbr,
    cast(NULL as varchar(7)) as cancel_reason_cmt,
    cast(NULL as varchar(7)) as los_proc_cd,
    cast(NULL as varchar(7)) as visit_stat_color_cd,
    stage.socdate as eff_dt,
    cast(NULL as timestamp) as lmp_dt,
    cast(NULL as timestamp) as appt_dt,
    cast(NULL as timestamp) as appt_checkin_dt,
    cast(NULL as timestamp) as appt_checkout_dt,
    cast(NULL as timestamp) as hosp_admit_dt,
    cast(NULL as timestamp) as hosp_dischrg_dt,
    -- case when discharges.discdate is not null then 1 else -2 end as enc_closed_ind,
    cast(discharges.discdate as date) as dischrg_dt,
    cast(NULL as timestamp) as appt_made_dt,
    cast(NULL as timestamp) as avs_print_dt,
    cast(NULL as timestamp) as enc_close_dt,
    pending.penddate as appt_cancel_dt,
    cast(NULL as timestamp) as appt_entry_dt,
    cast(NULL as timestamp) as chart_cosign_dt,
    -- Mapping has logic to create enc_closed_ind but not used
    -- instead all Fastrack data is set to -2 use below if updating to use other logic
    -2 as enc_closed_ind,
    -2 as adm_for_surg_ind,
    -2 as is_walk_in_ind,
    -2 as rfl_req_ind,
    -2 as less_72hr_hosp_admit_ind,
    0 as appt_cancel_24hr_ind,
    0 as appt_cancel_48hr_ind,
    0 as visit_last_stay_class_ind,
    case
        when stage.medical_record_number is not null and length(stage.medical_record_number) != 8
        then '-1'
        else cast(coalesce(stage.pat_id, '0') as character varying(254))
    end as pat_id,
    cast(coalesce(provider.prov_id,'0') as character varying(254)) as visit_prov_id,
    cast(coalesce(pc_provider.prov_id, '-1') as character varying(254)) as pc_prov_id,
    cast(0 as character varying(254)) as dischrg_prov_id,
     case
        when benefit_plan.record_stat_epp is null
        then 0
        else coalesce(payor.payor_id, -1)
    end as payor_id,
    cast(0 as bigint) as dischrg_prov_key,
    current_timestamp as create_dt,
    'FASTRACK' as create_by,
    current_timestamp as upd_dt,
    'FASTRACK' as upd_by
from
    stage
    left join {{ref('stg_visit_key_lookup')}} as stg_visit_key_lookup
        on stg_visit_key_lookup.encounter_id = stage.ctr
        and stg_visit_key_lookup.patient_id = stage.patient_number
        and stg_visit_key_lookup.source_name = 'fastrack'
    left join {{source('cdw','benefit_plan')}} as benefit_plan
        on benefit_plan.bp_id = stage.v_bp_id
    left join financial_class
        on lower(financial_class.fc_abbr) = lower(benefit_plan.record_stat_epp)
        and financial_class.row_num = 1
    left join {{source('cdw','provider')}} as provider
        on provider.prov_id = stage.v_prov_id
    left join {{source('cdw','provider')}} as pc_provider
        on pc_provider.prov_key = stage.prov_key
    left join {{source('cdw','department')}} as department
        on department.dept_id = 81
    left join {{source('cdw','location')}} as location
        on location.loc_id = 1021
    left join {{source('cdw','payor')}} as payor
        on payor.payor_id = stage.v_bp_id
    left join {{source('cdw','service_area')}} as service_area
        on service_area.svc_area_id = 10
    left join {{source('cdw','master_date')}} as master_date
        on master_date.full_dt = stage.admdate
    left join {{source('cdw','referral_source')}} as referral_source
        on referral_source.ref_src_id = stage.in_referral_source_code
    left join {{source('cdw','cdw_dictionary')}} as enc_type
        on enc_type.dict_cat_key = 5
        and enc_type.src_id = 91
    left join discharges
        on discharges.patient_number = stage.patient_number
        and discharges.socdatestr = stage.socdate
    left join pending
        on pending.patient_number = stage.patient_number
        and pending.socdatestr = stage.socdate
    left join {{source('cdw','cdw_dictionary')}} as appointment_status
        on appointment_status.dict_cat_nm = pending.dict_cat_nm
        and appointment_status.src_id = pending.appt_stat_src_id
    left join {{source('cdw','dim_reason_for_discharge')}} as dim_reason_for_discharge
        on dim_reason_for_discharge.rsn_disch_cd = discharges.v_reasoncode
