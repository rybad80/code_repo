with
pat_base as (
    select
        birth_history.mrn,
        birth_history.dob,
        birth_history.visit_key as fcbh_visit_key,
        lower(birth_history.post_sdu_department_group_name) as post_sdu_department_group_name,
        lower(birth_history.living_status) as living_status,
        lower(birth_history.discharge_department) as discharge_department,
        lower(birth_history.discharge_disposition) as discharge_disposition,
        birth_history.delivery_date,
        date(birth_history.delivery_date) as delivery_dt,
        add_months(date(birth_history.delivery_date), 12) as delivery_dt_add_1yr
    from {{ ref('fetal_center_birth_history') }} as birth_history
    inner join {{ ref('fetal_center_pregnancy') }} as pregnancy
        on pregnancy.pregnancy_episode_id = birth_history.pregnancy_episode_id
    where  birth_history.delivery_date is not null and birth_history.delivery_method is not null
),
pat_subset as (
    select
        pat_base.mrn,
        pat_base.dob,
        pat_base.delivery_date,
        pat_base.delivery_dt,
        pat_base.post_sdu_department_group_name,
        pat_base.living_status,
        pat_base.discharge_department,
        pat_base.discharge_disposition,
        pat_base.fcbh_visit_key,
        max(case when lower(neo_nicu_treatment_team.treatment_team) like '%green%' then 1 else 0 end
            ) as team_green_ind,
        max(case when surgery_procedure.service is not null then 1 else 0 end
            ) as cardiac_operative_ind,
        case
            when team_green_ind = 1 then 'neonatal surgical service'
            when pat_base.post_sdu_department_group_name not in ('cicu', 'coic')
                then 'neonatal non-surgical service'
            when cardiac_operative_ind = 1 then 'cardiac surgery'
            when pat_base.post_sdu_department_group_name in ('cicu', 'coic') then 'cardiac non-operative'
            when pat_base.living_status = 'fetal demise'
                or (pat_base.discharge_department = 'sdu' and pat_base.discharge_disposition = 'expired')
                then 'palliative care'
            when pat_base.discharge_department = 'sdu'
                and pat_base.discharge_disposition = 'discharged (routine)'
                then 'well baby discharges'
            else null end as sub_cohort,
        case when sub_cohort in ('neonatal surgical service',
                                'neonatal non-surgical service',
                                'cardiac surgery',
                                'cardiac non-operative'
                                ) then 1 else 0 end as drof_sub_cohort_ind
        from pat_base
        left join {{ ref('neo_nicu_treatment_team') }} as neo_nicu_treatment_team
            on pat_base.mrn = neo_nicu_treatment_team.mrn
        left join {{ ref('surgery_procedure') }} as surgery_procedure
            on pat_base.mrn = surgery_procedure.mrn
            and surgery_procedure.surgery_date between pat_base.delivery_dt and pat_base.delivery_dt_add_1yr
            and surgery_procedure.service = 'Cardiothoracic'
        group by
            pat_base.mrn,
            pat_base.dob,
            pat_base.delivery_date,
            pat_base.delivery_dt,
            pat_base.post_sdu_department_group_name,
            pat_base.living_status,
            pat_base.discharge_department,
            pat_base.discharge_disposition,
            pat_base.fcbh_visit_key
),
bh_enc as (
    select
        encounter_all.mrn,
        encounter_all.visit_key,
        encounter_all.csn,
        encounter_all.patient_name,
        encounter_all.encounter_date,
        encounter_all.provider_name,
        encounter_all.provider_id,
        encounter_all.department_name,
        encounter_all.department_id,
        encounter_all.visit_type,
        encounter_all.visit_type_id,
        encounter_all.encounter_type,
        encounter_all.encounter_type_id,
        encounter_all.appointment_status_id,
        year(add_months(encounter_all.encounter_date, 6)) as fiscal_year,
        year(add_months(pat_subset.delivery_dt, 6)) as fiscal_year_delivery,
        pat_subset.sub_cohort,
        pat_subset.drof_sub_cohort_ind,
        pat_subset.delivery_date,
        encounter_all.inpatient_ind,
        encounter_all.patient_class,
        encounter_all.hospital_admit_date,
        encounter_all.hospital_discharge_date,
        i.hospital_los_days,
        i.inpatient_los_days,
        i.icu_los_days,
        i.discharge_department,
        i.discharge_service,
        date_trunc('month', encounter_all.encounter_date) as visual_month,
        encounter_all.pat_key,
        encounter_all.hsp_acct_key
	from pat_subset
    inner join {{ ref('encounter_all') }} as encounter_all
        on pat_subset.fcbh_visit_key = encounter_all.visit_key
    left join {{ ref('encounter_inpatient') }} as i
		on pat_subset.fcbh_visit_key = i.visit_key
    where fiscal_year >= 2022
),
first_enc as (
    select enc_sorted.*
    from
        (select
            encounter_all.mrn,
            encounter_all.visit_key,
            encounter_all.csn,
            encounter_all.patient_name,
            encounter_all.encounter_date,
            encounter_all.provider_name,
            encounter_all.provider_id,
            encounter_all.department_name,
            encounter_all.department_id,
            encounter_all.visit_type,
            encounter_all.visit_type_id,
            encounter_all.encounter_type,
            encounter_all.encounter_type_id,
            encounter_all.appointment_status_id,
            year(add_months(encounter_all.encounter_date, 6)) as fiscal_year,
            year(add_months(date(pat_subset.delivery_date), 6)) as fiscal_year_delivery,
            pat_subset.sub_cohort,
            pat_subset.drof_sub_cohort_ind,
            pat_subset.delivery_date,
            encounter_all.inpatient_ind,
            encounter_all.patient_class,
            encounter_all.hospital_admit_date,
            encounter_all.hospital_discharge_date,
            date_trunc('month', encounter_all.encounter_date) as visual_month,
            encounter_all.pat_key,
            encounter_all.hsp_acct_key,
            row_number() over (
                partition by encounter_all.mrn
                order by encounter_all.encounter_date, abs(encounter_all.age_days), encounter_all.visit_key
                ) as enc_seq_num
            from pat_subset
            inner join {{ ref('encounter_all') }} as encounter_all
                on pat_subset.mrn = encounter_all.mrn
            where pat_subset.fcbh_visit_key is null
        ) as enc_sorted
    where enc_sorted.enc_seq_num = 1 and enc_sorted.fiscal_year >= 2022
)

select * from bh_enc
union all
select
    mrn,
    visit_key,
    csn,
    patient_name,
    encounter_date,
    provider_name,
    provider_id,
    department_name,
    department_id,
    visit_type,
    visit_type_id,
    encounter_type,
    encounter_type_id,
    appointment_status_id,
    fiscal_year,
    fiscal_year_delivery,
    sub_cohort,
    drof_sub_cohort_ind,
    delivery_date,
    inpatient_ind,
    patient_class,
    hospital_admit_date,
    hospital_discharge_date,
    null as hospital_los_days,
    null as inpatient_los_days,
    null as icu_los_days,
    null as discharge_department,
    null as discharge_service,
    visual_month,
    pat_key,
    hsp_acct_key
from first_enc
