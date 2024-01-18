with immune as (
    select
        encounter_primary_care.department_name,
        patient_immunization.pat_key,
        patient_immunization.visit_key,
        patient_immunization.immun_key,
        patient_immunization.immun_dt,
        -- to check for active status after the immune visit
        add_months(date_trunc('month', patient_immunization.immun_dt),1) as immun_month_year,
        immunization.immun_id,
        immunization.immun_nm,
        cast(dict_immun_stat.src_id as int) as flu_given_ind,
        patient_immunization.immun_entry_dt,
        patient_immunization.immun_expr_dt,
        patient_immunization.lot,
        row_number()
            over(
                partition by patient_immunization.pat_key, immun_month_year order by patient_immunization.immun_dt
            )
            as multi_shot_order

    from 
        {{source('cdw', 'patient_immunization')}} as patient_immunization
        inner join {{ref('encounter_primary_care')}} as encounter_primary_care
            on patient_immunization.visit_key = encounter_primary_care.visit_key
            -- immunization adminstered at the time of the encounter not recorded at the time of encounter
            and patient_immunization.immun_dt = encounter_primary_care.encounter_date
        inner join {{source('cdw', 'immunization')}} as immunization
            on patient_immunization.immun_key = immunization.immun_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_immun_stat
            on dict_immun_stat.dict_key = patient_immunization.dict_immun_stat_key
        inner join {{ref('lookup_influenza_vaccine')}} as lookup_influenza_vaccine
            on immunization.immun_id = lookup_influenza_vaccine.immun_id

    where 
        dict_immun_stat.src_id = 1 -- immun status = Given
        and patient_immunization.immun_dt >= '2015-07-01 00:00:00'
)

select
    {{
        dbt_utils.surrogate_key([
            'care_network_primary_care_active_patients.pat_key',
            'care_network_primary_care_active_patients.month_year'
        ])
    }} as primary_key,
    coalesce(immune.department_name, care_network_primary_care_active_patients.department_name) as department_name,
    'Flu Vaccination Rate' as metric_name,
    'pc_ee_flu_vaccination_rate' as metric_id,
    care_network_primary_care_active_patients.pat_key,
    coalesce(immune.immun_dt, care_network_primary_care_active_patients.month_year) as metric_date,
    immune.immun_dt,
    immune.visit_key,
    immune.immun_key,
    immune.immun_id,
    immune.immun_nm,
    immune.flu_given_ind,
    immune.immun_entry_dt,
    immune.immun_expr_dt,
    immune.lot

from 
    {{ref('care_network_primary_care_active_patients')}} as care_network_primary_care_active_patients
    left join immune 
        on care_network_primary_care_active_patients.pat_key = immune.pat_key
        and care_network_primary_care_active_patients.month_year = immune.immun_month_year
        and immune.multi_shot_order = 1

where
    care_network_primary_care_active_patients.month_year >= '2015-07-01 00:00:00' -- FY16 onwards
    and care_network_primary_care_active_patients.pc_active_patient_ind = 1
