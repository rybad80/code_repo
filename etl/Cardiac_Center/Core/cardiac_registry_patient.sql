with

cardiac_registry_patients as (
    select distinct
        registry_hospital_visit.pat_key
    from
        {{source('cdw', 'registry_hospital_visit')}} as registry_hospital_visit
    where
        registry_hospital_visit.pat_key != 0
),

/*extracardiac abnormalities, chromosomal abnormalities, and syndromes
can get duplicated in regestries (both parent and "other" fields).
will use setup CTEs to take distinct entries*/

extracard_ab_setup as (
    select distinct
        registry_patient_ncaa.pat_key,
        registry_patient_ncaa.ncaa_desc as extracard_ab
    from
        {{source('cdw', 'registry_patient_ncaa')}} as registry_patient_ncaa
    where
        registry_patient_ncaa.cur_rec_ind = 1
),

extracard_ab as (
    select
        extracard_ab_setup.pat_key,
        group_concat(extracard_ab_setup.extracard_ab, ';') as extracard_ab
    from
        extracard_ab_setup
    group by
        extracard_ab_setup.pat_key
),


extracard_ab_other_setup as (
    select distinct
        registry_patient_ncaa.pat_key,
        registry_patient_ncaa.ncaa_other_desc as extracard_ab_other
    from
        {{source('cdw', 'registry_patient_ncaa')}} as registry_patient_ncaa
    where
        registry_patient_ncaa.cur_rec_ind = 1
),

extracard_ab_other as (
    select
        extracard_ab_other_setup.pat_key,
        group_concat(extracard_ab_other_setup.extracard_ab_other, ';') as extracard_ab_other
    from
        extracard_ab_other_setup
    group by
        extracard_ab_other_setup.pat_key
),

chromosomal_ab_setup as (
    select distinct
        registry_patient_chrom_ab.pat_key,
        registry_patient_chrom_ab.chrom_ab_desc as chromosomal_ab
    from
        {{source('cdw', 'registry_patient_chrom_ab')}} as registry_patient_chrom_ab
    where
        registry_patient_chrom_ab.cur_rec_ind = 1
),

chromosomal_ab as (
    select
        chromosomal_ab_setup.pat_key,
        group_concat(chromosomal_ab_setup.chromosomal_ab, ';') as chromosomal_ab
    from
        chromosomal_ab_setup
    group by
        chromosomal_ab_setup.pat_key
),

chromosomal_ab_other_setup as (
    select
        registry_patient_chrom_ab.pat_key,
        registry_patient_chrom_ab.chrom_ab_other_desc as chromosomal_ab_other
    from
        {{source('cdw', 'registry_patient_chrom_ab')}} as registry_patient_chrom_ab
    where
        registry_patient_chrom_ab.cur_rec_ind = 1
),

chromosomal_ab_other as (
    select
        chromosomal_ab_other_setup.pat_key,
        group_concat(chromosomal_ab_other_setup.chromosomal_ab_other, ';') as chromosomal_ab_other
    from
        chromosomal_ab_other_setup
    group by
        chromosomal_ab_other_setup.pat_key
),


syndrome_setup as (
    select distinct
        registry_patient_syndrome.pat_key,
        registry_patient_syndrome.syndrm_desc as syndrome
    from
        {{source('cdw', 'registry_patient_syndrome')}} as registry_patient_syndrome
    where
        registry_patient_syndrome.cur_rec_ind = 1
),

syndrome as (
    select
        syndrome_setup.pat_key,
        group_concat(syndrome_setup.syndrome, ';') as syndrome
    from
        syndrome_setup
    group by
        syndrome_setup.pat_key
),

syndrome_other_setup as (
    select distinct
        registry_patient_syndrome.pat_key,
        registry_patient_syndrome.syndrm_other_desc as syndrome_other
    from
        {{source('cdw', 'registry_patient_syndrome')}} as registry_patient_syndrome
    where
        registry_patient_syndrome.cur_rec_ind = 1
),

syndrome_other as (
    select
        syndrome_other_setup.pat_key,
        group_concat(syndrome_other_setup.syndrome_other, ';') as syndrome_other
    from
        syndrome_other_setup
    group by
        syndrome_other_setup.pat_key
),

/*the following CTEs check phone encounters, epic outpatient, epic inpatient,
registry inpatient, sts followup, and date of death to be used as a last
contact date. phone encounters must be with a data specialist or PRO coordinator
*/

phone as (
    select
        stg_encounter.pat_key,
        date(max(stg_encounter.encounter_date)) as last_contact_date,
        'Telephone' as last_contact_method

    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join cardiac_registry_patients
            on stg_encounter.pat_key = cardiac_registry_patients.pat_key
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key

    where
        lower(stg_encounter.encounter_type) = 'telephone'
        and lower(provider.prov_id) in (
            '28321', -- booth, eurrai r
            '2002273', -- kane, kristin
            '2002283', -- mahle, marlene j
            '2003670', -- stagg, alyson
            '15201' -- veneziale, kelly l
        )
    group by
        stg_encounter.pat_key
),

outpatient as (
    select
        stg_encounter.pat_key,
        date(max(stg_encounter.encounter_date)) as last_contact_date,
        'Outpatient' as last_contact_method
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join cardiac_registry_patients
            on stg_encounter.pat_key = cardiac_registry_patients.pat_key
    where
        stg_encounter.encounter_type_id in (
            3, -- hospital encounter
            50, -- appointment
            101, -- office visit
            106, -- hospital
            108, -- immunization
            160 -- care coordination
        )
        and stg_encounter.appointment_status_id in (
            2, -- completed
            6 -- arrived
        )
    group by
        stg_encounter.pat_key
),

inpatient_epic as (
    select
        stg_encounter_inpatient.pat_key,
        date(max(stg_encounter.hospital_discharge_date)) as last_contact_date,
        'Inpatient Epic' as last_contact_method

    from
        {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = stg_encounter_inpatient.visit_key
        inner join cardiac_registry_patients
            on stg_encounter_inpatient.pat_key = cardiac_registry_patients.pat_key
    group by
        stg_encounter_inpatient.pat_key

),

inpatient_registry as (
    select
        registry_hospital_visit.pat_key,
        date(max(registry_hospital_visit.r_disch_dt)) as last_contact_date,
        'Inpatient Registry' as last_contact_method

    from
        {{source('cdw', 'registry_hospital_visit')}} as registry_hospital_visit
    where
        registry_hospital_visit.cur_rec_ind = 1
        and registry_hospital_visit.r_disch_dt is not null
    group by
        registry_hospital_visit.pat_key
),

sts_followup as (
    select
        registry_sts_followup.pat_key,
        date(max(registry_sts_followup.r_last_followup_dt)) as last_contact_date,
        'STS Followup' as last_contact_method

    from
        {{source('cdw', 'registry_sts_followup')}} as registry_sts_followup
    group by
        registry_sts_followup.pat_key
),


mortality as (
    select
        patient_all.pat_key,
        date(patient_all.death_date) as last_contact_date,
        'Date of Death' as last_contact_method

    from
        {{ref('patient_all')}} as patient_all
        inner join cardiac_registry_patients
            on patient_all.pat_key = cardiac_registry_patients.pat_key
    where
        patient_all.death_date is not null

),

last_contact_all as (
    select * from phone
    union all
    select * from outpatient
    union all
    select * from inpatient_epic
    union all
    select * from inpatient_registry
    union all
    select * from sts_followup
    union all
    select * from mortality
),

last_contact_seq as (
    select
        last_contact_all.pat_key,
        last_contact_all.last_contact_date,
        last_contact_all.last_contact_method,
        /*order by date and method so "date of death" will show for patients
        with death date and discharge date on same day*/
        row_number() over (partition by last_contact_all.pat_key
            order by last_contact_all.last_contact_date desc, last_contact_all.last_contact_method) as seq

    from
        last_contact_all
),

last_contact as (
    select
        last_contact_seq.pat_key,
        last_contact_seq.last_contact_date,
        last_contact_seq.last_contact_method
    from
        last_contact_seq
    where
        last_contact_seq.seq = 1

),

patient_info as (
    select
        cardiac_registry_patients.pat_key,
        registry_centr_demographics.r_pat_demographics_id as registry_patient_id,
        patient_all.patient_name,
        patient_all.mrn,
        patient_all.dob,
        patient_all.sex,
        patient_all.current_age,
        patient_all.race,
        patient_all.ethnicity,
        patient_all.race_ethnicity,
        registry_patient_anatomy.fund_dx_desc as fundamental_diagnosis,
        patient_all.deceased_ind,
        patient_all.death_date,
        patient_all.mychop_activation_ind,
        patient_all.mychop_declined_ind,
        coalesce(registry_patient_anatomy.r_birth_weight_in_kg, patient_all.birth_weight_kg) as birth_weight_kg,
        coalesce(registry_patient_anatomy.r_gest_age_in_weeks, patient_all.gestational_age_complete_weeks)
            as gest_age_weeks,
        registry_patient_anatomy.r_birth_lgth as birth_length_cm

    from cardiac_registry_patients
    inner join {{ref('patient_all')}} as patient_all
        on cardiac_registry_patients.pat_key = patient_all.pat_key
    inner join {{source('cdw', 'registry_centr_demographics')}} as registry_centr_demographics
        on cardiac_registry_patients.pat_key = registry_centr_demographics.pat_key
        and registry_centr_demographics.pat_key != 0
    left join {{source('cdw', 'registry_patient_anatomy')}} as registry_patient_anatomy
        on cardiac_registry_patients.pat_key = registry_patient_anatomy.pat_key
        and registry_patient_anatomy.cur_rec_ind = 1

    group by
        cardiac_registry_patients.pat_key,
        registry_centr_demographics.r_pat_demographics_id,
        patient_all.patient_name,
        patient_all.mrn,
        patient_all.dob,
        patient_all.sex,
        patient_all.current_age,
        patient_all.race,
        patient_all.ethnicity,
        patient_all.race_ethnicity,
        registry_patient_anatomy.fund_dx_desc,
        patient_all.deceased_ind,
        patient_all.death_date,
        patient_all.mychop_activation_ind,
        patient_all.mychop_declined_ind,
        registry_patient_anatomy.r_birth_weight_in_kg,
        patient_all.birth_weight_kg,
        registry_patient_anatomy.r_gest_age_in_weeks,
        patient_all.gestational_age_complete_weeks,
        registry_patient_anatomy.r_birth_lgth
)

select
    patient_info.pat_key,
    patient_info.registry_patient_id,
    patient_info.patient_name,
    patient_info.mrn,
    patient_info.dob,
    patient_info.sex,
    patient_info.current_age,
    patient_info.race,
    patient_info.ethnicity,
    patient_info.race_ethnicity,
    patient_info.fundamental_diagnosis,
    patient_info.deceased_ind,
    patient_info.death_date,
    patient_info.mychop_activation_ind,
    patient_info.mychop_declined_ind,
    patient_info.birth_weight_kg,
    patient_info.gest_age_weeks,
    patient_info.birth_length_cm,
    extracard_ab.extracard_ab,
    extracard_ab_other.extracard_ab_other,
    chromosomal_ab.chromosomal_ab,
    chromosomal_ab_other.chromosomal_ab_other,
    syndrome.syndrome,
    syndrome_other.syndrome_other,
    last_contact.last_contact_date,
    last_contact.last_contact_method

from patient_info
left join extracard_ab on patient_info.pat_key = extracard_ab.pat_key
left join extracard_ab_other on patient_info.pat_key = extracard_ab_other.pat_key
left join chromosomal_ab on patient_info.pat_key = chromosomal_ab.pat_key
left join chromosomal_ab_other on patient_info.pat_key = chromosomal_ab_other.pat_key
left join syndrome on patient_info.pat_key = syndrome.pat_key
left join syndrome_other on patient_info.pat_key = syndrome_other.pat_key
left join last_contact on patient_info.pat_key = last_contact.pat_key
