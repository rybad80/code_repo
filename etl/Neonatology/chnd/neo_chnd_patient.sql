{{ config(meta = {
    'critical': true
}) }}

with
preferred_mpp as (
    select
        patient_pk,
        gravida,
        delivery_typedispval,
        n1_minute,
        n5_minutes,
        n10_miniutes,
        fetal_surgerydispval,
        multiple_gestationdispval,
        antenatal_steroidsdispval,
        /* prefer chnd2.0 rows -- make them row 1 if patient_pk has more than 1 row */
        row_number() over (
            partition by patient_pk
            order by
                case when datamigrationflag = 'CHND2.0' then 0 else 1 end
        ) as row_number
    from
        {{ source('neo_velos_ods', 'neo_velos_mpp') }}
)
select
    cast(demographics.pk_person as integer) as chnd_pat_key,
    demographics.person_code as chnd_id,
    demographics.person_fname as first_name,
    demographics.person_lname as last_name,
    demographics.patient_akaname as aka_name,
    cast(demographics.mrn as varchar(255)) as mrn,
    cast(demographics.person_dob as date) as dob,
    demographics.person_gender as gender,
    cast(demographics.gestationweeks as integer) as gestational_age_complete_weeks,
    cast(demographics.gestationdays as integer) as gestational_age_remainder_days,
    cast(demographics.birthweightval as integer) as birth_weight_g,
    case
        when demographics.patientinborn = 'Yes' then 1
        else 0
    end as inborn_ind,
    case
        when demographics.patientinborn = 'Yes' then 'CHOP SDU'
        else coalesce(
            demographics.birthhospitalname,
            demographics.otherbirthhospitalname
        )
    end as birth_hospital,
    cast(demographics.motherszipcodeatbirth as varchar(10)) as mom_zip_at_birth,
    preferred_mpp.delivery_typedispval as delivery_type,
    preferred_mpp.n1_minute as apgar_1_minute,
    preferred_mpp.n5_minutes as apgar_5_minutes,
    preferred_mpp.n10_miniutes as apgar_10_minutes,
    preferred_mpp.fetal_surgerydispval as fetal_surgery,
    preferred_mpp.multiple_gestationdispval as multiple_gestation,
    preferred_mpp.antenatal_steroidsdispval as antenatal_steroids,
    demographics.datamigrationflag as chnd_version
from
    {{ source('neo_velos_ods', 'neo_velos_allpatientdemographics') }} as demographics
    left join preferred_mpp
        on preferred_mpp.patient_pk = demographics.pk_person
        and preferred_mpp.row_number = 1
