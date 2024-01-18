{{ config(
    meta={
        'critical': true
    }
) }}

with stg_demographics as (
	select
		patient.pat_id,
		zc_patient_race.name as race_ethnic_name,
        case
            when lower(zc_patient_race.name) not in
                ('refused', 'choose not to disclose', 'asked but unknown', 'unknown')
            then zc_patient_race.name
        end as race_ethnic_name_no_refused,
		0 as ethnic_ind,
		1 as race_ind

	from
		{{source('clarity_ods','patient')}} as patient
		left join {{source('clarity_ods','patient_race')}} as patient_race
			on patient_race.pat_id = patient.pat_id
		left join {{source('clarity_ods','zc_patient_race')}} as zc_patient_race
			on zc_patient_race.internal_id = patient_race.patient_race_c

	union all

	select
		patient.pat_id,
        zc_ethnic_bkgrnd.title as race_ethnic_name,
        case
            when lower(zc_ethnic_bkgrnd.title) not in
                ('refused', 'choose not to disclose', 'asked but unknown', 'unknown')
            then zc_ethnic_bkgrnd.title
        end as race_ethnic_name_no_refused,
        1 as ethnic_ind,
        0 as race_ind
	from
		{{source('clarity_ods','patient')}} as patient
		left join {{source('clarity_ods','ethnic_background')}} as ethnic_background
			on ethnic_background.pat_id = patient.pat_id
		left join {{source('clarity_ods','zc_ethnic_bkgrnd')}} as zc_ethnic_bkgrnd
			on zc_ethnic_bkgrnd.internal_id = ethnic_background.ethnic_bkgrnd_c
),

demographics as (
    select
        patient.pat_id,
        case
            when
                count(distinct patient_race.race_ethnic_name_no_refused) > 1 then 'Multi-Racial'
            else max(patient_race.race_ethnic_name)
        end as race,
        initcap(max(patient_ethnicity.race_ethnic_name)) as ethnicity,
        case when lower(ethnicity) = 'hispanic or latino' then 'Hispanic or Latino'
            when lower(ethnicity) != 'hispanic or latino' or ethnicity is null then
                case when lower(race) = 'white' then 'Non-Hispanic White'
                        when lower(race) = 'black or african american' then 'Non-Hispanic Black'
                        when lower(race) in ('asian', 'indian') then 'Asian'
                        else race
                    end
                else null
        end as race_ethnicity
    from
        {{source('clarity_ods','patient')}} as patient
        left join stg_demographics as patient_race
            on patient_race.pat_id = patient.pat_id
            and patient_race.race_ind = 1
        left join stg_demographics as patient_ethnicity
            on patient_ethnicity.pat_id = patient.pat_id
            and patient_ethnicity.ethnic_ind = 1
    group by
        patient.pat_id
),

pat_pref_name as (
    select
        patient_2.pat_id,
        pat_names.preferred_name,
        pat_names.preferred_type_c
    from
        {{source('clarity_ods', 'patient_2')}} as patient_2
        left join {{source('clarity_ods', 'names')}} as pat_names
            on patient_2.pat_name_record_id = pat_names.record_id
),

pat_gender as (
    select
        patient_4.pat_id,
        zc_gender_identity.name as gender_identity,
        zc_sex_asgn_at_birth.name as sex_assigned_at_birth
    from
        {{source('clarity_ods', 'patient_4')}} as patient_4
        inner join {{source('clarity_ods', 'zc_gender_identity')}} as zc_gender_identity
            on patient_4.gender_identity_c = zc_gender_identity.gender_identity_c
        inner join {{source('clarity_ods', 'zc_sex_asgn_at_birth')}} as zc_sex_asgn_at_birth
            on patient_4.sex_asgn_at_birth_c = zc_sex_asgn_at_birth.sex_asgn_at_birth_c
),


pat_other_info as (
    select
        patient.pat_id,
        case
            when patient.intrptr_needed_yn = 'Y' then 1
            else 0
        end as interpreter_needed
    from
        {{source('clarity_ods','patient')}} as patient
),

combine_sources as (
    select --noqa: PRS
        case --noqa: PRS
            when patient.pat_last_name like '%''%' or patient.pat_last_name like '%-%'
                then coalesce(
                    regexp_replace(
                        initcap(
                            regexp_replace(patient.pat_last_name, '([''-])(\w)', '\1  \2')),
                            '  ',
                            ''
                    ),
                    ''
                )
                || ', '
                || coalesce(initcap(patient.pat_first_name), '')
                || case when patient.pat_middle_name is not null then ' ' || patient.pat_middle_name else '' end
            else
                coalesce(initcap(patient.pat_last_name), '')
                || ', '
                || coalesce(initcap(patient.pat_first_name), '')
                || case when patient.pat_middle_name is not null then ' ' || patient.pat_middle_name else '' end
        end as patient_name, --noqa: L019
        patient.pat_first_name as patient_first_name,
        patient.pat_last_name as patient_last_name,
        patient.pat_middle_name as patient_middle_name,
        patient.pat_mrn_id as mrn,
        patient.pat_id,
        patient.birth_date as dob,
        zc_sex.abbr as sex,
        pat_gender.gender_identity,
        pat_gender.sex_assigned_at_birth,
        round(months_between(current_date, patient.birth_date) / 12, 2) as current_age,
        lower(patient.email_address) as email_address,
        patient.home_phone,
        add_line_1.address as mailing_address_line1,
        add_line_2.address as mailing_address_line2,
        patient.city as mailing_city,
        zc_state.title as mailing_state,
        patient.zip as mailing_zip,
        upper(
            case
                when zc_county.name is not null
                    and upper(zc_county.name) not like 'OTHER%' and upper(zc_county.name) not like 'UNKNOWN'
                    then zc_county.name
                when master_geography_by_zip.county is not null and upper(master_geography_by_zip.county) != 'DFLT'
                    then master_geography_by_zip.county
                else null
            end
        ) as county,
        zc_country.title as country,
        case
            when length(patient.ped_gest_age) >= 2 then cast(substring(patient.ped_gest_age, 1, 2) as int)
            else null
        end as gestational_age_complete_weeks,
        case
            when
                patient.ped_gest_age like '%,%' or patient.ped_gest_age like '%+%' or length(
                    patient.ped_gest_age
                ) <= 2 then 0
            when substring(patient.ped_gest_age, 3, 1) != '.'
                and substring(patient.ped_gest_age, 5, 1) != '/' then 0
            when
                substring(patient.ped_gest_age, 5, 1) = '/' then cast(substring(patient.ped_gest_age, 4, 1) as int)
            when
                substring(
                    patient.ped_gest_age, 3, 1
                ) = '.' then floor(cast(substring(patient.ped_gest_age, 3) || '0' as float) * 7)
        end as gestational_age_remainder_days,
        round((patient_3.ped_birth_wt_num * 0.0283495), 3) as birth_weight_kg,
        zc_language.title as preferred_language,
        case
            when pat_pref_name.preferred_type_c in (1, 100000)
            then
                case --noqa: PRS
                    when patient.pat_last_name like '%''%' or patient.pat_last_name like '%-%'
                        then coalesce(
                            regexp_replace(
                                initcap(
                                    regexp_replace(patient.pat_last_name, '([''-])(\w)', '\1  \2')),
                                    '  ',
                                    ''
                            ),
                            ''
                        )
                        || ', '
                        || coalesce(initcap(patient.pat_first_name), '')
                        || case when patient.pat_middle_name is not null then ' '
                        || patient.pat_middle_name else '' end
                    else
                        coalesce(initcap(patient.pat_last_name), '')
                        || ', '
                        || coalesce(initcap(patient.pat_first_name), '')
                        || case when patient.pat_middle_name is not null then ' '
                        || patient.pat_middle_name else '' end
                    end
            else patient_name
        end as preferred_name,
        demographics.race,
        demographics.ethnicity,
        demographics.race_ethnicity,
        pat_other_info.interpreter_needed,
        case
            when patient_4.send_sms_yn = 'Y' then 1
            else 0
        end as texting_opt_in_ind,
        case when lower(zc_patient_status.name) = 'deceased' then 1 else 0 end as deceased_ind,
        patient.death_date,
        case
            when patient.record_state_c = 1 then 'INACTIVE'
            when patient.record_state_c = 2 then 'DELETED'
        end as record_state,
        case
            when pat_merge_history.patient_mrg_hist is not null then 0
            else 1
        end as current_record_ind,
        'CLARITY' as create_source,
        'CLARITY' as update_source
    from
        {{ref('patient_snapshot')}} as patient
        left join {{source('clarity_ods','patient_3')}} as patient_3
            on patient_3.pat_id = patient.pat_id
        left join {{source('clarity_ods','patient_4')}} as patient_4
            on patient_4.pat_id = patient.pat_id
        left join demographics
            on demographics.pat_id = patient.pat_id
        left join {{source('cdw', 'master_geography')}} as master_geography_by_zip
            on master_geography_by_zip.zip = strleft(patient.zip, 5)
        left join pat_pref_name
            on patient.pat_id = pat_pref_name.pat_id
        left join pat_gender
            on patient.pat_id = pat_gender.pat_id
       left join pat_other_info
            on patient.pat_id = pat_other_info.pat_id
        left join {{ref('stg_dim_zip_market_mapping')}} as zip_market_mapping
            on zip_market_mapping.zip = strleft(patient.zip, 5)
        left join {{source('clarity_ods','zc_country')}} as zc_country
            on zc_country.country_c = patient.country_c
        left join {{source('clarity_ods','zc_state')}} as zc_state
            on zc_state.state_c = patient.state_c
        left join {{source('clarity_ods','zc_county')}} as zc_county
            on zc_county.county_c = patient.county_c
        left join {{source('clarity_ods','zc_sex')}} as zc_sex
            on zc_sex.rcpt_mem_sex_c = patient.sex_c
        left join {{source('clarity_ods','zc_patient_status')}} as zc_patient_status
            on zc_patient_status.patient_status_c = patient.pat_status_c
        left join {{source('clarity_ods','patient_type')}} as patient_type
            on patient_type.pat_id = patient.pat_id
            and patient_type.line = 1 -- Test patients will only have 1 line
        left join {{source('clarity_ods','zc_language')}} as zc_language
            on zc_language.language_c = patient.language_c
        left join {{source('clarity_ods','pat_address')}} as add_line_1
            on add_line_1.pat_id = patient.pat_id
            and add_line_1.line = 1
        left join {{source('clarity_ods','pat_address')}} as add_line_2
            on add_line_2.pat_id = patient.pat_id
            and add_line_2.line = 2
        left join {{ source('clarity_ods', 'pat_merge_history')}} as pat_merge_history
            on pat_merge_history.patient_mrg_hist = patient.pat_id
    where
        patient.dbt_valid_to is null        

    union all

    select
        patient_name,
        patient_first_name,
        patient_last_name,
        patient_middle_name,
        cast(mrn as character varying(50)) as mrn,
        cast(pat_id as character varying(50)) as pat_id,
        dob,
        sex,
        cast(gender_identity as character varying(50)) as gender_identity,
        cast(sex_assigned_at_birth as character varying(50)) as sex_assigned_at_birth,
        round(months_between(current_date, dob) / 12, 2) as current_age,
        cast(email_address as character varying(50)) as email_address,
        home_phone,
        mailing_address_line1,
        mailing_address_line2,
        mailing_city,
        mailing_state,
        mailing_zip,
        county,
        cast(country as character varying(50)) as country,
        gestational_age_complete_weeks,
        gestational_age_remainder_days,
        birth_weight_kg,
        preferred_language,
        preferred_name,
        cast(race as character varying(50)) as race,
        ethnicity,
        cast(race_ethnicity as character varying(50)) as race_ethnicity,
        null as interpreter_needed,
        texting_opt_in_ind,
        deceased_ind,
        cast(death_date as datetime) as death_date,
        cast(record_state as character varying(50)) as record_state,
        current_record_ind,
        'IDX' as create_source,
        'IDX' as update_source
    from
        {{source('manual_ods','idx_patient')}}

    union all

    select
        patient_name,
        patient_first_name,
        patient_last_name,
        patient_middle_name,
        cast(mrn as character varying(50)) as mrn,
        cast(pat_id as character varying(50)) as pat_id,
        dob,
        sex,
        cast(gender_identity as character varying(50)) as gender_identity,
        cast(sex_assigned_at_birth as character varying(50)) as sex_assigned_at_birth,
        round(months_between(current_date, dob) / 12, 2) as current_age,
        cast(email_address as character varying(50)) as email_address,
        home_phone,
        mailing_address_line1,
        mailing_address_line2,
        mailing_city,
        mailing_state,
        mailing_zip,
        county,
        cast(country as character varying(50)) as country,
        gestational_age_complete_weeks,
        gestational_age_remainder_days,
        birth_weight_kg,
        preferred_language,
        preferred_name,
        cast(race as character varying(50)) as race,
        ethnicity,
        cast(race_ethnicity as character varying(50)) as race_ethnicity,
        null as interpreter_needed,
        texting_opt_in_ind,
        deceased_ind,
        cast(death_date as datetime) as death_date,
        cast(record_state as character varying(50)) as record_state,
        0 as current_record_ind,
        'INFORMATICA' as create_source,
        'INFORMATICA' as update_source
    from
        {{source('manual_ods','clarity_deleted_patient')}}
    
)

select
    {{
        dbt_utils.surrogate_key([
            'pat_id',
            'create_source'
        ])
    }} as patient_key,
    patient_name,
    patient_first_name,
    patient_last_name,
    patient_middle_name,
    mrn,
    pat_id,
    dob,
    sex,
    gender_identity,
    sex_assigned_at_birth,
    current_age,
    email_address,
    home_phone,
    mailing_address_line1,
    mailing_address_line2,
    mailing_city,
    mailing_state,
    mailing_zip,
    county,
    country,
    gestational_age_complete_weeks,
    gestational_age_remainder_days,
    birth_weight_kg,
    preferred_language,
    preferred_name,
    race,
    ethnicity,
    race_ethnicity,
    interpreter_needed,
    texting_opt_in_ind,
    deceased_ind,
    death_date,
    record_state,
    current_record_ind,
    create_source,
    current_timestamp as create_date,
    update_source,
    current_timestamp as update_date
from
    combine_sources

union all

select
    -1 as patient_key,
    'UNSPECIFIED' as patient_name,
    'UNSPECIFIED' as patient_first_name,
    'UNSPECIFIED' as patient_last_name,
    'UNSPECIFIED' as patient_middle_name,
    'UNSPECIFIED' as mrn,
    '-1' as pat_id,
    '1900-01-01 00:00:00.000' as dob,
    'U' as sex,
    null AS gender_identity,
    null AS sex_assigned_at_birth,
    round(months_between(current_date, dob) / 12, 2) as current_age,
    'UNSPECIFIED' as email_address,
    'UNSPECIFIED' as home_phone,
    'UNSPECIFIED' as mailing_address_line1,
    'UNSPECIFIED' as mailing_address_line2,
    'UNSPECIFIED' as mailing_city,
    'UNSPECIFIED' as mailing_state,
    'UNSPECIFIED' as mailing_zip,
    'UNSPECIFIED' as county,
    'UNSPECIFIED' as country,
    null as gestational_age_complete_weeks,
    null as gestational_age_remainder_days,
    0 as birth_weight_kg,
    'UNSPECIFIED' as preferred_language,
    'UNSPECIFIED' as preferred_name,
    null as race,
    'UNSPECIFIED' as ethnicity,
    null as race_ethnicity,
    null as interpreter_needed,
    0 as texting_opt_in_ind,
    0 as deceased_ind,
    null as death_date,
    null as record_state,
    1 as current_record_ind,
    'UNSPECIFIED' as create_source,
    current_timestamp as create_date,
    'UNSPECIFIED' as update_source,
    current_timestamp as update_date

union all

select
    -2 as patient_key,
    'NOT APPLICABLE' as patient_name,
    'NOT APPLICABLE' as patient_first_name,
    'NOT APPLICABLE' as patient_last_name,
    'NOT APPLICABLE' as patient_middle_name,
    'NOT APPLICABLE' as mrn,
    '-2' as pat_id,
    '1900-01-01 00:00:00.000' as dob,
    'U' as sex,
    null AS gender_identity,
    null AS sex_assigned_at_birth,
    round(months_between(current_date, dob) / 12, 2) as current_age,
    'NOT APPLICABLE' as email_address,
    'NOT APPLICABLE' as home_phone,
    'NOT APPLICABLE' as mailing_address_line1,
    'NOT APPLICABLE' as mailing_address_line2,
    'NOT APPLICABLE' as mailing_city,
    'NOT APPLICABLE' as mailing_state,
    'NOT APPLICABLE' as mailing_zip,
    'NOT APPLICABLE' as county,
    'NOT APPLICABLE' as country,
    null as gestational_age_complete_weeks,
    null as gestational_age_remainder_days,
    0 as birth_weight_kg,
    'NOT APPLICABLE' as preferred_language,
    'NOT APPLICABLE' as preferred_name,
    null as race,
    'NOT APPLICABLE' as ethnicity,
    null as race_ethnicity,
    null as interpreter_needed,
    0 as texting_opt_in_ind,
    0 as deceased_ind,
    null as death_date,
    null as record_state,
    1 as current_record_ind,
    'NOT APPLICABLE' as create_source,
    current_timestamp as create_date,
    'NOT APPLICABLE' as update_source,
    current_timestamp as update_date