with pat_over_16_phone_line_confirmed as (
    select
        stg_rpm_patient.pat_id,
        min(case
            when pat_relationships_self.pat_rel_mobile_phne = other_communctn.other_communic_num
                then other_communctn.line
        end) as pat_mobile_confirmed_line
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'patient') }} as patient
            on patient.pat_id = stg_rpm_patient.pat_id
        left join {{ source('clarity_ods', 'pat_relationships') }} as pat_relationships_self
            on pat_relationships_self.pat_id = stg_rpm_patient.pat_id
                -- 1 -self; 14 - self-chop; 24 - patient cell
                and pat_relationships_self.pat_rel_relation_c in (1, 14, 24)
        left join {{ source('clarity_ods', 'other_communctn') }} as other_communctn
            on other_communctn.pat_id = stg_rpm_patient.pat_id
                and other_communctn.other_communic_c = 1
                and other_communctn.other_communic_num is not null
    group by
        stg_rpm_patient.pat_id
),

pat_over_16_phone_line_probable as ( -- will this apply to all >16 patients or just mindsmatter?
    select
        stg_rpm_patient.pat_id,
        -- minimum comm line number where the number doesn't exist
        min(other_communctn.line) as probable_line
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'patient') }} as patient
            on patient.pat_id = stg_rpm_patient.pat_id
        left join {{ source('clarity_ods', 'other_communctn') }} as other_communctn
            on other_communctn.pat_id = stg_rpm_patient.pat_id
                and other_communctn.other_communic_c = 1
                and other_communctn.other_communic_num is not null
    where not exists (
        select
            pat_relationships_not_self.*
        from
            {{ source('clarity_ods', 'pat_relationships') }} as pat_relationships_not_self
        where
            pat_relationships_not_self.pat_id = stg_rpm_patient.pat_id
            and pat_relationships_not_self.pat_rel_relation_c not in (1, 14, 24)
            and (pat_relationships_not_self.pat_rel_home_phone = other_communctn.other_communic_num
                or pat_relationships_not_self.pat_rel_mobile_phne = other_communctn.other_communic_num
                or pat_relationships_not_self.pat_rel_work_phone = other_communctn.other_communic_num)
    )
    group by
        stg_rpm_patient.pat_id
),

pat_over_16_phone as (
    select
        stg_rpm_patient.pat_id,
        case
            when pat_over_16_phone_line_confirmed.pat_mobile_confirmed_line is not null
                then other_communctn_confirmed.other_communic_num
            else null
        end as pat_mobile_confirmed,
        case
            when pat_over_16_phone_line_probable.probable_line is not null
                then other_communctn_probable.other_communic_num
            else null
        end as pat_mobile_probable
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'patient') }} as patient
            on patient.pat_id = stg_rpm_patient.pat_id
        left join pat_over_16_phone_line_confirmed
            on pat_over_16_phone_line_confirmed.pat_id = stg_rpm_patient.pat_id
        left join pat_over_16_phone_line_probable
            on pat_over_16_phone_line_probable.pat_id = stg_rpm_patient.pat_id
        left join {{ source('clarity_ods', 'other_communctn') }} as other_communctn_confirmed
            on other_communctn_confirmed.pat_id = stg_rpm_patient.pat_id
                and other_communctn_confirmed.other_communic_c = 1
                and other_communctn_confirmed.other_communic_num is not null
                and other_communctn_confirmed.line = pat_over_16_phone_line_confirmed.pat_mobile_confirmed_line
        left join {{ source('clarity_ods', 'other_communctn') }} as other_communctn_probable
            on other_communctn_probable.pat_id = stg_rpm_patient.pat_id
                and other_communctn_probable.other_communic_c = 1
                and other_communctn_probable.other_communic_num is not null
                and other_communctn_probable.line = pat_over_16_phone_line_probable.probable_line
),

pat_over_16_email as (
    select
        stg_rpm_patient.pat_id,
        max(case
            when pat_relationships_self.pat_rel_email = patient.email_address
                then 1
            else 0
        end) as pat_email_confirmed_ind,
        max(case
            when pat_relationships_not_self.pat_rel_email = patient.email_address
                then 1
            else 0
        end) as not_the_pat_email_ind,
        case
            when pat_email_confirmed_ind = 1 or not_the_pat_email_ind = 0
                then patient.email_address
        end as pat_email_over_16
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'patient') }} as patient
            on patient.pat_id = stg_rpm_patient.pat_id
        left join {{ source('clarity_ods', 'pat_relationships') }} as pat_relationships_self
            on pat_relationships_self.pat_id = stg_rpm_patient.pat_id
                -- 1 -self; 14 - self-chop; 24 - patient cell
                and pat_relationships_self.pat_rel_relation_c in (1, 14, 24)
        left join {{ source('clarity_ods', 'pat_relationships') }} as pat_relationships_not_self
            on pat_relationships_not_self.pat_id = stg_rpm_patient.pat_id
                and pat_relationships_not_self.pat_rel_relation_c not in (1, 14, 24)
    group by
        stg_rpm_patient.pat_id,
        patient.email_address
)

select
    stg_rpm_patient.pat_id,
    replace(pat_over_16_phone.pat_mobile_confirmed, '-', '') as patient_over_16_phone_confirmed,
    replace(pat_over_16_phone.pat_mobile_probable, '-', '') as patient_over_16_phone_probable,
    pat_over_16_email.pat_email_over_16 as patient_over_16_email
from
    {{ref('stg_rpm_patient') }} as stg_rpm_patient
    left join pat_over_16_phone
        on pat_over_16_phone.pat_id = stg_rpm_patient.pat_id
    left join pat_over_16_email
        on pat_over_16_email.pat_id = stg_rpm_patient.pat_id
