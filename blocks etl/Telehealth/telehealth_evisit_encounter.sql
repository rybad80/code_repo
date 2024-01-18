with evisit as (
    select
        encounter_all.visit_key,
        encounter_all.pat_key,
        encounter_all.encounter_date,
        visit.appt_entry_dt as evisit_open_date,
        visit.enc_close_dt as evisit_close_date,
        max(
            case when lower(in_basket_message.msg_desc) like '%new%myderm%' then 1 else 0 end
        ) as new_myderm_ind,
        max(
            case when lower(in_basket_message.msg_desc) like '%except acne%' then 1 else 0 end
        ) as followup_except_acne_ind,
        max(
            case when lower(in_basket_message.msg_desc) like '%for acne%' then 1 else 0 end
        ) as followup_for_acne_ind
    from
        {{ref('encounter_all') }} as encounter_all
        inner join {{ source('cdw', 'visit') }} as visit
            on visit.visit_key = encounter_all.visit_key
        inner join {{ source('cdw', 'fact_transaction') }} as fact_transaction
            on encounter_all.visit_key = fact_transaction.visit_key
        left join {{ source('cdw', 'in_basket_message') }} as in_basket_message
            on in_basket_message.visit_key = encounter_all.visit_key
            and (
                lower(in_basket_message.msg_desc) like '%new%myderm%'
                or lower(in_basket_message.msg_desc) like '%follow%up%'
            )
    where
        encounter_all.visit_type_id = '2817' -- 'e visit'
        and lower(fact_transaction.cpt_cd) != 'nochg'
        and fact_transaction.cpt_cd is not null
    group by
        encounter_all.visit_key,
        encounter_all.pat_key,
        encounter_all.encounter_date,
        visit.appt_entry_dt,
        visit.enc_close_dt
),

drive_distance as (
    select
        patient_department_drive_stats.pat_key,
        patient_department_drive_stats.tot_drive_distance_miles * 2 as round_trip_distance_miles,
        patient_department_drive_stats.tot_drive_time_minutes * 2 as round_trip_time_minutes
    from
        evisit
        inner join {{ source('cdw','patient_department_drive_stats') }} as patient_department_drive_stats
            on patient_department_drive_stats.pat_key = evisit.pat_key
        inner join {{ source('cdw','department') }} as department
            on patient_department_drive_stats.dept_key = department.dept_key
    where
        department.dept_id = 101012140 --bgr dermatology
    group by
        patient_department_drive_stats.pat_key,
        patient_department_drive_stats.tot_drive_distance_miles,
        patient_department_drive_stats.tot_drive_time_minutes
),

pcp_provider as (
        select
            evisit.pat_key,
            seq_num,
            pcp_prov_key,
            eff_dt as start_date,
            coalesce(
                    lead(eff_dt) over(partition by evisit.pat_key order by eff_dt) - 1,
                    '2099-12-31'
                ) as end_date,
            provider.full_nm as primary_care_provider_name,
            dim_provider_practice.prov_practice_nm as primary_care_practice_name
        from
            evisit
            inner join {{ source('cdw', 'patient_primary_care_provider') }} as patient_primary_care_provider
                on patient_primary_care_provider.pat_key = evisit.pat_key
            left join {{ source('cdw', 'provider') }} as provider
                on provider.prov_key = patient_primary_care_provider.pcp_prov_key
            left join {{ source('cdw', 'dim_provider_practice') }} as dim_provider_practice
                on dim_provider_practice.dim_prov_practice_key = provider.dim_prov_practice_key
        where
            patient_primary_care_provider.pat_pcp_deleted_ind = 0
            -- exclude deleted records, some are merged, others not sure
        group by
            evisit.pat_key,
            seq_num,
            pcp_prov_key,
            eff_dt,
            provider.full_nm,
            dim_provider_practice.prov_practice_nm
),

last_office_visit as (
    select
        evisit.visit_key,
        row_number() over(
            partition by evisit.visit_key order by encounter_office_visit_all.encounter_date desc
        ) as visit_seq_num,
        encounter_office_visit_all.encounter_date,
        encounter_office_visit_all.appointment_status
    from
        {{ref('encounter_office_visit_all') }} as encounter_office_visit_all
        inner join evisit on evisit.pat_key = encounter_office_visit_all.pat_key
    where
        encounter_office_visit_all.encounter_date < evisit.encounter_date
        and lower(encounter_office_visit_all.specialty) = 'dermatology'
        and encounter_office_visit_all.encounter_type_id in ('101', '50')
),

next_office_visit as (
select
    evisit.visit_key,
    row_number() over(
        partition by evisit.visit_key order by encounter_office_visit_all.encounter_date
    ) as visit_seq_num,
    encounter_office_visit_all.encounter_date,
    encounter_office_visit_all.appointment_status
from
    {{ref('encounter_office_visit_all') }} as encounter_office_visit_all
    inner join evisit on evisit.pat_key = encounter_office_visit_all.pat_key
where
    encounter_office_visit_all.encounter_date > evisit.encounter_date
    and lower(encounter_office_visit_all.specialty) = 'dermatology'
    and encounter_office_visit_all.encounter_type_id in ('101', '50')
),

business_dates as (
    select
        date_key,
        full_date
    from
        {{ref('stg_dim_date') }}
    where
        weekday_ind = 1
        and holiday_all_employees_ind = 0
),

business_hours as (
    select
        evisit.visit_key,
        max(
            8.0, hour(evisit.evisit_open_date) + minute(evisit.evisit_open_date) / 60.0
        ) as hour_start, -- earliest after 0800
        min(
            17.0, hour(evisit.evisit_close_date) + minute(evisit.evisit_close_date) / 60.0
        ) as hour_end, --latest up to 1700
        min(business_dates.full_date) as first_business_day,
        max(business_dates.full_date) as last_business_day,
        count(distinct business_dates.full_date) as n_business_days,
        -- number of days excluding start and end
        count(distinct
                case
                    when business_dates.full_date > date(evisit.evisit_open_date)
                        and business_dates.full_date < date(evisit.evisit_close_date)
                then business_dates.full_date
                end
            ) as n_full_days,
        -- # of business hours it took to respond
        case --noqa: PRS
            -- 0 when no busines days at all
            when n_business_days = 0 then 0
            -- when same day within business hours
            when date(evisit.evisit_open_date) = date(evisit.evisit_close_date)
                and hour_start between 8 and 17
                and hour_end between 8 and 17
                then hour_end - hour_start
            else
                -- 8 hours for each business day in-between
                (n_full_days * 8)
                -- start hour through 1700 for start day
                + case when first_business_day = date(evisit_open_date)
                    and hour_start between 8 and 17 then 17 - hour_start else 0 end
                -- 800 through end hour for end day
                + case when last_business_day = date(evisit_close_date)
                    and hour_end between 8 and 17 then hour_end - 8 else 0 end
            end as buisness_hours
    from
        evisit
        left join business_dates
            on business_dates.full_date >= date(evisit.evisit_open_date)
            and business_dates.full_date <= date(evisit.evisit_close_date)
    group by
        evisit.visit_key,
        evisit.evisit_open_date,
        evisit.evisit_close_date
)

select
	evisit.visit_key,
    encounter_all.mrn,
    encounter_all.patient_name,
    encounter_all.dob,
    encounter_all.age_years,
    encounter_all.sex,
    stg_patient.race_ethnicity,
    encounter_all.csn,
    encounter_all.provider_name,
    encounter_all.payor_group,
    encounter_all.payor_name,
    evisit.encounter_date,
    encounter_all.department_name,
    encounter_all.visit_type,
    encounter_all.visit_type_id,
    evisit.new_myderm_ind,
    evisit.followup_except_acne_ind,
    evisit.followup_for_acne_ind,
    case
		when evisit.new_myderm_ind = 1 then 'New MyDerm E-Visit'
		when evisit.followup_except_acne_ind = 1 then 'Follow-up after in-person CHOP Dermatology visit (except Acne)'
		when evisit.followup_for_acne_ind = 1 then 'Follow-up after in-person CHOP Dermatology visit for Acne'
		else 'Other'
		end as reason_for_visit,
    last_office_visit.encounter_date as last_appointment_date,
    last_office_visit.appointment_status as last_appointment_status,
    next_office_visit.encounter_date as next_appointment_date,
    next_office_visit.appointment_status as next_appointment_status,
    evisit.evisit_open_date,
    evisit.evisit_close_date,
    business_hours.buisness_hours,
    floor(
        case when business_hours.hour_start > 17.0 then 8.0 else business_hours.hour_start end
    ) as telederm_hour_open, -- opens at 8 the next day if later
    floor(
        case when business_hours.hour_end < 8.0 then 8.0 else business_hours.hour_end end
    ) as telederm_hour_close, -- closes at 8 if earlier
    business_hours.first_business_day,
    drive_distance.round_trip_distance_miles,
    drive_distance.round_trip_time_minutes,
    patient_address_hist.city,
    patient_address_hist.state,
    encounter_all.patient_address_zip_code,
    pcp_provider.primary_care_provider_name,
    pcp_provider.primary_care_practice_name,
    stg_patient.preferred_language,
    evisit.pat_key,
    encounter_all.prov_key
from
    evisit
    inner join {{ref('encounter_all')}} as encounter_all on encounter_all.visit_key = evisit.visit_key
    inner join {{ref('stg_patient')}} as stg_patient on stg_patient.pat_key = evisit.pat_key
    left join {{source('cdw', 'patient_address_hist')}} as  patient_address_hist
        on patient_address_hist.pat_key = evisit.pat_key
        and encounter_all.patient_address_seq_num = patient_address_hist.seq_num
    -- previous/future visits from evist                                                
    left join last_office_visit
        on last_office_visit.visit_key = evisit.visit_key and last_office_visit.visit_seq_num = 1
    left join next_office_visit
        on next_office_visit.visit_key = evisit.visit_key and next_office_visit.visit_seq_num = 1
    -- business hours
    left join business_hours on business_hours.visit_key = evisit.visit_key
    -- driving
    left join drive_distance  on drive_distance.pat_key = evisit.pat_key
    -- primary care
    left join pcp_provider
        on pcp_provider.pat_key = evisit.pat_key
        and evisit.encounter_date between pcp_provider.start_date and pcp_provider.end_date
where
    true
