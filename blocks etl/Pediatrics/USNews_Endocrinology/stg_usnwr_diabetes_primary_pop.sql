with most_recent_endo_visits as ( --added FOR FY22 chart review, sorted BY provider (NP)
    select
        encounter_specialty_care.patient_name,
        encounter_specialty_care.patient_key,
        encounter_specialty_care.mrn,
        encounter_specialty_care.dob,
        encounter_specialty_care.encounter_key,
        encounter_specialty_care.encounter_date,
        row_number() over (
            partition by
                encounter_specialty_care.patient_key
            order by
                encounter_specialty_care.encounter_date desc
        ) as most_recent_endo_visit
    from
        {{ref('encounter_specialty_care') }} as encounter_specialty_care
    where
        encounter_specialty_care.specialty_name in ('ENDOCRINOLOGY')
        and encounter_specialty_care.encounter_date
            between current_date - interval('1 year') and current_date
        and encounter_specialty_care.age_years < 18
),

encounters_num as (
    select
        stg_usnwr_diabetes_type_cohort.patient_key,
        count(distinct case
            when diabetes_visit_cohort.visit_type in (
            'NEW DIABETES TYPE 1 TRANSFER',
            'NEW DIABETES PATIENT',
            'NEW DIABETES TYPE 2 TRANSFER',
            'FOLLOW UP DIABETES',
            'DIABETES T1Y1 FOLLOW UP',
            'VIDEO VISIT DIABETES'
            )
                and lower(diabetes_visit_cohort.enc_type) = 'office visit'
            then diabetes_visit_cohort.encounter_key
        end) as pat_visits,
        max(case
            when most_recent_endo_visits.most_recent_endo_visit = 1
                then diabetes_visit_cohort.provider_nm
            else null
        end) as most_recent_provider,
        max(case
            when most_recent_endo_visits.most_recent_endo_visit = 1
                then diabetes_visit_cohort.prov_type
            else null
        end) as most_recent_provider_title,
        max(diabetes_visit_cohort.endo_vis_dt) as visit_endo_date,
        max(case
            when most_recent_endo_visits.most_recent_endo_visit = 1
                then most_recent_endo_visits.encounter_date
        end) as most_recent_endo_encounter,
        max(case
            when most_recent_endo_visits.most_recent_endo_visit = 1
                and month(most_recent_endo_visits.encounter_date) in (10, 11, 12)
            then 1 else 0
        end) as seen_flu_month
    from
        {{ref('stg_usnwr_diabetes_type_cohort')}} as stg_usnwr_diabetes_type_cohort
        inner join {{ ref('diabetes_visit_cohort') }} as diabetes_visit_cohort
            on stg_usnwr_diabetes_type_cohort.patient_key = diabetes_visit_cohort.patient_key
                and (diabetes_visit_cohort.endo_vis_dt
                    between stg_usnwr_diabetes_type_cohort.start_date and stg_usnwr_diabetes_type_cohort.end_date)
        inner join most_recent_endo_visits
            on most_recent_endo_visits.encounter_key = diabetes_visit_cohort.encounter_key
    where
        stg_usnwr_diabetes_type_cohort.duration_year > '1'
        and lower(diabetes_visit_cohort.prov_type) in ('nurse practitioner', 'physician', 'fellow')
    group by
        stg_usnwr_diabetes_type_cohort.patient_key
)

select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    stg_usnwr_diabetes_type_cohort.patient_key as primary_key,
    encounter_all.encounter_date as metric_date,
    stg_usnwr_diabetes_type_cohort.new_ip_diabetes_date,
    stg_usnwr_diabetes_type_cohort.first_outpatient_date,
    stg_usnwr_diabetes_type_cohort.ip_diagnosis_ind,
    stg_usnwr_diabetes_type_cohort.new_transfer_ind,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.metric_id,
    /*used for validation*/
    stg_usnwr_diabetes_type_cohort.diabetes_usnwr_year as submission_year,
    stg_usnwr_diabetes_type_cohort.patient_name,
    stg_usnwr_diabetes_type_cohort.mrn,
    stg_usnwr_diabetes_type_cohort.dob,
    stg_usnwr_diabetes_type_cohort.current_age,
    coalesce(stg_usnwr_diabetes_type_cohort.payor_group, encounter_all.payor_group) as payer_group,
    case
        when payer_group is null then null
        when lower(payer_group) = 'commercial' then 'private'
        else 'non-private'
    end as insurance_status,
    stg_usnwr_diabetes_type_cohort.diabetes_type_12,
    stg_usnwr_diabetes_type_cohort.duration_year as dx_duration_year,
    stg_usnwr_diabetes_type_cohort.first_dx_date as date_of_diagnosis,
    encounters_num.pat_visits,
    coalesce(encounters_num.most_recent_provider,
        encounter_all.provider_name) as most_recent_provider,
    coalesce(encounters_num.most_recent_provider_title,
        encounter_all.provider_type) as most_recent_provider_title,
    coalesce(encounters_num.most_recent_endo_encounter,
        encounters_num.visit_endo_date) as most_recent_endo_encounter,
    encounters_num.seen_flu_month,
    coalesce(stg_usnwr_diabetes_admissions.ip_admissions_ind, 0) as diabetes_admissions_ind,
    coalesce(stg_usnwr_diabetes_admissions.diabetes_ed_urgent_ind, 0) as diabetes_ed_urgent_ind,
    stg_usnwr_diabetes_admissions.prov_report_ip_ind,
    stg_usnwr_diabetes_admissions.prov_report_ed_ind,
    encounter_all.encounter_date as index_date,
    case
        when (insurance_status = 'private'
            and (stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 1'
                and usnews_metadata_calendar.metric_id = 'c29.2a'
                and stg_usnwr_diabetes_admissions.ip_admissions_ind = '1')
            or (stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 2'
                and usnews_metadata_calendar.metric_id = 'c29.2c'
                and stg_usnwr_diabetes_admissions.ip_admissions_ind = '1')
            or (stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 1'
                and usnews_metadata_calendar.metric_id = 'c29.2e'
                and stg_usnwr_diabetes_admissions.diabetes_ed_urgent_ind = '1')
            or (stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 2'
                and usnews_metadata_calendar.metric_id = 'c29.2g'
                and stg_usnwr_diabetes_admissions.diabetes_ed_urgent_ind = '1')
        ) then stg_usnwr_diabetes_type_cohort.patient_key
        when (insurance_status = 'non-private'
            and (stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 1'
                and usnews_metadata_calendar.metric_id = 'c29.2b'
                and stg_usnwr_diabetes_admissions.ip_admissions_ind = '1')
            or (stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 2'
                and usnews_metadata_calendar.metric_id = 'c29.2d'
                and stg_usnwr_diabetes_admissions.ip_admissions_ind = '1')
            or (stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 1'
                and usnews_metadata_calendar.metric_id = 'c29.2f'
                and stg_usnwr_diabetes_admissions.diabetes_ed_urgent_ind = '1')
            or (stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 2'
                and usnews_metadata_calendar.metric_id = 'c29.2h'
                and stg_usnwr_diabetes_admissions.diabetes_ed_urgent_ind = '1')
        ) then stg_usnwr_diabetes_type_cohort.patient_key
        else stg_usnwr_diabetes_type_cohort.patient_key
    end as num,
    case
        when usnews_metadata_calendar.metric_id in ('c29.2a', 'c29.2b', 'c29.2c', 'c29.2d', 'c29.2e', 'c29.2f')
        then stg_usnwr_diabetes_type_cohort.patient_key
    end as denom,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.division,
    stg_usnwr_diabetes_type_cohort.start_date,
    stg_usnwr_diabetes_type_cohort.end_date,
    stg_usnwr_diabetes_type_cohort.pat_key,
    stg_usnwr_diabetes_type_cohort.encounter_key
from
    {{ref('stg_usnwr_diabetes_type_cohort')}} as stg_usnwr_diabetes_type_cohort
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on lower(usnews_metadata_calendar.question_number) like 'c29%'
            and stg_usnwr_diabetes_type_cohort.transfer_exclu_ind = '0'
    inner join encounters_num
        on stg_usnwr_diabetes_type_cohort.patient_key = encounters_num.patient_key
    inner join {{ ref('encounter_all') }} as encounter_all
        on stg_usnwr_diabetes_type_cohort.encounter_key = encounter_all.encounter_key
            and lower(encounter_all.provider_type) in ('nurse practitioner', 'physician', 'fellow')
    left join {{ref('stg_usnwr_diabetes_admissions')}} as stg_usnwr_diabetes_admissions
        on stg_usnwr_diabetes_type_cohort.patient_key = stg_usnwr_diabetes_admissions.patient_key
where
    stg_usnwr_diabetes_type_cohort.current_age < '19'
    and encounters_num.pat_visits >= '2'
    and ((insurance_status = 'private'
        and stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 1'
        and (usnews_metadata_calendar.metric_id = 'c29.2a1'
            or (stg_usnwr_diabetes_admissions.ip_admissions_ind = '1'
                and usnews_metadata_calendar.metric_id = 'c29.2c1')
            or (stg_usnwr_diabetes_admissions.diabetes_ed_urgent_ind = '1'
                and usnews_metadata_calendar.metric_id = 'c29.2e1')
            ))
    or (insurance_status = 'non-private'
        and stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 1'
        and (usnews_metadata_calendar.metric_id = 'c29.2a2'
            or (stg_usnwr_diabetes_admissions.ip_admissions_ind = '1'
                and usnews_metadata_calendar.metric_id = 'c29.2c2')
            or (stg_usnwr_diabetes_admissions.diabetes_ed_urgent_ind = '1'
                and usnews_metadata_calendar.metric_id = 'c29.2e2')
            ))
    or (insurance_status = 'private'
        and stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 2'
        and (usnews_metadata_calendar.metric_id = 'c29.2b1'
            or (stg_usnwr_diabetes_admissions.ip_admissions_ind = '1'
                and usnews_metadata_calendar.metric_id = 'c29.2d1')
            or (stg_usnwr_diabetes_admissions.diabetes_ed_urgent_ind = '1'
                and usnews_metadata_calendar.metric_id = 'c29.2f1')
        ))
    or (insurance_status = 'non-private'
        and stg_usnwr_diabetes_type_cohort.diabetes_type_12 = 'Type 2'
        and (usnews_metadata_calendar.metric_id = 'c29.2b2'
            or (stg_usnwr_diabetes_admissions.ip_admissions_ind = '1'
                and usnews_metadata_calendar.metric_id = 'c29.2d2')
            or (stg_usnwr_diabetes_admissions.diabetes_ed_urgent_ind = '1'
                and usnews_metadata_calendar.metric_id = 'c29.2f2')
        ))
    )
