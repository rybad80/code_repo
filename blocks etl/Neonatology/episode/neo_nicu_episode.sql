with adt_rows as (
    select
        visit_key,
        pat_key,
        hospital_admit_date,
        enter_date,
        coalesce(exit_date, date('2050-01-01')) as tmp_exit_date,
        department_group_name,
        case
            when lag(department_group_name) over (
                partition by visit_key
                order by enter_date
                ) = department_group_name
                then 1
            else 0
        end as dupe_dept,
        /* indicate PHL ICU bed care group/level of care*/
        case
            when lower(bed_care_group) = 'phl icu'
                then 1
            else 0
        end as phl_icu_ind
    from
        {{ ref('adt_department_group') }}
    where
        lower(department_group_name) != 'not main census'
        and lower(bed_care_group) != 'periop'
),

nicu_starts as (
    select
        visit_key,
        pat_key,
        hospital_admit_date,
        enter_date,
        tmp_exit_date,
        lead(enter_date) over (partition by visit_key order by enter_date) as next_nicu_episode_start,
        row_number() over (partition by visit_key order by enter_date) as nicu_episode_number
    from
        adt_rows
    where
        lower(department_group_name) = 'nicu'
        and dupe_dept = 0
),

next_dept as (
    select
        visit_key,
        enter_date,
        case
            when lower(department_group_name) in ('ormain')
                then lead(department_group_name) over (
                    partition by visit_key
                    order by enter_date
                )
            else department_group_name
        end as next_non_periop_dept
    from
        {{ ref('adt_department_group') }}
),

episodes as (
    select
        nicu_starts.visit_key,
        nicu_starts.pat_key,
        nicu_starts.hospital_admit_date,
        nicu_starts.nicu_episode_number,
        {{ dbt_utils.surrogate_key(['nicu_starts.visit_key', 'nicu_starts.nicu_episode_number'])}} as episode_key,
        nicu_starts.enter_date as episode_start_date,
        case --noqa: PRS,L013
            when coalesce(max(adt_rows.tmp_exit_date), nicu_starts.tmp_exit_date) = date '2050-01-01' then null
            else coalesce(max(adt_rows.tmp_exit_date), nicu_starts.tmp_exit_date)
        end as episode_end_date,
        adt_rows.phl_icu_ind
    from
        nicu_starts
        left join adt_rows
            on adt_rows.visit_key = nicu_starts.visit_key
                and lower(adt_rows.department_group_name) = 'nicu'
                and (
                    adt_rows.tmp_exit_date < nicu_starts.next_nicu_episode_start
                    or nicu_starts.next_nicu_episode_start is null
                )
    group by
        nicu_starts.visit_key,
        nicu_starts.pat_key,
        nicu_starts.hospital_admit_date,
        nicu_starts.nicu_episode_number,
        nicu_starts.enter_date,
        nicu_starts.tmp_exit_date,
        adt_rows.phl_icu_ind
),

admission_source as (
    select
        episodes.episode_key,
        case
            when lower(adt_department_group.department_group_name) in (
                'nicu',
                'sdu',
                'ed'
            ) then adt_department_group.department_group_name
            else 'Other'
        end as hospital_admission_source,
        case when lower(adt_department_group.department_group_name) = 'sdu' then 1 else 0 end as inborn_ind
    from
        episodes as episodes
        inner join {{ ref('adt_department_group') }} as adt_department_group
            on adt_department_group.visit_key = episodes.visit_key
                and adt_department_group.enter_date = episodes.hospital_admit_date
),

stg_episode as (
    select
        episodes.visit_key,
        encounter_inpatient.hospital_admit_date,
        encounter_inpatient.hospital_discharge_date,
        encounter_inpatient.hospital_los_days,
        encounter_inpatient.discharge_disposition,
        episodes.pat_key,
        stg_patient.mrn,
        stg_patient.patient_name,
        stg_patient.dob,
        stg_patient.sex,
        stg_patient.gestational_age_complete_weeks,
        stg_patient.gestational_age_remainder_days,
        floor(stg_patient.birth_weight_kg * 1000) as birth_weight_grams,
        episodes.nicu_episode_number,
        episodes.episode_key,
        episodes.episode_start_date,
        episodes.episode_end_date,
        episodes.phl_icu_ind,
        {{
            dbt_chop_utils.datetime_diff(
                from_date='episodes.episode_start_date',
                to_date='episodes.episode_end_date',
                unit='day',
                result_precision=4
            )
        }} as nicu_los_days,
        admission_source.hospital_admission_source,
        admission_source.inborn_ind,
        case
            when episodes.episode_end_date is null then null
            when episodes.episode_end_date = encounter_inpatient.hospital_discharge_date
                then encounter_inpatient.discharge_disposition
            when lower(next_dept.next_non_periop_dept) in ('cicu', 'picu') then 'ICU'
            when lower(next_dept.next_non_periop_dept) in ('ccu', 'pcu') then 'Step down'
            else 'Floor'
        end as nicu_disposition
    from
        episodes
        inner join {{ ref('stg_patient') }} as stg_patient
            on stg_patient.pat_key = episodes.pat_key
        inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
            on encounter_inpatient.visit_key = episodes.visit_key
        inner join admission_source
            on admission_source.episode_key = episodes.episode_key
        left join next_dept
            on next_dept.visit_key = episodes.visit_key
                and next_dept.enter_date = episodes.episode_end_date
    where
        /* remove moms from the cohort'*/
        {{
           dbt_chop_utils.datetime_diff(
                from_date='stg_patient.dob',
                to_date='coalesce(encounter_inpatient.hospital_discharge_date, current_date)',
                unit='year'
            )
        }} < 10
        /* but keep patients who didn't get a dob entered (potential trauma patient) */
        or stg_patient.dob = '1901-01-01'
),

episode_treatment_team as (
    select
        stg_episode.episode_key,
        stg_neo_nicu_treatment_team_dates.treatment_team,
        case
            when coalesce(
                stg_neo_nicu_treatment_team_dates.start_date,
                stg_episode.episode_start_date
            ) <= stg_episode.episode_start_date then stg_episode.episode_start_date
            else stg_neo_nicu_treatment_team_dates.start_date
        end as treatment_team_start_date,
        case
            when coalesce(
                stg_neo_nicu_treatment_team_dates.end_date,
                stg_episode.episode_end_date
            ) >= stg_episode.episode_end_date then stg_episode.episode_end_date
            else stg_neo_nicu_treatment_team_dates.end_date
        end as treatment_team_end_date,
        row_number() over (
            partition by
                stg_episode.episode_key
            order by
                stg_neo_nicu_treatment_team_dates.start_date,
                stg_neo_nicu_treatment_team_dates.treatment_team
        ) as treatment_team_number,
        row_number() over (
            partition by
                stg_episode.episode_key
            order by
                stg_neo_nicu_treatment_team_dates.start_date desc,
                stg_neo_nicu_treatment_team_dates.treatment_team
        ) as treatment_team_number_desc
    from
        {{ ref('stg_neo_nicu_treatment_team_dates') }} as stg_neo_nicu_treatment_team_dates
        inner join stg_episode
            on stg_episode.visit_key = stg_neo_nicu_treatment_team_dates.visit_key
    where
        treatment_team_start_date between
        stg_episode.episode_start_date
        and coalesce(stg_episode.episode_end_date, current_date)
),

episode_start_and_end_treament_team as (
    select
        episode_key,
        min(case when treatment_team_number = 1 then treatment_team end) as episode_start_treatment_team,
        min(case when treatment_team_number_desc = 1 then treatment_team end) as episode_end_treatment_team
    from
        episode_treatment_team
    group by
        episode_key
),

itcu_transfers as (
    select
        stg_episode.episode_key,
        1 as itcu_transfer_ind
    from
        stg_episode
        inner join {{ ref('adt_department_group') }} as adt_department_group
            on stg_episode.visit_key = adt_department_group.visit_key
    where
        lower(adt_department_group.department_group_name) = 'itcu'
        and stg_episode.episode_end_date = adt_department_group.enter_date
)

select
    stg_episode.episode_key,
    stg_episode.patient_name,
    stg_episode.mrn,
    stg_episode.dob,
    stg_episode.sex,
    stg_episode.gestational_age_complete_weeks,
    stg_episode.gestational_age_remainder_days,
    stg_episode.birth_weight_grams,
    stg_episode.hospital_admit_date,
    stg_episode.hospital_discharge_date,
    stg_episode.episode_start_date,
    stg_episode.episode_end_date,
    stg_episode.nicu_disposition,
    stg_episode.nicu_los_days,
    stg_episode.nicu_episode_number,
    stg_episode.phl_icu_ind,
    episode_start_and_end_treament_team.episode_start_treatment_team,
    episode_start_and_end_treament_team.episode_end_treatment_team,
    stg_episode.hospital_los_days,
    stg_episode.hospital_admission_source,
    stg_episode.inborn_ind,
    stg_episode.discharge_disposition,
    coalesce(itcu_transfers.itcu_transfer_ind, 0) as itcu_transfer_ind,
    stg_episode.visit_key,
    stg_episode.pat_key
from
    stg_episode
    left join episode_start_and_end_treament_team
        on episode_start_and_end_treament_team.episode_key = stg_episode.episode_key
    left join itcu_transfers
        on itcu_transfers.episode_key = stg_episode.episode_key
