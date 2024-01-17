{{ config(meta = {
    'critical': true
}) }}

with provider_record_source as (
        select
        stg_encounter.csn,
        visit_treatment.visit_key,
        stg_encounter.encounter_key,
        visit_treatment.prov_start_dt as provider_care_team_start_date,
        case when visit.hosp_dischrg_dt
        < coalesce(visit_treatment.prov_end_dt, current_date)
            then visit.hosp_dischrg_dt
            else visit_treatment.prov_end_dt
        end as provider_care_team_end_date,
        provider.full_nm as provider_care_team_name,
        lookup_provider_care_team.provider_care_team_group_name,
        lookup_provider_care_team.provider_care_team_group_category,
        provider.prov_id,
        'provider_record_ser' as source_summary,
        row_number() over (partition by
            stg_encounter.csn,
            visit_treatment.prov_start_dt,
            provider.prov_id order by visit_treatment.prov_end_dt desc, seq_num
        ) as line_for_duplication
    from
        {{source('cdw', 'visit_treatment')}} as visit_treatment
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = visit_treatment.visit_key
        inner join {{source('cdw', 'visit')}} as visit
            on visit.visit_key = stg_encounter.visit_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_treat_rel
            on dict_treat_rel.dict_key = visit_treatment.dict_treat_rel_key
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = visit_treatment.prov_key
        left join {{ref('lookup_provider_care_team')}} as lookup_provider_care_team
            on lookup_provider_care_team.provider_id = provider.prov_id
    where
        (dict_treat_rel.src_id = 4 --team
        --not all teams are tagged as teams. This introduces some false positives
        or provider.prov_type = 'Resource')
        and visit_treatment.prov_start_dt <= '2021-03-22'
),

provider_record_source_no_dups as (
    select
        csn,
        visit_key,
        encounter_key,
        provider_care_team_start_date,
        provider_care_team_end_date,
        provider_care_team_name,
        provider_care_team_group_name,
        provider_care_team_group_category,
        null::bigint as care_team_id,
        prov_id as provider_id,
        source_summary
    from provider_record_source
    where line_for_duplication = 1
),

team_audit as (
    select
        ept_team_audit.pat_enc_csn_id as csn,
        ept_team_audit.line,
        provteam_rec_info.record_name as provider_care_team_name,
        ept_team_audit.team_audit_id,
        ept_team_audit.team_audit_instant,
        zc_team_action.name as team_action_name,
        case
            when lag(zc_team_action.team_action_c) over (
                partition by ept_team_audit.pat_enc_csn_id,
                             ept_team_audit.team_audit_id
                order by ept_team_audit.line) = zc_team_action.team_action_c
                then 1
        end as duplicate_action_ind
    from
        {{source('clarity_ods', 'ept_team_audit')}} as ept_team_audit
        inner join {{source('clarity_ods', 'zc_team_action')}} as zc_team_action
            on zc_team_action.team_action_c = ept_team_audit.team_action_c
        inner join {{source('clarity_ods', 'provteam_rec_info')}} as provteam_rec_info
            on provteam_rec_info.id = ept_team_audit.team_audit_id
    where
        zc_team_action.name in (
            'Add',
            'Remove')
),

team_audit_no_dups as (
    select
        *,
        team_audit_instant as provider_care_team_start_date,
        lead(team_audit_instant) over (
            partition by csn,
                         provider_care_team_name
            order by line)
        as provider_care_team_end_date
    from
        team_audit
    where
        duplicate_action_ind is null
),

provider_care_team_source as (
    select
        team_audit_no_dups.csn,
        stg_encounter.visit_key,
        stg_encounter.encounter_key,
        team_audit_no_dups.provider_care_team_start_date,
        case when visit.hosp_dischrg_dt
                < coalesce(team_audit_no_dups.provider_care_team_end_date, current_date)
                then visit.hosp_dischrg_dt
                else team_audit_no_dups.provider_care_team_end_date
        end as provider_care_team_end_date,
        team_audit_no_dups.provider_care_team_name,
        lookup_provider_care_team.provider_care_team_group_name,
        lookup_provider_care_team.provider_care_team_group_category,
        team_audit_id as care_team_id,
        null::varchar(100) as provider_id,
        'provider_care_team_pct' as source_summary
from
    team_audit_no_dups
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = team_audit_no_dups.csn
    inner join {{source('cdw', 'visit')}} as visit
        on visit.visit_key = stg_encounter.visit_key
    left join {{ref('lookup_provider_care_team')}} as lookup_provider_care_team
        on lookup_provider_care_team.care_team_id = team_audit_no_dups.team_audit_id
where
    team_action_name = 'Add'
)

select *
from provider_record_source_no_dups
union all
select *
from provider_care_team_source
