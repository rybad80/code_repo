with xwalks as (
    select 
        pat_enc_csn_id::varchar(50) as encounter_id, 
        '' as patient_id, 
        'clarity' as source_name
    from 
        {{source('clarity_ods','pat_enc')}} as pat_enc
    union all
    select 
        pat_enc_csn_id::varchar(50) as encounter_id, 
        pat_id::varchar(50) as patient_id,
        'clarity' as source_name
    from 
        {{source('clarity_ods','pat_enc')}} as pat_enc
),
/* This avoids clashes with legacy visit keys */
max_visit as (
    select max(legacy_visit_key) + 1 as max_visit_key
    from {{source('manual_ods', 'xwalk_visit_key_cdw_to_dbt')}}
),
id_plus_raw as (
    select
        abs({{
            dbt_utils.surrogate_key([
                'patient_id',
                'encounter_id',
                'source_name'
            ])
        }}) + max_visit.max_visit_key as visit_key,
        encounter_id,
        patient_id,
        source_name        
    from xwalks
    inner join max_visit on 1 = 1
),
id_plus_legacy as (
    select 
        coalesce(legacy_xwalk.legacy_visit_key, id_plus_raw.visit_key) as visit_key,
        id_plus_raw.encounter_id,
        id_plus_raw.patient_id,
        id_plus_raw.source_name,
        row_number() over (partition by id_plus_raw.visit_key order by id_plus_raw.encounter_id) as dupe_ctr        
    from 
        id_plus_raw
        left join {{source('manual_ods', 'xwalk_visit_key_cdw_to_dbt')}} as legacy_xwalk
            on id_plus_raw.encounter_id = legacy_xwalk.encounter_id
            and (id_plus_raw.patient_id = legacy_xwalk.patient_id or id_plus_raw.source_name = 'clarity')
            and id_plus_raw.source_name = legacy_xwalk.source_name
),
all_ids as (
    select distinct visit_key 
    from id_plus_legacy
)
select 
    case 
        when dupe_ctr = 1 then visit_key
        /* add to hash to avoid a clashing id */
        when dupe_ctr > 1 and (visit_key + dupe_ctr) not in (select visit_key from all_ids)
            then (visit_key + dupe_ctr)
        when dupe_ctr > 1 and (visit_key + dupe_ctr + 1) not in (select visit_key from all_ids)
            then (visit_key + dupe_ctr + 1)
        else (visit_key + dupe_ctr + 2) 
    end as visit_key,
    encounter_id,
    patient_id,
    source_name    
from 
    id_plus_legacy



