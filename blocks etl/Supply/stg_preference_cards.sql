with base_procedure_list as (
    select
        or_proc.or_proc_id,
        or_proc.proc_name,
        or_proc.picklist_id,
        or_proc.rec_typ_c,
        --convert an inactive flag to an active flag  
        case or_proc.inactive_yn when 'N' then 1 else 0 end as active_ind
    from
        {{source('clarity_ods', 'or_proc')}} as or_proc
),
procedure_authorized_location as (--remove duplicates which are listed in EPIC 
select distinct
    or_proc_authloc.or_proc_id,
    or_proc_authloc.auth_locations_id
    from
         {{source('clarity_ods', 'or_proc_authloc')}} as or_proc_authloc
),
--each default procedure is written in the first table, and then all 
   --the modified preference cards (tied to the default procedure) is the second table
preference_card_list as (--(default procedure) union all (modified preference cards tied to that base procedure) 
select
        base_procedure_list.or_proc_id as or_proc_id,
        base_procedure_list.proc_name as or_procedure_name,
        base_procedure_list.active_ind as or_procedure_active_ind,
        base_procedure_list.or_proc_id as preference_card_id,
        base_procedure_list.proc_name as preference_card_name,
        base_procedure_list.active_ind as preference_card_active_ind,
        'default' as default_or_modified_preference_card,
        base_procedure_list.picklist_id as picklist_id,
        procedure_authorized_location.auth_locations_id as location_id,
        or_pklst.surgeon_id as provider_id
    from
        base_procedure_list
    --picklist contains the supplies and instruments ids
    left join
        {{source('clarity_ods', 'or_pklst')}} as or_pklst on
            or_pklst.pick_list_id = base_procedure_list.picklist_id
    --base procedure activated location id 
    left join
        procedure_authorized_location on
            procedure_authorized_location.or_proc_id = base_procedure_list.or_proc_id
    where
        base_procedure_list.rec_typ_c = 2 --base procedures only
    union all
    select
        base_procedure_list.or_proc_id as or_proc_id,
        base_procedure_list.proc_name as or_procedure_name,
        base_procedure_list.active_ind as or_procedure_active_ind,
        or_proc_mod_orp_index.or_proc_id as preference_card_id,
        mod_proc.proc_name as preference_card_name,
        mod_proc.active_ind as preference_card_active_ind,
        'modified' as default_or_modified_preference_card,
        mod_proc.picklist_id as picklist_id,
        or_proc_mod_eaf_index.pmods_eaf_index_id as location_id,
        or_pklst.surgeon_id as provider_id
    from
        base_procedure_list
    --ties the base procedure id to the child preference card id 
    left join
        {{source('clarity_ods', 'or_proc_mod_orp_index')}} as or_proc_mod_orp_index on
            or_proc_mod_orp_index.pmods_orp_index_id = base_procedure_list.or_proc_id
    --preference card names 
    left join
        base_procedure_list as mod_proc on
            mod_proc.or_proc_id = or_proc_mod_orp_index.or_proc_id
    --picklist contains the supplies and instrument ids
    left join
        {{source('clarity_ods', 'or_pklst')}} as or_pklst on or_pklst.pick_list_id = mod_proc.picklist_id
    --preference card activated location id  
    left join
        {{source('clarity_ods', 'or_proc_mod_eaf_index')}} as or_proc_mod_eaf_index on
            or_proc_mod_eaf_index.or_proc_id = mod_proc.or_proc_id
    where
        base_procedure_list.rec_typ_c = 2 --base procedures only 
),

cards_reviewed as (--last date a preference card was reviewed 
select
        or_proc_audit_trl.proc_id,
        max(cast(or_proc_audit_trl.audit_date as date)) as last_reviewed_date
    from
        {{source('clarity_ods', 'or_proc_audit_trl')}} as or_proc_audit_trl
    where
        or_proc_audit_trl.audit_action_c = 3 --preference card reviewed action 
    group by
        or_proc_audit_trl.proc_id
)
select
  {{
        dbt_utils.surrogate_key([
            'preference_card_list.or_proc_id',
            'preference_card_list.preference_card_id',
            'preference_card_list.location_id'
        ])
    }} as preference_card_key,
    preference_card_list.or_procedure_name,
    preference_card_list.or_proc_id,
    preference_card_list.or_procedure_active_ind,
    preference_card_list.preference_card_name,
    preference_card_list.preference_card_id,
    preference_card_list.preference_card_active_ind,
    preference_card_list.default_or_modified_preference_card,
    preference_card_list.picklist_id,
    clarity_ser.prov_name as provider_name,
    preference_card_list.provider_id,
    clarity_loc.loc_name as location_name,
    preference_card_list.location_id,
    cards_reviewed.last_reviewed_date
from
    preference_card_list
--location names of the location id 
left join
    {{source('clarity_ods', 'clarity_loc')}} as clarity_loc on
        clarity_loc.loc_id = preference_card_list.location_id
left join
    {{source('clarity_ods', 'clarity_ser')}} as clarity_ser on
        clarity_ser.prov_id = preference_card_list.provider_id
left join
    cards_reviewed on
        cards_reviewed.proc_id = preference_card_list.preference_card_id
        