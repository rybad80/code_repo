with stage as (
    select
        stg_vaccination_unioned.pat_id,
        stg_vaccination_unioned.received_date,
        stg_vaccination_unioned.documented_date,
        stg_vaccination_unioned.grouper_records_numeric_id,
        case
            when max(stg_vaccination_unioned.dxr_ind) = 1 and max(stg_vaccination_unioned.lpl_ind) = 1
            then 'LPL and DXR record'
            when max(stg_vaccination_unioned.dxr_ind) = 1 and max(stg_vaccination_unioned.lpl_ind) = 0
            then 'DXR record only'
            when max(stg_vaccination_unioned.dxr_ind) = 0 and max(stg_vaccination_unioned.lpl_ind) = 1
            then 'LPL record only'
            when max(stg_vaccination_unioned.sde_ind) = 1
                or max(stg_vaccination_unioned.smart_list_ind) = 1
                or max(stg_vaccination_unioned.flowsheet_ind) = 1
                or max(stg_vaccination_unioned.document_info_ind) = 1
            then 'Self Reported record'
        end as admin_source,
        max(stg_vaccination_unioned.influenza_vaccine_ind) as influenza_vaccine_ind,
        max(stg_vaccination_unioned.dxr_ind) as dxr_ind,
        max(stg_vaccination_unioned.lpl_ind) as lpl_ind,
        max(stg_vaccination_unioned.sde_ind) as sde_ind,
        max(stg_vaccination_unioned.smart_list_ind) as smart_list_ind,
        max(stg_vaccination_unioned.flowsheet_ind) as flowsheet_ind,
        max(stg_vaccination_unioned.document_info_ind) as document_info_ind
    from
         {{ ref('stg_vaccination_unioned') }} as stg_vaccination_unioned
    group by
        stg_vaccination_unioned.pat_id,
        stg_vaccination_unioned.received_date,
        stg_vaccination_unioned.documented_date,
        stg_vaccination_unioned.grouper_records_numeric_id
)
select
    {{
            dbt_utils.surrogate_key([
                'stage.pat_id',
                'stage.documented_date',
                'stage.received_date',
                'stage.grouper_records_numeric_id'
            ])
        }} as vaccination_received_key,
    stg_patient.pat_key,
    stg_patient.mrn,
    stage.pat_id,
    stage.received_date,
    stage.documented_date,
    stage.admin_source,
    stage.grouper_records_numeric_id,
    stg_lpl_vaccination.immune_id,
    stg_lpl_vaccination.order_id,
    max(
        case
            when grouper_compiled_rec_list.base_grouper_id = '123464'
            then 1
            else 0
            end
    ) as covid_vaccine_ind,
    max(
        case
            when grouper_compiled_rec_list.base_grouper_id = '1137225'
                or stage.influenza_vaccine_ind = 1
            then 1
            else 0
            end
    ) as influenza_vaccine_ind
from stage
    left join {{source('clarity_ods', 'grouper_compiled_rec_list')}}
        as grouper_compiled_rec_list
        on grouper_compiled_rec_list.grouper_records_numeric_id
        = stage.grouper_records_numeric_id
        and lower(grouper_compiled_rec_list.compiled_context) = 'lim'
    inner join {{ref('stg_patient')}} as stg_patient
        on stage.pat_id = stg_patient.pat_id
    left join
        {{ref('stg_lpl_vaccination')}} as stg_lpl_vaccination
        on stage.pat_id = stg_lpl_vaccination.pat_id
        and stage.received_date = stg_lpl_vaccination.received_date
        and stage.grouper_records_numeric_id = stg_lpl_vaccination.grouper_records_numeric_id
        and stg_lpl_vaccination.row_num = 1
group by
    vaccination_received_key,
    stg_patient.pat_key,
    stg_patient.mrn,
    stage.pat_id,
    stage.received_date,
    stage.documented_date,
    stg_lpl_vaccination.immune_id,
    stg_lpl_vaccination.order_id,
    stage.admin_source,
    stage.grouper_records_numeric_id
