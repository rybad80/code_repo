with unioned as (
    select
        pat_id,
        received_date,
        documented_date,
        grouper_records_numeric_id,
        influenza_vaccine_ind,
        lpl_ind,
        dxr_ind,
        sde_ind,
        smart_list_ind,
        flowsheet_ind,
        document_info_ind
    from
        {{ ref('stg_vaccination_unioned_documented') }}
    union all
    select
        pat_id,
        received_date,
        documented_date,
        grouper_records_numeric_id,
        influenza_vaccine_ind,
        lpl_ind,
        dxr_ind,
        sde_ind,
        smart_list_ind,
        flowsheet_ind,
        document_info_ind
    from
        {{ ref('stg_vaccination_unioned_received') }}
)
select
    pat_id,
    received_date,
    documented_date,
    grouper_records_numeric_id,
    influenza_vaccine_ind,
    lpl_ind,
    dxr_ind,
    sde_ind,
    smart_list_ind,
    flowsheet_ind,
    document_info_ind
from
    unioned
group by
    pat_id,
    received_date,
    documented_date,
    grouper_records_numeric_id,
    influenza_vaccine_ind,
    lpl_ind,
    dxr_ind,
    sde_ind,
    smart_list_ind,
    flowsheet_ind,
    document_info_ind
