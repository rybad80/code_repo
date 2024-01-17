with unioned as (
    select
        stg_lpl_vaccination.pat_id,
        stg_lpl_vaccination.received_date,
        null as documented_date,
        stg_lpl_vaccination.grouper_records_numeric_id,
        stg_lpl_vaccination.influenza_vaccine_ind,
        1 as lpl_ind,
        0 as dxr_ind,
        0 as sde_ind,
        0 as smart_list_ind,
        0 as flowsheet_ind,
        0 as document_info_ind
    from {{ ref ('stg_lpl_vaccination') }} as stg_lpl_vaccination
    group by
        stg_lpl_vaccination.pat_id,
        stg_lpl_vaccination.received_date,
        stg_lpl_vaccination.grouper_records_numeric_id,
        stg_lpl_vaccination.influenza_vaccine_ind
    union all
    select
        stg_dxr_vaccination.pat_id, -- CONTAINS NULL pat_id
        stg_dxr_vaccination.received_date,
        null as documented_date,
        stg_dxr_vaccination.grouper_records_numeric_id,
        stg_dxr_vaccination.influenza_vaccine_ind,
        0 as lpl_ind,
        1 as dxr_ind,
        0 as sde_ind,
        0 as smart_list_ind,
        0 as flowsheet_ind,
        0 as document_info_ind
    from {{ ref ('stg_dxr_vaccination') }} as stg_dxr_vaccination
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
