with  document_info as (
    select
        doc_information.doc_pt_id,
        stg_patient_ods.pat_id,
        date(doc_information.scan_time) as documented_date,
        null as grouper_records_numeric_id,
        1 as influenza_vaccine_ind
    from
        {{ source ('clarity_ods', 'doc_information') }} as doc_information
        inner join {{ ref('stg_patient_ods') }} as stg_patient_ods
            on doc_information.doc_pt_id = stg_patient_ods.pat_id
    where
        upper(doc_information.doc_descr) like '%FLU VACCINE%'
        or upper(doc_information.doc_descr) like '%FLU SHOT%'
    group by
        doc_information.doc_pt_id,
        stg_patient_ods.pat_id,
        date(doc_information.scan_time)
),

flowsheet as (
select
    flowsheet_all.pat_key,
    stg_patient_ods.pat_id,
    date(flowsheet_all.encounter_date) as documented_date,
    null as grouper_records_numeric_id,
    1 as influenza_vaccine_ind
from {{ ref ('flowsheet_all') }} as flowsheet_all
  inner join {{ ref ('stg_patient_ods') }} as stg_patient_ods
        on flowsheet_all.pat_key = stg_patient_ods.patient_key
where
    flowsheet_all.flowsheet_id in (
    15789 --'Flu Shot Up to Date? (Sept 1 - March 31 Only)'
        )
    and flowsheet_all.meas_val in ('Yes', 'Yes, administering today')
    and flowsheet_all.meas_val is not null
group by
    flowsheet_all.pat_key,
    stg_patient_ods.pat_id,
    date(flowsheet_all.encounter_date)
),

stage as (
    select
        stg_sde_vaccination.pat_id,
        null as received_date,
        stg_sde_vaccination.documented_date,
        stg_sde_vaccination.grouper_records_numeric_id,
        stg_sde_vaccination.influenza_vaccine_ind,
        0 as lpl_ind,
        0 as dxr_ind,
        1 as sde_ind,
        0 as smart_list_ind,
        0 as flowsheet_ind,
        0 as document_info_ind
    from {{ ref ('stg_sde_vaccination') }} as stg_sde_vaccination
    group by
        stg_sde_vaccination.pat_id,
        stg_sde_vaccination.documented_date,
        stg_sde_vaccination.grouper_records_numeric_id,
        stg_sde_vaccination.influenza_vaccine_ind
    union all
        select
        stg_bpa_vaccination.pat_id,
        null as received_date,
        stg_bpa_vaccination.care_asst_month_year_documented as documented_date,
        stg_bpa_vaccination.historical_seq as grouper_records_numeric_id,
        stg_bpa_vaccination.influenza_vaccine_ind,
        0 as lpl_ind,
        0 as dxr_ind,
        1 as sde_ind,
        0 as smart_list_ind,
        0 as flowsheet_ind,
        0 as document_info_ind
    from {{ ref ('stg_bpa_vaccination') }} as stg_bpa_vaccination
    group by
        stg_bpa_vaccination.pat_id,
        stg_bpa_vaccination.care_asst_month_year_documented,
        stg_bpa_vaccination.historical_seq,
        stg_bpa_vaccination.influenza_vaccine_ind
    union all
    select
        stg_smart_list_vaccination.pat_id,
        null as received_date,
        stg_smart_list_vaccination.documented_date,
        stg_smart_list_vaccination.grouper_records_numeric_id,
        stg_smart_list_vaccination.influenza_vaccine_ind,
        0 as lpl_ind,
        0 as dxr_ind,
        0 as sde_ind,
        1 as smart_list_ind,
        0 as flowsheet_ind,
        0 as document_info_ind
    from {{ ref ('stg_smart_list_vaccination') }} as stg_smart_list_vaccination
    group by
        stg_smart_list_vaccination.pat_id,
        stg_smart_list_vaccination.documented_date,
        stg_smart_list_vaccination.grouper_records_numeric_id,
        stg_smart_list_vaccination.influenza_vaccine_ind
    union all
    select
        stg_bpa_vaccination.pat_id,
        null as received_date,
        stg_bpa_vaccination.care_asst_month_year_documented as documented_date,
        stg_bpa_vaccination.historical_seq as grouper_records_numeric_id,
        stg_bpa_vaccination.influenza_vaccine_ind,
        0 as lpl_ind,
        0 as dxr_ind,
        1 as sde_ind,
        0 as smart_list_ind,
        0 as flowsheet_ind,
        0 as document_info_ind
    from {{ ref ('stg_bpa_vaccination') }} as stg_bpa_vaccination
    group by
        stg_bpa_vaccination.pat_id,
        stg_bpa_vaccination.care_asst_month_year_documented,
        stg_bpa_vaccination.historical_seq,
        stg_bpa_vaccination.influenza_vaccine_ind
    union all
    select
        flowsheet.pat_id,
        null as received_date,
        flowsheet.documented_date,
        flowsheet.grouper_records_numeric_id,
        flowsheet.influenza_vaccine_ind,
        0 as lpl_ind,
        0 as dxr_ind,
        0 as sde_ind,
        0 as smart_list_ind,
        1 as flowsheet_ind,
        0 as document_info_ind
    from flowsheet
    union all
    select
        document_info.pat_id,
        null as received_date,
        document_info.documented_date,
        document_info.grouper_records_numeric_id,
        document_info.influenza_vaccine_ind,
        0 as lpl_ind,
        0 as dxr_ind,
        0 as sde_ind,
        0 as smart_list_ind,
        0 as flowsheet_ind,
        1 as document_info_ind
    from document_info
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
    stage
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
