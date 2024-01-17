with fs_clinical_concern as (
    select
        flowsheet_all.pat_key,
        flowsheet_all.visit_key,
        flowsheet_all.fs_rec_key,
        flowsheet_all.seq_num,
        flowsheet_all.flowsheet_id,
        flowsheet_all.flowsheet_record_id,
        flowsheet_all.flowsheet_name,
        flowsheet_all.meas_val,
        flowsheet_all.recorded_date
    from
        {{ ref('flowsheet_all') }} as flowsheet_all
    where
        flowsheet_all.flowsheet_id = 10376
        and flowsheet_all.meas_val is not null
)

select
    fs_clinical_concern.pat_key,
    fs_clinical_concern.visit_key,
    case
        when fs_clinical_concern.meas_val = 'No concern for clinical deterioration risk'
            then 'Clinical Concern - None'
        when fs_clinical_concern.meas_val = 'Slight concern for clinical deterioration risk'
            then 'Clinical Concern - Slight'
        when fs_clinical_concern.meas_val = 'Moderate concern for clinical deterioration risk'
            then 'Clinical Concern - Moderate'
        when fs_clinical_concern.meas_val = 'Significant concern for clinical deterioration risk'
            then 'Clinical Concern - Significant'
    end as event_type_name,
    case
        when fs_clinical_concern.meas_val = 'No concern for clinical deterioration risk'
            then 'CLIN_CONCERN_0'
        when fs_clinical_concern.meas_val = 'Slight concern for clinical deterioration risk'
            then 'CLIN_CONCERN_1'
        when fs_clinical_concern.meas_val = 'Moderate concern for clinical deterioration risk'
            then 'CLIN_CONCERN_2'
        when fs_clinical_concern.meas_val = 'Significant concern for clinical deterioration risk'
            then 'CLIN_CONCERN_3'
    end as event_type_abbrev,
    fs_clinical_concern.recorded_date as event_start_date,
    null as event_end_date
from
    fs_clinical_concern
where
    fs_clinical_concern.recorded_date >= '2017-01-01'
