with kaps_records as (
    select
        LPAD(TRIM(TRANSLATE(kaps_tbl_inc_cases.file_id, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()-,', '')), 15, '0') as record_id,
        kaps_person.mrn as mrn,
        kaps_person.encounter_number as csn,
        DATE(kaps_location.event_date) as event_dt,
        CAST(kaps_location.event_time as time) as event_tm,
        DATE(kaps_location.last_modified) as submit_dt,
        case
            when kaps_location.event_program like 'NIC%'
                then 'NICU'
            when kaps_location.event_program like 'PIC%'
                then 'PICU'
            else kaps_location.event_program
        end as dept_name,
        null as procedural_area,
        'Yes' as confirmed_not_ue,
        'KAPS' as numerator_source
    from
        {{ source('kaps_ods', 'kaps_tbl_inc_cases') }} as kaps_tbl_inc_cases
        inner join {{ source('kaps_ods', 'kaps_tbl_rsk_event_when_where') }} as kaps_location
            on kaps_tbl_inc_cases.incident_id = kaps_location.incident_id
        inner join {{ source('kaps_ods', 'kaps_tbl_rsk_event_person_affected') }} as kaps_person
            on kaps_tbl_inc_cases.incident_id = kaps_person.incident_id
    where (
            LOWER(kaps_tbl_inc_cases.specific_event_type) = 'extubation - unplanned'
            or LOWER(kaps_tbl_inc_cases.specific_event_type) = 'extubation - self'
        )
        and LOWER(dept_name) in (
            'nicu',
            'picu',
            'cicu'
        )
        --Exclude KAPS created after first Redcap
        and event_dt < '2021-01-19'
        and (
            kaps_tbl_inc_cases.event_description is null
            or (
                kaps_tbl_inc_cases.event_description not like '% trach %'
                and kaps_tbl_inc_cases.event_description not like '%tracheostomy%'
            )
        )
),
rsr as (
    select
        rsr.redcap_record,
        rsr.mstr_redcap_event_key,
        UPPER(MAX(rsr.survey_response_return_cd)) as return_cd,
        MAX(rsr.survey_response_first_submit_dt) as timestamps
    from
        {{ source('cdw', 'master_redcap_survey_response') }} as rsr
    group by
        rsr.redcap_record,
        rsr.mstr_redcap_event_key
),
rc as (
    --region gather results from UE Redcap
    select
        rcp.app_title,
        rcq.mstr_redcap_quest_key,
        rcq.field_order,
        rcq.field_nm,
        rcq.element_label,
        rcd.record,
        rcea.element_id,
        SUBSTR(COALESCE(rcea.element_desc, rcd.value), 1, 250) as value,
        rsr.return_cd,
        rsr.timestamps,
        ROW_NUMBER() over (
            partition by
                rcd.record,
                rcd.mstr_redcap_quest_key
            order by
                rcea.element_id) as row_num
    from
        {{ source('cdw', 'redcap_detail') }} as rcd
        left join {{ source('cdw', 'master_redcap_project') }} as rcp
            on rcp.mstr_project_key = rcd.mstr_project_key
        left join {{ source('cdw', 'master_redcap_question') }} as rcq
            on rcq.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key
        left join {{ source('cdw', 'master_redcap_element_answr') }} as rcea
            on rcea.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key
            and rcd.value = rcea.element_id
        left join rsr
             on rsr.mstr_redcap_event_key = rcd.mstr_redcap_event_key
             and rsr.redcap_record = rcd.record
    where
        rcd.cur_rec_ind = 1
        and rcp.project_id = 1078 --UE Bedside Review
),
rc_flat as (
    --region convert table to wide where each row is a singular redcap
    --only columns used in dashboard will be captured to reduce processing
    select
        rc.record as record_id,
        MAX(rc.return_cd) as return_cd,
        MAX(rc.timestamps) as submit_dt,
        MAX(case when rc.field_nm = 'mrn'
            then rc.value end) as mrn,
        MAX(case when rc.field_nm = 'csn'
            and rc.value != 'unknown'
            then rc.value end) as csn,
        MAX(case when rc.field_nm = 'event_date'
            then rc.value end) as event_dt,
        MAX(case when rc.field_nm = 'event_time'
            then rc.value end) as event_tm,
        MAX(case when rc.field_nm = 'unit'
            then rc.value end) as unit_occurred,
        MAX(case when rc.field_nm = 'proc_area_loc'
            then rc.value end) as procedural_area,
        MAX(case when rc.field_nm = 'ett_remove_deliberate'
            then rc.value end) as confirmed_not_ue,
        MAX(case when rc.field_nm = 'ue_bedside_review_complete'
            then rc.value end) as complete_ind
    from
        rc
    group by
        rc.record
--end region
),
redcap_records as (
    select
        rc_flat.record_id,
        rc_flat.mrn,
        rc_flat.csn,
        DATE(rc_flat.event_dt) as event_dt,
        CAST(rc_flat.event_tm as time) as event_tm,
        rc_flat.submit_dt,
        case when rc_flat.unit_occurred in (
                'NIC East',
                'NIC C',
                'NIC West 1',
                'NIC West 2',
                'NIC Northeast'
            ) then 'NICU'
            else rc_flat.unit_occurred end as dept_name,
        rc_flat.procedural_area,
        --override UEs prior to Indeterminate field
        case when rc_flat.event_dt < '2023-02-17'
            and rc_flat.confirmed_not_ue is null
            then 'Yes'
            else rc_flat.confirmed_not_ue
            end as confirmed_not_ue,
        'REDCAP' as numerator_source
    from
        rc_flat
    --end region
),

joined as (
    --region combine unprocessed KAPS and Redcap data
    select
        *
    from
        kaps_records

    union all

    select
        *
    from
        redcap_records
    --exclude records where clinicians intentionally removed UEs
    --also exclude records after 02/17/2023 that haven't been reviewed
    where
        confirmed_not_ue != 'No'
)

select
    joined.*,
    patient.pat_key,
    visit.visit_key
from
    joined
    left join {{ source('cdw', 'patient') }} as patient
        on joined.mrn = patient.pat_mrn_id
    left join {{ source('cdw', 'visit') }} as visit
        on CAST(joined.csn as numeric(14, 3)) = visit.enc_id
