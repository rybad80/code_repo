with vpah as (
    select
        pend_action.pat_enc_csn_id,
        stg_encounter.patient_key,
        stg_encounter.encounter_key
    from
        {{source('clarity_ods','pend_action')}} as pend_action
        inner join {{ref('stg_encounter')}} as stg_encounter
            on pend_action.pat_enc_csn_id = stg_encounter.csn
        /* the Transfer Out for the visit_pending_action */
        inner join {{source('clarity_ods','clarity_adt')}} as clarity_adt
            on pend_action.linked_event_id = clarity_adt.xfer_in_event_id
        inner join {{ref('stg_department_all')}} as stg_department_all
            on pend_action.unit_id = stg_department_all.department_id
            and stg_department_all.intended_use_id !=  1003 -- ED
    where
        stg_encounter.encounter_date >= '2017-07-01'
        and event_type_c = 4 -- Transfer Out
        and event_subtype_c != 2 -- Not Cancelled
),

selectevtsts as (
    select
        vpah.pat_enc_csn_id,
        ed_iev_event_info.event_time,
        vpah.patient_key,
        ed_iev_event_info.event_type,
        dense_rank() over (partition by ed_iev_pat_info.pat_enc_csn_id, ed_iev_event_info.event_type
                           order by ed_iev_pat_info.pat_enc_csn_id,
                           ed_iev_event_info.event_type, ed_iev_event_info.event_time desc --noqa: L037
        ) as get_latest_event_dt_rownum
    from
        {{source('clarity_ods','ed_iev_pat_info')}} as ed_iev_pat_info
        inner join {{source('clarity_ods','ed_iev_event_info')}} as ed_iev_event_info
            on ed_iev_event_info.event_id = ed_iev_pat_info.event_id
        inner join vpah
            on ed_iev_pat_info.pat_enc_csn_id = vpah.pat_enc_csn_id
    where
        ed_iev_event_info.event_type in
        (
            '300100', -- ED Admit-MD report given add Patient Left
            '300102' -- ED Admit-Handoff Report review
        )
        or (-- ED used different event id pre FY 2017
            -- ED ADMIT DISPOSITION SELECTED
            ed_iev_event_info.event_type = '1061222'
            and ed_iev_event_info.event_time >= '2017-07-01'
        )
        or (-- CHOP ED READY TO PLAN Ready to Plan
            ed_iev_event_info.event_type = '300835'
            and ed_iev_event_info.event_time < '2017-07-01'
        )
),

vstedevt as (
    select
        pat_enc_csn_id,
        event_type,
        event_time as event_action_dt_tm,
        patient_key
    from
        selectevtsts
    where
        -- need to get the max date and comments for that one that is the latest per event type
        -- and EVENT_ID in (239,300499) -- ones for which we care about EVENT_CMT to put in the  INFO field
        get_latest_event_dt_rownum = 1
),

dates as (
    select
        capacity_ip_census_cohort.visit_event_key,
        max(
            case
                when vstedevt.event_type in ('300835', '1061222')
                then  vstedevt.event_action_dt_tm
            end
        ) as ed_mrft_date,
        min(
            case
                when stg_capacity_pending_action.event_action_nm = 'Unit Assign'
                then  stg_capacity_pending_action.event_action_dt_tm
            end
        ) as admit_unit_assigned_date,
        min(
            case
                when stg_capacity_pending_action.event_action_nm = 'Bed Assign'
                then  stg_capacity_pending_action.event_action_dt_tm
            end
        ) as admit_bed_assigned_date,
        min(
            case
                when stg_capacity_pending_action.event_action_nm = 'Bed Approval'
                then  stg_capacity_pending_action.event_action_dt_tm
            end
        ) as admit_bed_approval_date,
        max(
            case
                when vstedevt.event_type = '300102'
                then  vstedevt.event_action_dt_tm
            end
        ) as rn_handoff_date,
        max(
            case
                when vstedevt.event_type = '300100'
                then  vstedevt.event_action_dt_tm
            end
        ) as md_handoff_date,
        capacity_ip_census_cohort.inpatient_census_admit_date as admit_from_ed_date
    from
        {{ref('capacity_ip_census_cohort')}} as capacity_ip_census_cohort
        left join vstedevt
            on vstedevt.pat_enc_csn_id = capacity_ip_census_cohort.csn
        left join {{ref('stg_capacity_pending_action')}} as stg_capacity_pending_action
            on stg_capacity_pending_action.pat_enc_csn_id = capacity_ip_census_cohort.csn
    group by
        capacity_ip_census_cohort.visit_event_key,
        capacity_ip_census_cohort.inpatient_census_admit_date
),

intervals as (
    select
        visit_event_key,
        extract( --noqa: PRS
            epoch from rn_handoff_date - admit_bed_assigned_date
        ) / 60.0 as admit_bed_assigned_to_rn_handoff_mins,
        extract( --noqa: PRS
            epoch from md_handoff_date - admit_unit_assigned_date
        ) / 60.0 as admit_unit_assigned_to_md_handoff_mins,
        extract( --noqa: PRS
            epoch from rn_handoff_date - admit_bed_approval_date
        ) / 60.0 as admit_bed_approval_to_rn_handoff_mins,
        extract( --noqa: PRS
            epoch from md_handoff_date - admit_bed_approval_date
        ) / 60.0 as admit_bed_approval_to_md_handoff_mins,
        extract( --noqa: PRS
            epoch from admit_bed_approval_date - admit_bed_assigned_date
        ) / 60.0 as admit_bed_assigned_to_bed_approval_mins,
        extract( --noqa: PRS
            epoch from admit_bed_assigned_date - admit_unit_assigned_date
        ) / 60.0 as admit_unit_assigned_to_bed_assigned_mins,
        extract( --noqa: PRS
            epoch from admit_unit_assigned_date - ed_mrft_date
        ) / 60.0 as ed_mrft_to_unit_assigned_mins,
        extract( --noqa: PRS
            epoch from admit_from_ed_date - ed_mrft_date
        ) / 60.0 as ed_mrft_to_admit_mins,
        extract( --noqa: PRS
            epoch from admit_from_ed_date - admit_bed_assigned_date
        ) / 60.0 as admit_bed_assigned_to_admit_mins,
        extract( --noqa: PRS
            epoch from admit_from_ed_date - admit_bed_approval_date
        ) / 60.0 as admit_bed_approval_to_admit_mins,
        extract( --noqa: PRS
            epoch from admit_from_ed_date - admit_unit_assigned_date
        ) / 60.0 as admit_unit_assigned_to_admit_mins
    from
        dates
)

select
    capacity_ip_census_cohort.visit_key,
    intervals.visit_event_key,
    capacity_ip_census_cohort.pat_key,
    capacity_ip_census_cohort.dept_key,
    capacity_ip_census_cohort.mrn,
    capacity_ip_census_cohort.csn,
    capacity_ip_census_cohort.patient_name,
    capacity_ip_census_cohort.dob,
    capacity_ip_census_cohort.inpatient_census_admit_date,
    capacity_ip_census_cohort.admission_department_center_abbr,
    dates.ed_mrft_date,
    dates.admit_unit_assigned_date,
    dates.admit_bed_assigned_date,
    dates.admit_bed_approval_date,
    dates.rn_handoff_date,
    dates.md_handoff_date,
    dates.admit_from_ed_date,
    case
        when
            admit_bed_assigned_to_rn_handoff_mins >= 0
            and admit_bed_assigned_to_admit_mins >= 0
        then
            admit_bed_assigned_to_rn_handoff_mins
    end as admit_bed_assigned_to_rn_handoff_mins,
    case
        when
            admit_unit_assigned_to_md_handoff_mins >= 0
            and admit_unit_assigned_to_admit_mins >= 0
        then
            admit_unit_assigned_to_md_handoff_mins
    end as admit_unit_assigned_to_md_handoff_mins,
    case
        when
            admit_bed_approval_to_rn_handoff_mins >= 0
            and admit_bed_approval_to_admit_mins >= 0
        then
            admit_bed_approval_to_rn_handoff_mins
    end as admit_bed_approval_to_rn_handoff_mins,
    case
        when
            admit_bed_approval_to_md_handoff_mins >= 0
            and admit_bed_approval_to_admit_mins >= 0
        then
            admit_bed_approval_to_md_handoff_mins
    end as admit_bed_approval_to_md_handoff_mins,
    case
        when
            admit_bed_assigned_to_bed_approval_mins >= 0
            and admit_bed_assigned_to_admit_mins >= 0
            and admit_bed_approval_to_admit_mins >= 0
        then
            admit_bed_assigned_to_bed_approval_mins
    end as admit_bed_assigned_to_bed_approval_mins,
    case
        when
            admit_unit_assigned_to_bed_assigned_mins >= 0
            and admit_unit_assigned_to_admit_mins >= 0
        then
            admit_unit_assigned_to_bed_assigned_mins
    end as admit_unit_assigned_to_bed_assigned_mins,
    case
        when
            ed_mrft_to_unit_assigned_mins >= 0
            and admit_unit_assigned_to_admit_mins >= 0
        then
            ed_mrft_to_unit_assigned_mins
    end as ed_mrft_to_unit_assigned_mins,
    case
        when
            ed_mrft_to_admit_mins >= 0
        then
            ed_mrft_to_admit_mins
    end as ed_mrft_to_admit_mins,
    case
        when ed_mrft_to_admit_mins <= 120 and ed_mrft_to_admit_mins >= 0
        then 1
        when ed_mrft_to_admit_mins > 120 and ed_mrft_to_admit_mins >= 0
        then 0
    end as ed_mrft_to_admit_target_ind
from
    intervals
    inner join {{ref('capacity_ip_census_cohort')}} as capacity_ip_census_cohort
        on capacity_ip_census_cohort.visit_event_key = intervals.visit_event_key
    inner join dates
        on dates.visit_event_key = intervals.visit_event_key
    inner join {{ref('adt_department')}} as adt_department
        on adt_department.visit_event_key = intervals.visit_event_key
where
    dates.ed_mrft_date is not null
    -- extreme outliers
    and intervals.ed_mrft_to_admit_mins <= 2000
    and intervals.admit_unit_assigned_to_md_handoff_mins <= 2000
    -- admitted to inpatient unit intervals.visit_event_key is the IP admission event
    and adt_department.all_department_order = 2
