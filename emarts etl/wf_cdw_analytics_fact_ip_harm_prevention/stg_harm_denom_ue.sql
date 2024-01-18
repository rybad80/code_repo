with icu_days as (
    --region count days patient was intubated in ICU
    select
        v.visit_key,
        v.pat_key,
        v.dept_key,
        date(fact_census_occ.census_dt) as event_dt,
        d.department_center_abbr,
        max(case when fact_census_occ.census_dt between
            v.enter_dt and v.exit_dt
            then 1 else 0 end) as dept_at_midnight_ind
    from
        {{ref('stg_visit_event_service')}} as v
        --patient census matches particular service unit
        inner join {{ source('cdw', 'fact_census_occ') }} as fact_census_occ
            on v.visit_key = fact_census_occ.visit_key
            --month where first UE incident was reported.
            and fact_census_occ.census_dt >= '2015-12-01'
            and fact_census_occ.census_dt between
                --date function used in case UE on first day of ventilation
                date(v.enter_dt) and v.exit_dt
        --patient invasively ventilated on census date
        inner join {{ source('cdw', 'visit_stay_info') }} as visit_stay_info
            on fact_census_occ.visit_key = visit_stay_info.visit_key
        inner join {{ source('cdw', 'flowsheet_record') }} as flowsheet_record
            on visit_stay_info.vsi_key = flowsheet_record.vsi_key
            and flowsheet_record.fs_rec_dt = fact_census_occ.census_dt
        inner join {{ source('cdw', 'flowsheet_measure') }} as flowsheet_measure
            on flowsheet_record.fs_rec_key = flowsheet_measure.fs_rec_key
        inner join {{ source('cdw', 'flowsheet_group') }} as flowsheet_group
            on flowsheet_measure.fs_key = flowsheet_group.grp_fs_key
        inner join {{ source('cdw','flowsheet') }} as flowsheet
            on flowsheet_group.fs_key = flowsheet.fs_key
            and flowsheet.fs_id = 40010938 --Invasive Ventilation
        --limit to ICU departments
        inner join {{ref('fact_department_rollup')}} as d
            on v.dept_key = d.dept_key
            and d.unit_dept_grp_abbr in (
                'NICU',
                'CICU',
                'PICU',
                'PICU OVF'
            )
            and fact_census_occ.census_dt = d.dept_align_dt
    group by
        v.visit_key,
        v.pat_key,
        v.dept_key,
        fact_census_occ.census_dt,
        d.department_center_abbr
),

lda_latest as (
    --region acquire most recent LDA record per flowsheet
    select
        flowsheet_lda_group.fs_key,
        flowsheet_lda_group.seq_num,
        flowsheet_lda_group.dict_lda_type_key,
        max(flowsheet_lda_group.contact_dt_key) as max_contact_date
    from
        {{ source('cdw', 'flowsheet_lda_group') }} as flowsheet_lda_group
    group by
        flowsheet_lda_group.fs_key,
        flowsheet_lda_group.seq_num,
        flowsheet_lda_group.dict_lda_type_key
--end region
),

airway_active as (
    --region select most recent patient airway records
    select
        patient_lda.pat_key,
        patient_lda.place_dt,
        case
            when patient_lda.remove_dt <= current_timestamp
            then patient_lda.remove_dt
        end as remove_dt,
        patient_lda.lda_desc
    from
        {{ source('cdw', 'patient_lda') }} as patient_lda
        --Use latest LDA record
        inner join lda_latest
            on patient_lda.fs_key = lda_latest.fs_key
        --select only airway records
        inner join {{ source('cdw', 'cdw_dictionary') }} as lda_group
            on lda_latest.dict_lda_type_key = lda_group.dict_key
            and lda_group.src_id = 5 --airway
    where
        lower(patient_lda.lda_desc) like '%endotracheal%' --ETT
        or lower(patient_lda.lda_desc) like '%naso%' --Nasopharyngeal Airway
    group by
        patient_lda.pat_key,
        patient_lda.place_dt,
        patient_lda.remove_dt,
        patient_lda.lda_desc
)

select
    icu_days.visit_key,
    icu_days.pat_key,
    icu_days.dept_key,
    icu_days.event_dt,
    icu_days.department_center_abbr,
    'FACT_CENSUS' as denominator_source,
    max(--count overnight days in ICU
        case
            when icu_days.dept_at_midnight_ind = 1
            and icu_days.event_dt > airway_active.place_dt
            and (
                icu_days.event_dt <= airway_active.remove_dt
                or airway_active.remove_dt is null
            )
        then 1 else 0 end
    ) as overnight_ind
from
    icu_days
    inner join airway_active
        on icu_days.pat_key = airway_active.pat_key
where ( --airway device used during patient encounter
    --scenario 1: lda is placed and removed in chop
    (icu_days.event_dt between date(airway_active.place_dt)
        and date(airway_active.remove_dt))
    --scenario 2: lda is placed outside of chop
    or (airway_active.place_dt is null
        and icu_days.event_dt <= date(airway_active.remove_dt))
    --scenario 3: lda has been not removed yet
    or (icu_days.event_dt >= date(airway_active.place_dt)
        and airway_active.remove_dt is null)
    --scenario 4: lda is placed and removed outside of chop
    or (airway_active.place_dt is null
        and airway_active.remove_dt is null)
)
group by
    icu_days.visit_key,
    icu_days.pat_key,
    icu_days.dept_key,
    icu_days.event_dt,
    icu_days.dept_key,
    icu_days.department_center_abbr
