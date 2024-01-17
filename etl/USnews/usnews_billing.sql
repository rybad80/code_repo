with proc_dx as (
    select
        usnwr_proc.submission_year,
        usnwr_proc.metric_id,
        usnwr_proc.metric_name,
        usnwr_proc.start_date,
        usnwr_proc.end_date,
        tx_id,
        source_summary,
        diagnosis_seq_num,
        usnwr_proc.inclusion_ind as proc_inclusion_ind,
        usnwr_proc.exclusion_ind as proc_exclusion_ind,
        0 as dx_inclusion_ind,
        0 as dx_exclusion_ind
    from
        {{ref('usnews_metadata_calendar')}} as usnwr_proc
        inner join
            {{ref('procedure_billing')}}  as procedure_billing
            on procedure_billing.cpt_code = usnwr_proc.code
                and lower(usnwr_proc.code_type) = 'cpt_code'
    where
        age_years between usnwr_proc.age_gte and usnwr_proc.age_lt
        and service_date between start_date and end_date

    union all

    select
        usnwr_dx.submission_year,
        usnwr_dx.metric_id,
        usnwr_dx.metric_name,
        usnwr_dx.start_date,
        usnwr_dx.end_date,
        tx_id,
        source_summary,
        diagnosis_seq_num,
        0 as proc_inclusion_ind,
        0 as proc_exclusion_ind,
        usnwr_dx.inclusion_ind as dx_inclusion_ind,
        usnwr_dx.exclusion_ind as dx_exclusion_ind
    from
        {{ref('usnews_metadata_calendar')}} as usnwr_dx
        inner join
            {{ref('procedure_billing')}}  as procedure_billing
            on procedure_billing.icd10_code = usnwr_dx.code
                and lower(usnwr_dx.code_type) = 'icd10_code'
    where
        age_years between usnwr_dx.age_gte and usnwr_dx.age_lt
        and service_date between start_date and end_date
),

proc_dx_combo as (
    select
        submission_year,
        metric_id,
        metric_name,
        start_date,
        end_date,
        tx_id,
        source_summary,
        diagnosis_seq_num,
        max(proc_inclusion_ind) as proc_inclusion_ind,
        max(proc_exclusion_ind) as proc_exclusion_ind,
        max(dx_inclusion_ind) as dx_inclusion_ind,
        max(dx_exclusion_ind) as dx_exclusion_ind
    from
        proc_dx
    group by
        submission_year,
        metric_id,
        metric_name,
        start_date,
        end_date,
        tx_id,
        source_summary,
        diagnosis_seq_num
),

stage as (
    select distinct
        submission_year,
        metric_id,
        metric_name,
        start_date,
        end_date,
        dx_inclusion_ind,
        dx_exclusion_ind,
        proc_inclusion_ind,
        proc_exclusion_ind,
        procedure_billing.*,
        department.specialty as department_specialty
    from
        proc_dx_combo
        inner join {{ref('procedure_billing')}}  as procedure_billing
            on proc_dx_combo.tx_id = procedure_billing.tx_id
                and procedure_billing.source_summary = proc_dx_combo.source_summary
                and procedure_billing.diagnosis_seq_num = proc_dx_combo.diagnosis_seq_num
        left join
            {{source('cdw', 'department')}} as department
            on procedure_billing.department_id = department.dept_id
    where
        date(procedure_billing.service_date) >= '2016-01-01'
        and {{ limit_dates_for_dev(ref_date = 'procedure_billing.service_date') }}
)
select distinct
    stage.*,
    lookup_usnews_metadata.division,
    lookup_usnews_metadata.question_number,
    lookup_usnews_metadata.age_gte,
    lookup_usnews_metadata.age_lt,
    lookup_usnews_metadata.num_calculation,
    lookup_usnews_metadata.denom_calculation,
    lookup_usnews_metadata.direction,
    case when lookup_usnews_metadata.keep_criteria = 'procedure'
            and stage.proc_inclusion_ind = 1 then 1
        when lookup_usnews_metadata.keep_criteria = 'dx'
            and stage.dx_inclusion_ind = 1 then 1
        when lookup_usnews_metadata.keep_criteria = 'procedure or dx'
            and (stage.proc_inclusion_ind = 1 or stage.dx_inclusion_ind = 1) then 1
        when lookup_usnews_metadata.keep_criteria = 'procedure and dx'
            and (stage.proc_inclusion_ind = 1 and stage.dx_inclusion_ind = 1) then 1
        else 0 end as keep_ind,
    lookup_usnews_metadata.review_ind,
    lookup_usnews_metadata.submitted_ind,
    lookup_usnews_metadata.scored_ind
from
    stage
    inner join {{ref('lookup_usnews_metadata')}} as lookup_usnews_metadata
        on lookup_usnews_metadata.metric_id = stage.metric_id
        and lookup_usnews_metadata.metric_name = stage.metric_name
where
    age_years between lookup_usnews_metadata.age_gte and lookup_usnews_metadata.age_lt
    and service_date between start_date and end_date
    and (lower(stage.department_specialty) = lower(lookup_usnews_metadata.specialty)
        or specialty is null)
    and keep_ind = 1
