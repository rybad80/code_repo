/*TABLE AT PROC_NM LEVEL*/
with or_procs as (
    select
        or_log.log_key,
        or_log_case_times.event_in_dt,
        or_log_case_times.event_out_dt,
        case
            when surgery_procedure.or_procedure_name = 'EXIT â CESAREAN DELIVERY, NEONATAL LUNG LOBECTOMY'
            then 'EXIT CESAREAN DELIVERY, NEONATAL LUNG LOBECTOMY'
            else substring(surgery_procedure.or_procedure_name from 1 for 80) --noqa: PRS
        end as or_proc_nm,
           /* INDICATORS AT PROCEDURE LEVEL */
        case when lower(or_proc_nm) like '%tumor%' or lower(or_proc_nm) like '%mass%'
            then 1 else 0 end as tumor_mass_ind,
        case when lower(or_proc_nm) like '%lesion%' then 1 else 0 end as lesion_ind,
        case when lower(or_proc_nm) like '%excision%' then 1 else 0 end as excision_ind,
        case when lower(or_proc_nm) like '%resection%' or lower(or_proc_nm) like '%removal%'
        then 1 else 0 end as resection_removal_ind,
        case when lower(or_proc_nm) like '%ectomy%' then 1 else 0 end as ectomy_ind,
        case when lower(or_proc_nm) like '%biopsy%' then 1 else 0 end as biopsy_ind,
        surgery_procedure.procedure_seq_num
    from
        {{ref('surgery_procedure')}} as surgery_procedure
        inner join {{source('cdw','or_log')}} as or_log
            on or_log.log_key = surgery_procedure.log_key
        inner join {{source('cdw', 'or_log_case_times')}} as or_log_case_times
            on or_log.log_key = or_log_case_times.log_key
        inner join {{source('cdw', 'cdw_dictionary')}} as or_event_type
            on or_log_case_times.dict_or_pat_event_key = or_event_type.dict_key
        inner join {{source('cdw', 'cdw_dictionary')}} as or_stat
            on or_log.dict_or_stat_key = or_stat.dict_key
        inner join {{source('cdw', 'cdw_dictionary')}} as or_perf
            on or_log.dict_not_perf_key = or_perf.dict_key
    where
        (lower(or_proc_nm) like '%tumor%'
        or lower(or_proc_nm) like '%mass%'
        or lower(or_procedure_name) like '%lesion%'
        or lower(or_procedure_name) like '%excision%'
        or lower(or_proc_nm) like '%ectomy%'
        or lower(or_procedure_name) like '%biopsy%'
        or lower(or_procedure_name) like '%resection%'
        or lower(or_procedure_name) like '%removal%'
        )

        and lower(or_procedure_name) not like '%shunt%'
        and lower(or_procedure_name) not like '%lumbar puncture%'
        and lower(or_procedure_name) not like '%spinal puncture%'
        and lower(or_procedure_name) not like '%spinal puncture%'

        -- added by Dr.Phillips 1/21/2020
        and or_procedure_name not like 'DIGIT, NAIL BED EXCISION'
        and or_procedure_name not like 'EAR TUBE REMOVAL'
        and or_procedure_name not like 'EUA, CERUMEN REMOVAL'
        and or_procedure_name not like 'FOREIGN BODY REMOVAL FROM EAR'
        and or_procedure_name not like 'FOREIGN BODY REMOVAL FROM NOSE'
        and or_procedure_name not like 'FOREIGN BODY REMOVAL, COMPLICATED'
        and or_procedure_name not like 'FOREIGN BODY REMOVAL, SIMPLE'
        and or_procedure_name not like 'IUD INSERTION OR REMOVAL'
        and or_procedure_name not like 'RETAINED LINE REMOVAL'
        and or_procedure_name not like 'TOOTH/TEETH, SURGICAL REMOVAL PARTIAL BONY IMPACTED'
        and or_procedure_name not like 'TOOTH/TEETH, SURGICAL REMOVAL, BONY IMPACTED'
        and or_procedure_name not like 'TOOTH/TEETH, SURGICAL REMOVAL,SOFT TISSUE IMPACTED'

        and or_stat.src_id = 2.0000 /*2: posted*/
        and or_perf.src_id = -2.0000 /*-2: not applicable*/
        and or_log.log_key != 0
        and or_event_type.src_id = 5 /*5: in room*/
),

surgeries_with_any_onco as (
    select
        log_key,
        1 as oncology_service_ind
    from
        {{ref('surgery_procedure')}}
    where
        service = 'Oncology'
    group by log_key
),

log_keys as (
    select
        anesthesia_encounter_link.or_log_key as log_key
    from
        {{ref ('stg_cancer_center_visit')}} as stg_cancer_center_visit
        inner join {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
            on stg_cancer_center_visit.pat_key = anesthesia_encounter_link.pat_key
    group by
        anesthesia_encounter_link.or_log_key
)

select
    surgery_encounter.pat_key,
    surgery_encounter.mrn,
    surgery_encounter.patient_name,
    or_procs.log_key,
    surgery_procedure.procedure_seq_num,
    surgery_encounter.service as procedure_service,
    surgery_procedure.service as surgeon_service,
    or_procs.event_in_dt,
    or_procs.event_out_dt,
    or_procs.or_proc_nm,
    /* INDICATORS AT PROCEDURE LEVEL */
    or_procs.tumor_mass_ind,
    or_procs.lesion_ind,
    or_procs.excision_ind,
    or_procs.resection_removal_ind,
    or_procs.ectomy_ind,
    or_procs.biopsy_ind
from
    log_keys
    inner join or_procs on or_procs.log_key = log_keys.log_key
    inner join {{ref('surgery_encounter')}} as surgery_encounter
        on surgery_encounter.log_key = or_procs.log_key
    left join {{ref ('surgery_procedure')}} as surgery_procedure
        on surgery_procedure.log_key = or_procs.log_key
            and surgery_procedure.procedure_seq_num = or_procs.procedure_seq_num
    left join surgeries_with_any_onco
        on surgeries_with_any_onco.log_key = or_procs.log_key
where
    procedure_service = 'Oncology'
    or surgeries_with_any_onco.oncology_service_ind = 1
