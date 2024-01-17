select
        cardiac_arrest_cohort_model.cicu_enc_key,
        cardiac_arrest_cohort_model.pat_key,
        flowsheet_measure.rec_dt
    from
        {{ref('cardiac_arrest_cohort_model')}} as cardiac_arrest_cohort_model
        inner join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
            on visit_stay_info.visit_key = cardiac_arrest_cohort_model.visit_key
        inner join {{source('cdw', 'flowsheet_record')}} as flowsheet_record
            on flowsheet_record.vsi_key = visit_stay_info.vsi_key
        inner join {{source('cdw', 'flowsheet_measure')}} as flowsheet_measure
            on flowsheet_measure.fs_rec_key = flowsheet_record.fs_rec_key
        inner join {{source('cdw', 'flowsheet')}} as flowsheet
            on flowsheet.fs_key = flowsheet_measure.fs_key
    where
        flowsheet.fs_id = 40060204
        and flowsheet_measure.rec_dt between cardiac_arrest_cohort_model.in_date
            and cardiac_arrest_cohort_model.out_date + cast('4 hours' as interval)
