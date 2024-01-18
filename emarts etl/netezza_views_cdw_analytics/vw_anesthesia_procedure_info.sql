with vkeys as (
    select
        ol.log_key,
        case
            when (vai.vsi_key notnull) then vai.vsi_key
            when (
                (ol.vsi_key <> 0)
                and (ol.vsi_key <> -1)
            ) then ol.vsi_key
            else null :: int8
        end as vkey
    from
        (
            (
                {{source('cdw', 'anesthesia_encounter_link')}} as anenc
                left join {{source('cdw', 'or_log')}} as ol on ((ol.log_key = anenc.or_log_key))
            )
            left join {{source('cdw', 'visit_addl_info')}} as vai on ((vai.visit_key = anenc.visit_key))
        )
    where
        (
            (ol.log_key <> 0)
            and (ol.log_key <> -1)
        )
),
height as (
    select
        anenc.or_log_key as log_key,
        vkeys.vkey as vsi_key,
        (
            "NUMERIC"(height2.meas_val, 2490386) * '2.54' :: numeric(3, 2)
        ) as height_cm,
        height2.rec_dt as height_recorded_tm,
        (
            date_part(
                'EPOCH' :: "VARCHAR",
                (anenc.anes_start_tm - height2.rec_dt)
            ) / 60
        ) as timediff_ht
    from
        (
            (
                (
                    {{source('cdw', 'anesthesia_encounter_link')}} as anenc
                    join vkeys on ((anenc.or_log_key = vkeys.log_key))
                )
                join {{source('cdw', 'flowsheet_record')}} as flrec on ((vkeys.vkey = flrec.vsi_key))
            )
            join (
                select
                    fs.fs_key,
                    fs.dict_val_type_key,
                    fs.dict_row_type_key,
                    fs.dim_intake_type_key,
                    fs.dim_output_type_key,
                    fs.fs_id,
                    fs.fs_nm,
                    fs.fs_desc,
                    fs.disp_nm,
                    fs.fs_unit,
                    fs.min_val,
                    fs.max_val,
                    fs.min_warn_val,
                    fs.max_warn_val,
                    fs.fs_frmla,
                    fs.create_dt,
                    fs.create_by,
                    fs.upd_dt,
                    fs.upd_by,
                    fsmeas.fs_rec_key,
                    fsmeas.seq_num,
                    fsmeas.fs_key,
                    fsmeas.fs_temp_key,
                    fsmeas.taken_emp_key,
                    fsmeas.entry_emp_key,
                    fsmeas.device_key,
                    fsmeas.occurance,
                    fsmeas.rec_dt,
                    fsmeas.entry_dt,
                    fsmeas.meas_val,
                    fsmeas.meas_val_num,
                    fsmeas.meas_cmt,
                    fsmeas.abnormal_ind,
                    fsmeas.create_dt,
                    fsmeas.create_by,
                    fsmeas.upd_dt,
                    fsmeas.upd_by,
                    fsmeas.md5
                from
                    (
                        {{source('cdw', 'flowsheet')}} as fs
                        join {{source('cdw', 'flowsheet_measure')}} as fsmeas on ((fs.fs_key = fsmeas.fs_key))
                    )
                where
                    (
                        (fs.fs_id = 11)
                        and (fsmeas.meas_val notnull)
                    )
            ) height2 on ((flrec.fs_rec_key = height2.fs_rec_key))
        )
    where
        (
            (
                (
                    date_part(
                        'EPOCH' :: "VARCHAR",
                        (anenc.anes_start_tm - height2.rec_dt)
                    ) / 60
                ) >= -120
            )
            and (
                (
                    date_part(
                        'EPOCH' :: "VARCHAR",
                        (anenc.anes_start_tm - height2.rec_dt)
                    ) / 60
                ) <= 7200
            )
        )
),
weight as (
    select
        distinct anenc.or_log_key as log_key,
        vkeys.vkey as vsi_key,
        (
            "NUMERIC"(weight2.meas_val, 2490386) * '0.028349523125' :: numeric(12, 12)
        ) as weight_kg,
        weight2.rec_dt as weight_recorded_tm,
        (
            date_part(
                'EPOCH' :: "VARCHAR",
                (anenc.anes_start_tm - weight2.rec_dt)
            ) / 60
        ) as timediff_wt
    from
        (
            (
                (
                    (
                        {{source('cdw', 'anesthesia_encounter_link')}} as anenc
                        join vkeys on ((anenc.or_log_key = vkeys.log_key))
                    )
                    join {{source('cdw', 'visit_addl_info')}} as vai on ((anenc.pat_key = vai.pat_key))
                )
                join {{source('cdw', 'flowsheet_record')}} as flrec on ((vai.vsi_key = flrec.vsi_key))
            )
            join (
                select
                    fs.fs_key,
                    fs.dict_val_type_key,
                    fs.dict_row_type_key,
                    fs.dim_intake_type_key,
                    fs.dim_output_type_key,
                    fs.fs_id,
                    fs.fs_nm,
                    fs.fs_desc,
                    fs.disp_nm,
                    fs.fs_unit,
                    fs.min_val,
                    fs.max_val,
                    fs.min_warn_val,
                    fs.max_warn_val,
                    fs.fs_frmla,
                    fs.create_dt,
                    fs.create_by,
                    fs.upd_dt,
                    fs.upd_by,
                    fsmeas.fs_rec_key,
                    fsmeas.seq_num,
                    fsmeas.fs_key,
                    fsmeas.fs_temp_key,
                    fsmeas.taken_emp_key,
                    fsmeas.entry_emp_key,
                    fsmeas.device_key,
                    fsmeas.occurance,
                    fsmeas.rec_dt,
                    fsmeas.entry_dt,
                    fsmeas.meas_val,
                    fsmeas.meas_val_num,
                    fsmeas.meas_cmt,
                    fsmeas.abnormal_ind,
                    fsmeas.create_dt,
                    fsmeas.create_by,
                    fsmeas.upd_dt,
                    fsmeas.upd_by,
                    fsmeas.md5
                from
                    (
                        {{source('cdw', 'flowsheet')}} as fs
                        join {{source('cdw', 'flowsheet_measure')}} as fsmeas on ((fs.fs_key = fsmeas.fs_key))
                    )
                where
                    (
                        (fs.fs_id = 14)
                        and (fsmeas.meas_val notnull)
                    )
            ) weight2 on ((flrec.fs_rec_key = weight2.fs_rec_key))
        )
    where
        (
            (
                (
                    date_part(
                        'EPOCH' :: "VARCHAR",
                        (anenc.anes_start_tm - weight2.rec_dt)
                    ) / 60
                ) >= -120
            )
            and (
                (
                    date_part(
                        'EPOCH' :: "VARCHAR",
                        (anenc.anes_start_tm - weight2.rec_dt)
                    ) / 60
                ) <= 2880
            )
        )
),
cr_epic as (
    select
        crhosp.cr_case_anes_key,
        null :: unknown as anes_id,
        crhosp.hosp_enc_csn,
        vinfo.vsi_key,
        vinfo.pat_key,
        crhosp.pat_mrn_id,
        crhosp.patient_name,
        crhosp.gender,
        crhosp.race,
        crhosp.ethnicity,
        crhosp.dob,
        crhosp.service_date,
        crhosp.age_in_days,
        crhosp.age_in_years,
        crhosp.height_cm,
        crhosp.weight_kg,
        vinfo.hosp_admit_dt,
        vinfo.hosp_disch_dt,
        crhosp.patient_class,
        crhosp.anes_proc_name,
        crhosp.primary_anes,
        crhosp.primary_surgeon,
        '' as surgical_service,
        crhosp.asa,
        crhosp.emerg_stat_ind,
        crhosp.anes_start_tm,
        crhosp.anes_end_tm,
        (
            date_part(
                'EPOCH' :: "VARCHAR",
                (crhosp.anes_end_tm - crhosp.anes_start_tm)
            ) / 60
        ) as anes_dur_min,
        crhosp.proc_start_tm,
        crhosp.proc_end_tm,
        (
            date_part(
                'EPOCH' :: "VARCHAR",
                (crhosp.proc_end_tm - crhosp.proc_start_tm)
            ) / 60
        ) as surgery_dur_min,
        crhosp.room_name,
        null :: unknown as department,
        'N/A' as epic_cancel_ind,
        'N/A' as epic_paper_ind,
        'N/A' as epic_standby_ind,
        'N/A' as valid_case_ind
    from
        (
            (
                select
                    cr_hosp_match.cr_case_anes_key,
                    case
                        when (cr_hosp_match.acct_enc notnull) then cr_hosp_match.acct_enc
                        when (cr_hosp_match.vai_enc notnull) then cr_hosp_match.vai_enc
                        else int8("VARCHAR"(cr_hosp_match.accountnumber))
                    end as hosp_enc_csn,
                    cr_hosp_match.pat_mrn_id,
                    cr_hosp_match.patient_name,
                    cr_hosp_match.gender,
                    cr_hosp_match.race,
                    cr_hosp_match.ethnicity,
                    cr_hosp_match.dob,
                    cr_hosp_match.service_date,
                    cr_hosp_match.age_in_days,
                    cr_hosp_match.age_in_years,
                    cr_hosp_match.height_cm,
                    cr_hosp_match.weight_kg,
                    cr_hosp_match.anes_proc_name,
                    cr_hosp_match.primary_anes,
                    cr_hosp_match.primary_surgeon,
                    cr_hosp_match.asa,
                    cr_hosp_match.emerg_stat_ind,
                    cr_hosp_match.anes_start_tm,
                    cr_hosp_match.patient_class,
                    cr_hosp_match.anes_end_tm,
                    cr_hosp_match.proc_start_tm,
                    cr_hosp_match.proc_end_tm,
                    cr_hosp_match.room_name
                from
                    (
                        select
                            rpt1.casenumber as cr_case_anes_key,
                            rpt1.accountnumber,
                            acct_nbr_vis.enc_id as acct_enc,
                            min(hosp_vis.enc_id) as vai_enc,
                            rpt1.medicalrecordnumber as pat_mrn_id,
                            rpt1.patientname as patient_name,
                            rpt1.gender,
                            race.dict_nm as race,
                            ethnic.dict_nm as ethnicity,
                            rpt1.birthdate as dob,
                            rpt1.servicedate as service_date,
                            date_part(
                                'DAY' :: "VARCHAR",
                                (rpt1.servicedate - rpt1.birthdate)
                            ) as age_in_days,
                            date_part(
                                'YEARS' :: "VARCHAR",
                                age(rpt1.servicedate, rpt1.birthdate)
                            ) as age_in_years,
                            rpt1.height as height_cm,
                            rpt1.weight as weight_kg,
                            case
                                when (btrim(rpt1.patientclass) = 'DM' :: "NVARCHAR") then 'Day MEDICINE' :: "NVARCHAR"
                                when (btrim(rpt1.patientclass) = 'DS' :: "NVARCHAR") then 'Day Surgery' :: "NVARCHAR"
                                when (btrim(rpt1.patientclass) = 'ED' :: "NVARCHAR") then 'EMERGENCY DEPARTMENT' :: "NVARCHAR"
                                when (btrim(rpt1.patientclass) = 'IP' :: "NVARCHAR") then 'Inpatient' :: "NVARCHAR"
                                when (btrim(rpt1.patientclass) = 'OP' :: "NVARCHAR") then 'Outpatient' :: "NVARCHAR"
                                when (btrim(rpt1.patientclass) = 'PAT' :: "NVARCHAR") then 'PATIENT ADMIT' :: "NVARCHAR"
                                when (btrim(rpt1.patientclass) = 'PTTH' :: "NVARCHAR") then 'PATIENT 23 HOURS' :: "NVARCHAR"
                                when (btrim(rpt1.patientclass) = 'TTH' :: "NVARCHAR") then '23 HOURS' :: "NVARCHAR"
                                when (btrim(rpt1.patientclass) = '' :: "NVARCHAR") then "NVARCHAR"(null :: "VARCHAR")
                                else rpt1.patientclass
                            end as patient_class,
                            rpt1.performedprocedure as anes_proc_name,
                            rpt1.attendinganes as primary_anes,
                            rpt1.primarysurgeon as primary_surgeon,
                            rpt1.asastatus as asa,
                            rpt1.emergency as emerg_stat_ind,
                            rpt3.anesthesiastart as anes_start_tm,
                            rpt3.anesthesiaend as anes_end_tm,
                            rpt3.procedurestart as proc_start_tm,
                            rpt3.procedureend as proc_end_tm,
                            rpt1.operatingroom as room_name
                        from
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    {{source('cdw', 'compurecord_report_001')}} as rpt1
                                                    left join {{source('cdw', 'compurecord_report_003')}} as rpt3 on ((rpt1.internalcaseid = rpt3.internalcaseid))
                                                )
                                                left join (
                                                    select
                                                        vai.pat_key,
                                                        vai.enc_id,
                                                        pat.pat_mrn_id,
                                                        vai.hosp_admit_dt,
                                                        vai.hosp_disch_dt
                                                    from
                                                        (
                                                            {{source('cdw', 'visit_addl_info')}} as vai
                                                            join {{source('cdw', 'patient')}} as pat on ((vai.pat_key = pat.pat_key))
                                                        )
                                                ) hosp_vis on (
                                                    (
                                                        (
                                                            (
                                                                rpt1.medicalrecordnumber = "NVARCHAR"(hosp_vis.pat_mrn_id)
                                                            )
                                                            and (rpt3.anesthesiastart >= hosp_vis.hosp_admit_dt)
                                                        )
                                                        and (rpt3.anesthesiaend <= hosp_vis.hosp_disch_dt)
                                                    )
                                                )
                                            )
                                            left join {{source('cdw', 'visit_addl_info')}} as acct_nbr_vis on (
                                                (
                                                    (
                                                        (
                                                            int8("VARCHAR"(rpt1.accountnumber)) = acct_nbr_vis.enc_id
                                                        )
                                                        and (
                                                            rpt3.anesthesiastart >= acct_nbr_vis.hosp_admit_dt
                                                        )
                                                    )
                                                    and (rpt3.anesthesiaend <= acct_nbr_vis.hosp_disch_dt)
                                                )
                                            )
                                        )
                                        left join {{source('cdw', 'patient')}} as pt on (
                                            (
                                                (
                                                    rpt1.medicalrecordnumber = "NVARCHAR"(pt.pat_mrn_id)
                                                )
                                                and (pt.pat_id <> 'Z1746000' :: "VARCHAR")
                                            )
                                        )
                                    )
                                    left join (
                                        select
                                            pr.pat_key,
                                            pr.seq_num,
                                            pr.dict_race_ethnic_key,
                                            pr.ethnic_ind,
                                            pr.race_ind,
                                            pr.create_dt,
                                            pr.create_by,
                                            pr.upd_dt,
                                            pr.upd_by,
                                            dictrace.dict_key,
                                            dictrace.dict_cat_key,
                                            dictrace.dict_cat_nm,
                                            dictrace.dict_nm,
                                            dictrace.dict_abbr,
                                            dictrace.create_dt,
                                            dictrace.create_by,
                                            dictrace.src_id
                                        from
                                            (
                                                {{source('cdw', 'patient_race_ethnicity')}} as pr
                                                join {{source('cdw', 'cdw_dictionary')}} as dictrace on ((pr.dict_race_ethnic_key = dictrace.dict_key))
                                            )
                                        where
                                            (
                                                (pr.race_ind = 1)
                                                and (pr.seq_num = 1)
                                            )
                                    ) race on ((pt.pat_key = race.pat_key))
                                )
                                left join (
                                    select
                                        pe.pat_key,
                                        pe.seq_num,
                                        pe.dict_race_ethnic_key,
                                        pe.ethnic_ind,
                                        pe.race_ind,
                                        pe.create_dt,
                                        pe.create_by,
                                        pe.upd_dt,
                                        pe.upd_by,
                                        dictethnic.dict_key,
                                        dictethnic.dict_cat_key,
                                        dictethnic.dict_cat_nm,
                                        dictethnic.dict_nm,
                                        dictethnic.dict_abbr,
                                        dictethnic.create_dt,
                                        dictethnic.create_by,
                                        dictethnic.src_id
                                    from
                                        (
                                            {{source('cdw', 'patient_race_ethnicity')}} as pe
                                            join {{source('cdw', 'cdw_dictionary')}} as dictethnic on ((pe.dict_race_ethnic_key = dictethnic.dict_key))
                                        )
                                    where
                                        (
                                            (pe.ethnic_ind = 1)
                                            and (pe.seq_num = 1)
                                        )
                                ) ethnic on ((pt.pat_key = ethnic.pat_key))
                            )
                        where
                            (
                                (rpt1.casenumber notnull)
                                and (
                                    rpt1.servicedate >= '2007-06-17 00:00:00' :: "TIMESTAMP"
                                )
                            )
                        group by
                            rpt1.casenumber,
                            rpt1.accountnumber,
                            acct_nbr_vis.enc_id,
                            rpt1.medicalrecordnumber,
                            rpt1.patientname,
                            rpt1.gender,
                            race.dict_nm,
                            ethnic.dict_nm,
                            rpt1.birthdate,
                            rpt1.servicedate,
                            rpt1.height,
                            rpt1.weight,
                            rpt1.performedprocedure,
                            rpt1.attendinganes,
                            rpt1.primarysurgeon,
                            rpt1.patientclass,
                            rpt1.asastatus,
                            rpt1.emergency,
                            rpt3.anesthesiastart,
                            rpt3.anesthesiaend,
                            rpt3.procedurestart,
                            rpt3.procedureend,
                            rpt1.operatingroom
                    ) cr_hosp_match
            ) crhosp
            left join {{source('cdw', 'visit_addl_info')}} as vinfo on ((crhosp.hosp_enc_csn = vinfo.enc_id))
        )
),
cr_height1 as (
    select
        cr_epic.cr_case_anes_key,
        (
            "NUMERIC"(cr_height.meas_val, 2490386) * '2.54' :: numeric(3, 2)
        ) as cr_height_cm,
        cr_height.rec_dt as height_recorded_tm,
        (
            date_part(
                'EPOCH' :: "VARCHAR",
                (cr_epic.anes_start_tm - cr_height.rec_dt)
            ) / 60
        ) as cr_timediff_ht
    from
        (
            (
                cr_epic
                join {{source('cdw', 'flowsheet_record')}} as flrec on ((cr_epic.vsi_key = flrec.vsi_key))
            )
            join (
                select
                    fs.fs_key,
                    fs.dict_val_type_key,
                    fs.dict_row_type_key,
                    fs.dim_intake_type_key,
                    fs.dim_output_type_key,
                    fs.fs_id,
                    fs.fs_nm,
                    fs.fs_desc,
                    fs.disp_nm,
                    fs.fs_unit,
                    fs.min_val,
                    fs.max_val,
                    fs.min_warn_val,
                    fs.max_warn_val,
                    fs.fs_frmla,
                    fs.create_dt,
                    fs.create_by,
                    fs.upd_dt,
                    fs.upd_by,
                    fsmeas.fs_rec_key,
                    fsmeas.seq_num,
                    fsmeas.fs_key,
                    fsmeas.fs_temp_key,
                    fsmeas.taken_emp_key,
                    fsmeas.entry_emp_key,
                    fsmeas.device_key,
                    fsmeas.occurance,
                    fsmeas.rec_dt,
                    fsmeas.entry_dt,
                    fsmeas.meas_val,
                    fsmeas.meas_val_num,
                    fsmeas.meas_cmt,
                    fsmeas.abnormal_ind,
                    fsmeas.create_dt,
                    fsmeas.create_by,
                    fsmeas.upd_dt,
                    fsmeas.upd_by,
                    fsmeas.md5
                from
                    (
                        {{source('cdw', 'flowsheet')}} as fs
                        join {{source('cdw', 'flowsheet_measure')}} as fsmeas on ((fs.fs_key = fsmeas.fs_key))
                    )
                where
                    (
                        (fs.fs_id = 11)
                        and (fsmeas.meas_val notnull)
                    )
            ) cr_height on ((flrec.fs_rec_key = cr_height.fs_rec_key))
        )
    where
        (
            (cr_epic.height_cm isnull)
            and (
                (
                    (
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (cr_epic.anes_start_tm - cr_height.rec_dt)
                        ) / 60
                    ) >= -120
                )
                and (
                    (
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (cr_epic.anes_start_tm - cr_height.rec_dt)
                        ) / 60
                    ) <= 7200
                )
            )
        )
),
cr_height2 as (
    select
        cr_height1.cr_case_anes_key,
        min(cr_height1.cr_timediff_ht) as cr_timediff_ht
    from
        cr_height1
    group by
        cr_height1.cr_case_anes_key
),
cr_weight1 as (
    select
        cr_epic.cr_case_anes_key,
        (
            "NUMERIC"(cr_weight.meas_val, 2490386) * '0.028349523125' :: numeric(12, 12)
        ) as cr_weight_kg,
        cr_weight.rec_dt as weight_recorded_tm,
        (
            date_part(
                'EPOCH' :: "VARCHAR",
                (cr_epic.anes_start_tm - cr_weight.rec_dt)
            ) / 60
        ) as cr_timediff_wt
    from
        (
            (
                (
                    cr_epic
                    join {{source('cdw', 'visit_addl_info')}} as vai on ((cr_epic.pat_key = vai.pat_key))
                )
                join {{source('cdw', 'flowsheet_record')}} as flrec on ((vai.vsi_key = flrec.vsi_key))
            )
            join (
                select
                    fs.fs_key,
                    fs.dict_val_type_key,
                    fs.dict_row_type_key,
                    fs.dim_intake_type_key,
                    fs.dim_output_type_key,
                    fs.fs_id,
                    fs.fs_nm,
                    fs.fs_desc,
                    fs.disp_nm,
                    fs.fs_unit,
                    fs.min_val,
                    fs.max_val,
                    fs.min_warn_val,
                    fs.max_warn_val,
                    fs.fs_frmla,
                    fs.create_dt,
                    fs.create_by,
                    fs.upd_dt,
                    fs.upd_by,
                    fsmeas.fs_rec_key,
                    fsmeas.seq_num,
                    fsmeas.fs_key,
                    fsmeas.fs_temp_key,
                    fsmeas.taken_emp_key,
                    fsmeas.entry_emp_key,
                    fsmeas.device_key,
                    fsmeas.occurance,
                    fsmeas.rec_dt,
                    fsmeas.entry_dt,
                    fsmeas.meas_val,
                    fsmeas.meas_val_num,
                    fsmeas.meas_cmt,
                    fsmeas.abnormal_ind,
                    fsmeas.create_dt,
                    fsmeas.create_by,
                    fsmeas.upd_dt,
                    fsmeas.upd_by,
                    fsmeas.md5
                from
                    (
                        {{source('cdw', 'flowsheet')}} as fs
                        join {{source('cdw', 'flowsheet_measure')}} as fsmeas on ((fs.fs_key = fsmeas.fs_key))
                    )
                where
                    (
                        (fs.fs_id = 14)
                        and (fsmeas.meas_val notnull)
                    )
            ) cr_weight on ((flrec.fs_rec_key = cr_weight.fs_rec_key))
        )
    where
        (
            (cr_epic.weight_kg isnull)
            and (
                (
                    (
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (cr_epic.anes_start_tm - cr_weight.rec_dt)
                        ) / 60
                    ) >= -120
                )
                and (
                    (
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (cr_epic.anes_start_tm - cr_weight.rec_dt)
                        ) / 60
                    ) <= 2880
                )
            )
        )
),
cr_weight2 as (
    select
        cr_weight1.cr_case_anes_key,
        min(cr_weight1.cr_timediff_wt) as cr_timediff_wt
    from
        cr_weight1
    group by
        cr_weight1.cr_case_anes_key
) (
    (
        select
            ("VARCHAR"(anes_case2.anes_key)) :: varchar(15) as cr_case_anes_key,
            anes_case2.anes_id,
            anes_case2.log_key,
            anes_case2.log_id,
            anes_case2.hosp_enc_csn,
            anes_case2.pat_mrn_id,
            anes_case2.patient_name,
            ("NVARCHAR"(anes_case2.gender)) :: nvarchar(16) as gender,
            anes_case2.race,
            anes_case2.ethnicity,
            anes_case2.dob,
            "TIMESTAMP"(anes_case2.service_dt) as service_dt,
            anes_case2.age_in_days,
            anes_case2.age_in_years,
            float8(height.height_cm) as height_cm,
            height.height_recorded_tm,
            float8(weight.weight_kg) as weight_kg,
            weight.weight_recorded_tm,
            case
                when (
                    (
                        (
                            (height.height_cm isnull)
                            or (height.height_cm = '0' :: numeric)
                        )
                        or (weight.weight_kg isnull)
                    )
                    or (weight.weight_kg = '0' :: numeric)
                ) then null :: float8
                else (
                    float8(weight.weight_kg) / (
                        float8((height.height_cm / '100' :: numeric(3, 0))) ^ 2
                    )
                )
            end as bmi,
            anes_case2.hosp_admit_dt,
            anes_case2.hosp_disch_dt,
            anes_case2.patient_class,
            anes_case2.anes_proc_name,
            anes_case2.anes as primary_anes,
            anes_case2.surgeon as primary_surgeon,
            anes_case2.surgical_service,
            anes_case2.asa_status,
            anes_case2.emerg_stat_ind,
            anes_case2.anes_start_tm,
            anes_case2.anes_end_tm,
            (
                date_part(
                    'EPOCH' :: "VARCHAR",
                    (
                        anes_case2.anes_end_tm - anes_case2.anes_start_tm
                    )
                ) / 60
            ) as anes_dur_min,
            anes_case2.proc_start_tm,
            anes_case2.proc_close_tm,
            (
                date_part(
                    'EPOCH' :: "VARCHAR",
                    (
                        anes_case2.proc_close_tm - anes_case2.proc_start_tm
                    )
                ) / 60
            ) as surgery_dur_min,
            anes_case2.room_name,
            anes_case2.department,
            anes_case2.epic_cancel_ind,
            anes_case2.epic_paper_ind,
            anes_case2.epic_standby_ind,
            case
                when (
                    (anes_case2.epic_paper_ind = 'YES' :: "VARCHAR")
                    or (anes_case2.anes_start_tm isnull)
                ) then 'NO' :: "VARCHAR"
                when (
                    (
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (
                                anes_case2.anes_end_tm - anes_case2.anes_start_tm
                            )
                        ) / 60
                    ) isnull
                ) then 'NO' :: "VARCHAR"
                else 'YES' :: "VARCHAR"
            end as valid_case_ind
        from
            (
                (
                    (
                        select
                            anes_case.log_id,
                            anes_case.log_key,
                            anes_case.anes_key,
                            anes_case.anes_id,
                            anes_case.vsi_key,
                            anes_case.hosp_enc_csn,
                            anes_case.emerg_stat_ind,
                            anes_case.service_dt,
                            anes_case.hosp_admit_dt,
                            anes_case.hosp_disch_dt,
                            anes_case.pat_mrn_id,
                            anes_case.patient_name,
                            anes_case.dob,
                            anes_case.gender,
                            anes_case.race,
                            anes_case.ethnicity,
                            anes_case.anes_proc_name,
                            anes_case.anes_start_tm,
                            anes_case.anes_end_tm,
                            anes_case.proc_start_tm,
                            anes_case.patient_class,
                            anes_case.proc_close_tm,
                            anes_case.surgeon,
                            anes_case.surgical_service,
                            anes_case.anes,
                            anes_case.asa_status,
                            anes_case.age_in_days,
                            anes_case.age_in_years,
                            min(anes_case.timediff_ht) as timediff_ht,
                            min(anes_case.timediff_wt) as timediff_wt,
                            anes_case.room_name,
                            anes_case.department,
                            anes_case.epic_cancel_ind,
                            anes_case.epic_paper_ind,
                            anes_case.epic_standby_ind
                        from
                            (
                                select
                                    ol.log_id,
                                    ol.log_key,
                                    anenc.anes_id,
                                    anenc.anes_key,
                                    vai.vsi_key,
                                    vai.enc_id as hosp_enc_csn,
                                    svc.dict_nm as surgical_service,
                                    vai.hosp_admit_dt,
                                    vai.hosp_disch_dt,
                                    case
                                        when (emergent.visit_key notnull) then 1
                                        else 0
                                    end as emerg_stat_ind,
                                    pat.pat_mrn_id,
                                    pat.full_nm as patient_name,
                                    pat.dob,
                                    pat.sex as gender,
                                    race.dict_nm as race,
                                    ethnic.dict_nm as ethnicity,
                                    md.full_dt as service_dt,
                                    anenc.anes_proc_name,
                                    anenc.anes_start_tm,
                                    anenc.anes_end_tm,
                                    ps.event_in_dt as proc_start_tm,
                                    pe.event_in_dt as proc_close_tm,
                                    surgeon.full_nm as surgeon,
                                    anes.full_nm as anes,
                                    pclass.dict_nm as patient_class,
                                    case
                                        when (asa.dict_nm = 'ASA I' :: "VARCHAR") then 1
                                        when (asa.dict_nm = 'ASA II' :: "VARCHAR") then 2
                                        when (asa.dict_nm = 'ASA III' :: "VARCHAR") then 3
                                        when (asa.dict_nm = 'ASA IV' :: "VARCHAR") then 4
                                        when (asa.dict_nm = 'ASA V' :: "VARCHAR") then 5
                                        when (asa.dict_nm = 'ASA VI' :: "VARCHAR") then 6
                                        else null :: int4
                                    end as asa_status,
                                    date_part(
                                        'DAY' :: "VARCHAR",
                                        ("TIMESTAMP"(md.full_dt) - pat.dob)
                                    ) as age_in_days,
                                    date_part(
                                        'YEARS' :: "VARCHAR",
                                        age("TIMESTAMP"(md.full_dt), pat.dob)
                                    ) as age_in_years,
                                    (
                                        date_part(
                                            'EPOCH' :: "VARCHAR",
                                            (anenc.anes_start_tm - height.rec_dt)
                                        ) / 60
                                    ) as timediff_ht,
                                    (
                                        date_part(
                                            'EPOCH' :: "VARCHAR",
                                            (anenc.anes_start_tm - weight.rec_dt)
                                        ) / 60
                                    ) as timediff_wt,
                                    rm.full_nm as room_name,
                                    dep.dept_nm as department,
                                    case
                                        when (cancel.visit_key notnull) then 'YES' :: "VARCHAR"
                                        else 'NO' :: "VARCHAR"
                                    end as epic_cancel_ind,
                                    case
                                        when (paper.visit_key notnull) then 'YES' :: "VARCHAR"
                                        else 'NO' :: "VARCHAR"
                                    end as epic_paper_ind,
                                    case
                                        when (standby.visit_key notnull) then 'YES' :: "VARCHAR"
                                        else 'NO' :: "VARCHAR"
                                    end as epic_standby_ind
                                from
                                    {{source('cdw', 'anesthesia_encounter_link')}} as anenc
                                    left join {{source('cdw', 'or_log')}} as ol on ((anenc.or_log_key = ol.log_key))
                                    left join {{source('cdw', 'or_case')}} as oc on ((ol.log_key = oc.log_key))
                                    left join {{source('cdw', 'cdw_dictionary')}} as svc on ((oc.dict_or_svc_key = svc.dict_key))
                                    left join {{source('cdw', 'visit')}} as visit on ((anenc.anes_visit_key = visit.visit_key))
                                    left join {{source('cdw', 'department')}} as dep on ((visit.dept_key = dep.dept_key))
                                    left join {{source('cdw', 'master_date')}} as md on ((anenc.anes_dt_key = md.dt_key))
                                    left join {{source('cdw', 'patient')}} as pat on ((anenc.pat_key = pat.pat_key))
                                    left join (
                                        select
                                            p_end.log_key,
                                            p_end.seq_num,
                                            p_end.dict_or_pat_event_key,
                                            p_end.event_in_dt,
                                            p_end.event_out_dt,
                                            p_end.event_tm_elaps_sec,
                                            p_end.dict_or_event_type_key,
                                            p_end.dict_or_pat_stat_key,
                                            p_end.track_stat_dt,
                                            p_end.create_by,
                                            p_end.create_dt,
                                            p_end.upd_by,
                                            p_end.upd_dt,
                                            procend.dict_key,
                                            procend.dict_cat_key,
                                            procend.dict_cat_nm,
                                            procend.dict_nm,
                                            procend.dict_abbr,
                                            procend.create_dt,
                                            procend.create_by,
                                            procend.src_id
                                        from
                                            (
                                                {{source('cdw', 'or_log_case_times')}} as p_end
                                                join {{source('cdw', 'cdw_dictionary')}} as procend on (
                                                    (
                                                        (p_end.dict_or_pat_event_key = procend.dict_key)
                                                        and (procend.src_id = '8' :: numeric(1, 0))
                                                    )
                                                )
                                            )
                                    ) pe on ((ol.log_key = pe.log_key))
                                    left join (
                                        select
                                            p_start.log_key,
                                            p_start.seq_num,
                                            p_start.dict_or_pat_event_key,
                                            p_start.event_in_dt,
                                            p_start.event_out_dt,
                                            p_start.event_tm_elaps_sec,
                                            p_start.dict_or_event_type_key,
                                            p_start.dict_or_pat_stat_key,
                                            p_start.track_stat_dt,
                                            p_start.create_by,
                                            p_start.create_dt,
                                            p_start.upd_by,
                                            p_start.upd_dt,
                                            procstart.dict_key,
                                            procstart.dict_cat_key,
                                            procstart.dict_cat_nm,
                                            procstart.dict_nm,
                                            procstart.dict_abbr,
                                            procstart.create_dt,
                                            procstart.create_by,
                                            procstart.src_id
                                        from
                                            (
                                                {{source('cdw', 'or_log_case_times')}} as p_start
                                                join {{source('cdw', 'cdw_dictionary')}} as procstart on (
                                                    (
                                                        (
                                                            p_start.dict_or_pat_event_key = procstart.dict_key
                                                        )
                                                        and (procstart.src_id = '7' :: numeric(1, 0))
                                                    )
                                                )
                                            )
                                    ) ps on ((ol.log_key = ps.log_key))
                                    left join (
                                        select
                                            surg.log_key,
                                            surg.seq_num,
                                            surg.surg_prov_key,
                                            surg.dict_or_role_key,
                                            surg.dict_or_svc_key,
                                            surg.start_dt,
                                            surg.end_dt,
                                            surg.tot_lgth_sec,
                                            surg.panel_num,
                                            surg.create_by,
                                            surg.create_dt,
                                            surg.upd_by,
                                            surg.upd_dt,
                                            prov.prov_key,
                                            prov.dict_modality_type_key,
                                            prov.dim_prov_lic_disp_key,
                                            prov.dim_prov_practice_key,
                                            prov.prov_id,
                                            prov.ext_id,
                                            prov.full_nm,
                                            prov.last_nm,
                                            prov.first_nm,
                                            prov.title,
                                            prov.prov_type,
                                            prov.rfl_src_type,
                                            prov.gl_prefix,
                                            prov.user_id,
                                            prov.epic_prov_id,
                                            prov.upin,
                                            prov.ssn,
                                            prov.emp_stat,
                                            prov.ext_nm,
                                            prov.active_stat,
                                            prov.email,
                                            prov.sex,
                                            prov.dob,
                                            prov.medicare_prov_id,
                                            prov.medicaid_prov_id,
                                            prov.base_cost,
                                            prov.npi,
                                            prov.rpt_grp_1,
                                            prov.rpt_grp_2,
                                            prov.rpt_grp_3,
                                            prov.rpt_grp_4,
                                            prov.rpt_grp_5,
                                            prov.rpt_grp_6,
                                            prov.rpt_grp_7,
                                            prov.rpt_grp_8,
                                            prov.rpt_grp_9,
                                            prov.rpt_grp_10,
                                            prov.hospitalist_ind,
                                            prov.create_dt,
                                            prov.create_by,
                                            prov.upd_dt,
                                            prov.upd_by,
                                            osdict.dict_key,
                                            osdict.dict_cat_key,
                                            osdict.dict_cat_nm,
                                            osdict.dict_nm,
                                            osdict.dict_abbr,
                                            osdict.create_dt,
                                            osdict.create_by,
                                            osdict.src_id
                                        from
                                            (
                                                (
                                                    {{source('cdw', 'or_log_surgeons')}} as surg
                                                    join {{source('cdw', 'provider')}} as prov on ((surg.surg_prov_key = prov.prov_key))
                                                )
                                                join {{source('cdw', 'cdw_dictionary')}} as osdict on ((surg.dict_or_role_key = osdict.dict_key))
                                            )
                                        where
                                            (
                                                (surg.panel_num = 1)
                                                and (osdict.src_id = '1' :: numeric(1, 0))
                                            )
                                    ) surgeon on ((ol.log_key = surgeon.log_key))
                                    left join {{source('cdw', 'provider')}} as anes on ((anenc.prov_key = anes.prov_key))
                                    left join {{source('cdw', 'cdw_dictionary')}} as asa on ((ol.dict_or_asa_rating_key = asa.dict_key))
                                    left join (
                                        select
                                            distinct ni.visit_key
                                        from
                                            (
                                                (
                                                    (
                                                        {{source('cdw', 'note_info')}} as ni
                                                        left join {{source('cdw', 'smart_data_element_info')}} as sdi on ((ni.note_id = sdi.rec_id_char))
                                                    )
                                                    left join {{source('cdw', 'smart_data_element_value')}} as sde on ((sde.sde_key = sdi.sde_key))
                                                )
                                                left join {{source('cdw', 'clinical_concept')}} as con on ((sdi.concept_key = con.concept_key))
                                            )
                                        where
                                            (
                                                (
                                                    con.concept_desc = 'WORKFLOW - ANESTHESIA - ASA STATUS - EMERGENT' :: "VARCHAR"
                                                )
                                                and (sde.elem_val = '1' :: "NVARCHAR")
                                            )
                                    ) emergent on (
                                        (anenc.anes_event_visit_key = emergent.visit_key)
                                    )
                                    left join {{source('cdw', 'cdw_dictionary')}} as pclass on ((ol.dict_pat_class_key = pclass.dict_key))
                                    left join vkeys on ((vkeys.log_key = ol.log_key))
                                    left join {{source('cdw', 'visit_addl_info')}} as vai on ((vai.vsi_key = vkeys.vkey))
                                    left join (
                                        select
                                            pr.pat_key,
                                            pr.seq_num,
                                            pr.dict_race_ethnic_key,
                                            pr.ethnic_ind,
                                            pr.race_ind,
                                            pr.create_dt,
                                            pr.create_by,
                                            pr.upd_dt,
                                            pr.upd_by,
                                            dictrace.dict_key,
                                            dictrace.dict_cat_key,
                                            dictrace.dict_cat_nm,
                                            dictrace.dict_nm,
                                            dictrace.dict_abbr,
                                            dictrace.create_dt,
                                            dictrace.create_by,
                                            dictrace.src_id
                                        from
                                            (
                                                {{source('cdw', 'patient_race_ethnicity')}} as pr
                                                join {{source('cdw', 'cdw_dictionary')}} as dictrace on ((pr.dict_race_ethnic_key = dictrace.dict_key))
                                            )
                                        where
                                            (
                                                (pr.race_ind = 1)
                                                and (pr.seq_num = 1)
                                            )
                                    ) race on ((anenc.pat_key = race.pat_key))
                                    left join (
                                        select
                                            pe.pat_key,
                                            pe.seq_num,
                                            pe.dict_race_ethnic_key,
                                            pe.ethnic_ind,
                                            pe.race_ind,
                                            pe.create_dt,
                                            pe.create_by,
                                            pe.upd_dt,
                                            pe.upd_by,
                                            dictethnic.dict_key,
                                            dictethnic.dict_cat_key,
                                            dictethnic.dict_cat_nm,
                                            dictethnic.dict_nm,
                                            dictethnic.dict_abbr,
                                            dictethnic.create_dt,
                                            dictethnic.create_by,
                                            dictethnic.src_id
                                        from
                                            (
                                                {{source('cdw', 'patient_race_ethnicity')}} as pe
                                                join {{source('cdw', 'cdw_dictionary')}} as dictethnic on ((pe.dict_race_ethnic_key = dictethnic.dict_key))
                                            )
                                        where
                                            (
                                                (pe.ethnic_ind = 1)
                                                and (pe.seq_num = 1)
                                            )
                                    ) ethnic on ((anenc.pat_key = ethnic.pat_key))
                                    left join {{source('cdw', 'flowsheet_record')}} as flrec on ((vkeys.vkey = flrec.vsi_key))
                                    left join (
                                        select
                                            fs.fs_key,
                                            fs.dict_val_type_key,
                                            fs.dict_row_type_key,
                                            fs.dim_intake_type_key,
                                            fs.dim_output_type_key,
                                            fs.fs_id,
                                            fs.fs_nm,
                                            fs.fs_desc,
                                            fs.disp_nm,
                                            fs.fs_unit,
                                            fs.min_val,
                                            fs.max_val,
                                            fs.min_warn_val,
                                            fs.max_warn_val,
                                            fs.fs_frmla,
                                            fs.create_dt,
                                            fs.create_by,
                                            fs.upd_dt,
                                            fs.upd_by,
                                            fsmeas.fs_rec_key,
                                            fsmeas.seq_num,
                                            fsmeas.fs_key,
                                            fsmeas.fs_temp_key,
                                            fsmeas.taken_emp_key,
                                            fsmeas.entry_emp_key,
                                            fsmeas.device_key,
                                            fsmeas.occurance,
                                            fsmeas.rec_dt,
                                            fsmeas.entry_dt,
                                            fsmeas.meas_val,
                                            fsmeas.meas_val_num,
                                            fsmeas.meas_cmt,
                                            fsmeas.abnormal_ind,
                                            fsmeas.create_dt,
                                            fsmeas.create_by,
                                            fsmeas.upd_dt,
                                            fsmeas.upd_by,
                                            fsmeas.md5
                                        from
                                            (
                                                {{source('cdw', 'flowsheet')}} as fs
                                                join {{source('cdw', 'flowsheet_measure')}} as fsmeas on ((fs.fs_key = fsmeas.fs_key))
                                            )
                                        where
                                            (
                                                (fs.fs_id = 11)
                                                and (fsmeas.meas_val notnull)
                                            )
                                    ) height on (
                                        (
                                            (flrec.fs_rec_key = height.fs_rec_key)
                                            and (
                                                (
                                                    (
                                                        date_part(
                                                            'EPOCH' :: "VARCHAR",
                                                            (anenc.anes_start_tm - height.rec_dt)
                                                        ) / 60
                                                    ) >= -120
                                                )
                                                and (
                                                    (
                                                        date_part(
                                                            'EPOCH' :: "VARCHAR",
                                                            (anenc.anes_start_tm - height.rec_dt)
                                                        ) / 60
                                                    ) <= 7200
                                                )
                                            )
                                        )
                                    )
                                    left join (
                                        select
                                            fs.fs_key,
                                            fs.dict_val_type_key,
                                            fs.dict_row_type_key,
                                            fs.dim_intake_type_key,
                                            fs.dim_output_type_key,
                                            fs.fs_id,
                                            fs.fs_nm,
                                            fs.fs_desc,
                                            fs.disp_nm,
                                            fs.fs_unit,
                                            fs.min_val,
                                            fs.max_val,
                                            fs.min_warn_val,
                                            fs.max_warn_val,
                                            fs.fs_frmla,
                                            fs.create_dt,
                                            fs.create_by,
                                            fs.upd_dt,
                                            fs.upd_by,
                                            fsmeas.fs_rec_key,
                                            fsmeas.seq_num,
                                            fsmeas.fs_key,
                                            fsmeas.fs_temp_key,
                                            fsmeas.taken_emp_key,
                                            fsmeas.entry_emp_key,
                                            fsmeas.device_key,
                                            fsmeas.occurance,
                                            fsmeas.rec_dt,
                                            fsmeas.entry_dt,
                                            fsmeas.meas_val,
                                            fsmeas.meas_val_num,
                                            fsmeas.meas_cmt,
                                            fsmeas.abnormal_ind,
                                            fsmeas.create_dt,
                                            fsmeas.create_by,
                                            fsmeas.upd_dt,
                                            fsmeas.upd_by,
                                            fsmeas.md5
                                        from
                                            (
                                                {{source('cdw', 'flowsheet')}} as fs
                                                join {{source('cdw', 'flowsheet_measure')}} as fsmeas on ((fs.fs_key = fsmeas.fs_key))
                                            )
                                        where
                                            (
                                                (fs.fs_id = 14)
                                                and (fsmeas.meas_val notnull)
                                            )
                                    ) weight on (
                                        (
                                            (flrec.fs_rec_key = weight.fs_rec_key)
                                            and (
                                                (
                                                    (
                                                        date_part(
                                                            'EPOCH' :: "VARCHAR",
                                                            (anenc.anes_start_tm - weight.rec_dt)
                                                        ) / 60
                                                    ) >= -120
                                                )
                                                and (
                                                    (
                                                        date_part(
                                                            'EPOCH' :: "VARCHAR",
                                                            (anenc.anes_start_tm - weight.rec_dt)
                                                        ) / 60
                                                    ) <= 2880
                                                )
                                            )
                                        )
                                    )
                                    left join {{source('cdw', 'provider')}} as rm on ((ol.room_prov_key = rm.prov_key))
                                    left join (
                                        select
                                            ev.visit_ed_event_key,
                                            ev.visit_key,
                                            ev.pat_key,
                                            ev.event_init_emp_key,
                                            ev.event_type_key,
                                            ev.pat_event_id,
                                            ev.seq_num,
                                            ev.event_dt,
                                            ev.event_rec_dt,
                                            ev.event_cmt,
                                            ev.event_stat,
                                            ev.create_dt,
                                            ev.create_by,
                                            ev.upd_dt,
                                            ev.upd_by,
                                            met.event_type_key,
                                            met.event_id,
                                            met.event_nm,
                                            met.event_disp_nm,
                                            met.event_desc,
                                            met.cdw_ind,
                                            met.create_dt,
                                            met.create_by,
                                            met.upd_dt,
                                            met.upd_by
                                        from
                                            (
                                                (
                                                    select
                                                        ev.visit_ed_event_key,
                                                        ev.visit_key,
                                                        ev.pat_key,
                                                        ev.event_init_emp_key,
                                                        ev.event_type_key,
                                                        ev.pat_event_id,
                                                        ev.seq_num,
                                                        ev.event_dt,
                                                        ev.event_rec_dt,
                                                        ev.event_cmt,
                                                        ev.event_stat,
                                                        ev.create_dt,
                                                        ev.create_by,
                                                        ev.upd_dt,
                                                        ev.upd_by
                                                    from
                                                        {{source('cdw', 'visit_ed_event')}} as ev
                                                    where
                                                        (ev.event_stat isnull)
                                                ) ev
                                                left join {{source('cdw', 'master_event_type')}} as met on ((ev.event_type_key = met.event_type_key))
                                            )
                                        where
                                            (
                                                met.event_id in (
                                                    '1120000058' :: int8,
                                                    '1120000057' :: int8,
                                                    '112000066' :: int8,
                                                    '1120000065' :: int8,
                                                    '100420' :: int8,
                                                    '100421' :: int8
                                                )
                                            )
                                    ) cancel on ((anenc.anes_visit_key = cancel.visit_key))
                                    left join (
                                        select
                                            ev.visit_ed_event_key,
                                            ev.visit_key,
                                            ev.pat_key,
                                            ev.event_init_emp_key,
                                            ev.event_type_key,
                                            ev.pat_event_id,
                                            ev.seq_num,
                                            ev.event_dt,
                                            ev.event_rec_dt,
                                            ev.event_cmt,
                                            ev.event_stat,
                                            ev.create_dt,
                                            ev.create_by,
                                            ev.upd_dt,
                                            ev.upd_by,
                                            met.event_type_key,
                                            met.event_id,
                                            met.event_nm,
                                            met.event_disp_nm,
                                            met.event_desc,
                                            met.cdw_ind,
                                            met.create_dt,
                                            met.create_by,
                                            met.upd_dt,
                                            met.upd_by
                                        from
                                            (
                                                (
                                                    select
                                                        ev.visit_ed_event_key,
                                                        ev.visit_key,
                                                        ev.pat_key,
                                                        ev.event_init_emp_key,
                                                        ev.event_type_key,
                                                        ev.pat_event_id,
                                                        ev.seq_num,
                                                        ev.event_dt,
                                                        ev.event_rec_dt,
                                                        ev.event_cmt,
                                                        ev.event_stat,
                                                        ev.create_dt,
                                                        ev.create_by,
                                                        ev.upd_dt,
                                                        ev.upd_by
                                                    from
                                                        {{source('cdw', 'visit_ed_event')}} as ev
                                                    where
                                                        (ev.event_stat isnull)
                                                ) ev
                                                left join {{source('cdw', 'master_event_type')}} as met on ((ev.event_type_key = met.event_type_key))
                                            )
                                        where
                                            (met.event_id in ('1120000056' :: int8))
                                    ) paper on ((anenc.anes_visit_key = paper.visit_key))
                                    left join (
                                        select
                                            distinct sdi.visit_key
                                        from
                                            (
                                                {{source('cdw', 'smart_data_element_info')}} as sdi
                                                join {{source('cdw', 'smart_data_element_value')}} as sde on ((sdi.sde_key = sde.sde_key))
                                            )
                                        where
                                            (
                                                sde.elem_val = 'Anesthesia Standby' :: "NVARCHAR"
                                            )
                                    ) standby on ((anenc.anes_event_visit_key = standby.visit_key))
                                where
                                    (
                                        (ol.log_key <> 0)
                                        and (ol.log_key <> -1)
                                    )
                            ) anes_case
                        group by
                            anes_case.log_id,
                            anes_case.log_key,
                            anes_case.anes_key,
                            anes_case.anes_id,
                            anes_case.vsi_key,
                            anes_case.hosp_enc_csn,
                            anes_case.emerg_stat_ind,
                            anes_case.service_dt,
                            anes_case.hosp_admit_dt,
                            anes_case.hosp_disch_dt,
                            anes_case.pat_mrn_id,
                            anes_case.patient_name,
                            anes_case.dob,
                            anes_case.gender,
                            anes_case.race,
                            anes_case.ethnicity,
                            anes_case.anes_proc_name,
                            anes_case.anes_start_tm,
                            anes_case.anes_end_tm,
                            anes_case.proc_start_tm,
                            anes_case.proc_close_tm,
                            anes_case.surgeon,
                            anes_case.surgical_service,
                            anes_case.anes,
                            anes_case.asa_status,
                            anes_case.age_in_days,
                            anes_case.age_in_years,
                            anes_case.patient_class,
                            anes_case.room_name,
                            anes_case.department,
                            anes_case.epic_cancel_ind,
                            anes_case.epic_paper_ind,
                            anes_case.epic_standby_ind
                    ) anes_case2
                    left join height on (
                        (
                            (anes_case2.log_key = height.log_key)
                            and (anes_case2.timediff_ht = height.timediff_ht)
                        )
                    )
                )
                left join (
                    select
                        weight.log_key,
                        min(weight.weight_recorded_tm) as weight_recorded_tm,
                        min(weight.weight_kg) as weight_kg,
                        weight.timediff_wt
                    from
                        weight
                    group by
                        weight.log_key,
                        weight.timediff_wt
                ) weight on (
                    (
                        (anes_case2.log_key = weight.log_key)
                        and (anes_case2.timediff_wt = weight.timediff_wt)
                    )
                )
            )
    )
    union
    all (
        select
            distinct (cr_epic2.cr_case_anes_key) :: nvarchar(15) as cr_case_anes_key,
            null :: int8 as anes_id,
            null :: int8 as log_key,
            (cr_epic2.cr_case_anes_key) :: nvarchar(50) as log_id,
            cr_epic2.hosp_enc_csn,
            (cr_epic2.pat_mrn_id) :: nvarchar(255) as pat_mrn_id,
            (cr_epic2.patient_name) :: nvarchar(200) as patient_name,
            cr_epic2.gender,
            cr_epic2.race,
            cr_epic2.ethnicity,
            cr_epic2.dob,
            cr_epic2.service_date as service_dt,
            cr_epic2.age_in_days,
            cr_epic2.age_in_years,
            cr_epic2.height_cm,
            cr_epic2.height_recorded_tm,
            cr_epic2.weight_kg,
            cr_epic2.weight_recorded_tm,
            case
                when (
                    (
                        (
                            (cr_epic2.height_cm isnull)
                            or (cr_epic2.height_cm = 0)
                        )
                        or (cr_epic2.weight_kg isnull)
                    )
                    or (cr_epic2.weight_kg = 0)
                ) then null :: float8
                else (
                    cr_epic2.weight_kg / ((cr_epic2.height_cm / 100) ^ 2)
                )
            end as bmi,
            cr_epic2.hosp_admit_dt,
            cr_epic2.hosp_disch_dt,
            (cr_epic2.patient_class) :: nvarchar(500) as patient_class,
            (cr_epic2.anes_proc_name) :: nvarchar(500) as anes_proc_name,
            (cr_epic2.primary_anes) :: nvarchar(200) as primary_anes,
            (cr_epic2.primary_surgeon) :: nvarchar(200) as primary_surgeon,
            ('' :: "VARCHAR") :: varchar(500) as surgical_service,
            cr_epic2.asa as asa_status,
            int4(cr_epic2.emerg_stat_ind) as emerg_stat_ind,
            cr_epic2.anes_start_tm,
            cr_epic2.anes_end_tm,
            cr_epic2.anes_dur_min,
            cr_epic2.proc_start_tm,
            cr_epic2.proc_end_tm as proc_close_tm,
            cr_epic2.surgery_dur_min,
            (cr_epic2.room_name) :: nvarchar(200) as room_name,
            (null :: "VARCHAR") :: varchar(300) as department,
            ('N/A' :: "VARCHAR") :: varchar(3) as epic_cancel_ind,
            ('N/A' :: "VARCHAR") :: varchar(3) as epic_paper_ind,
            ('N/A' :: "VARCHAR") :: varchar(3) as epic_standby_ind,
            ('N/A' :: "VARCHAR") :: varchar(3) as valid_case_ind
        from
            (
                select
                    cr_epic.cr_case_anes_key,
                    cr_epic.hosp_enc_csn,
                    cr_epic.pat_mrn_id,
                    cr_epic.patient_name,
                    cr_epic.gender,
                    cr_epic.race,
                    cr_epic.ethnicity,
                    cr_epic.dob,
                    cr_epic.service_date,
                    cr_epic.age_in_days,
                    cr_epic.age_in_years,
                    case
                        when (cr_epic.height_cm notnull) then float8(cr_epic.height_cm)
                        when (ht.height_cm notnull) then float8(ht.height_cm)
                        else null :: float8
                    end as height_cm,
                    ht.height_recorded_tm,
                    case
                        when (cr_epic.weight_kg notnull) then float8(cr_epic.weight_kg)
                        when (wt.weight_kg notnull) then float8(wt.weight_kg)
                        else null :: float8
                    end as weight_kg,
                    wt.weight_recorded_tm,
                    cr_epic.hosp_admit_dt,
                    cr_epic.hosp_disch_dt,
                    cr_epic.patient_class,
                    cr_epic.anes_proc_name,
                    cr_epic.primary_anes,
                    cr_epic.primary_surgeon,
                    cr_epic.asa,
                    cr_epic.emerg_stat_ind,
                    cr_epic.anes_start_tm,
                    cr_epic.anes_end_tm,
                    cr_epic.anes_dur_min,
                    cr_epic.proc_start_tm,
                    cr_epic.proc_end_tm,
                    cr_epic.surgery_dur_min,
                    cr_epic.room_name
                from
                    (
                        (
                            cr_epic
                            left join (
                                select
                                    ht1.cr_case_anes_key,
                                    min(ht1.cr_height_cm) as height_cm,
                                    ht1.height_recorded_tm
                                from
                                    (
                                        cr_height1 ht1
                                        join cr_height2 ht2 on (
                                            (
                                                (ht1.cr_case_anes_key = ht2.cr_case_anes_key)
                                                and (ht1.cr_timediff_ht = ht2.cr_timediff_ht)
                                            )
                                        )
                                    )
                                group by
                                    ht1.cr_case_anes_key,
                                    ht1.height_recorded_tm
                            ) ht on ((cr_epic.cr_case_anes_key = ht.cr_case_anes_key))
                        )
                        left join (
                            select
                                wt1.cr_case_anes_key,
                                min(wt1.cr_weight_kg) as weight_kg,
                                wt1.weight_recorded_tm
                            from
                                (
                                    cr_weight1 wt1
                                    join cr_weight2 wt2 on (
                                        (
                                            (wt1.cr_case_anes_key = wt2.cr_case_anes_key)
                                            and (wt1.cr_timediff_wt = wt2.cr_timediff_wt)
                                        )
                                    )
                                )
                            group by
                                wt1.cr_case_anes_key,
                                wt1.weight_recorded_tm
                        ) wt on ((cr_epic.cr_case_anes_key = wt.cr_case_anes_key))
                    )
            ) cr_epic2
    )
)
union
all (
    select
        (rpt1.casenumber) :: nvarchar(15) as cr_case_anes_key,
        null :: int8 as anes_id,
        null :: int8 as log_key,
        (rpt1.casenumber) :: nvarchar(50) as log_id,
        null :: int8 as hosp_enc_csn,
        (rpt1.medicalrecordnumber) :: nvarchar(255) as pat_mrn_id,
        pat.full_nm as patient_name,
        rpt1.gender,
        race.dict_nm as race,
        ethnic.dict_nm as ethnicity,
        rpt1.birthdate as dob,
        rpt1.servicedate as service_dt,
        date_part(
            'DAY' :: "VARCHAR",
            (rpt1.servicedate - rpt1.birthdate)
        ) as age_in_days,
        date_part(
            'YEARS' :: "VARCHAR",
            age(rpt1.servicedate, rpt1.birthdate)
        ) as age_in_years,
        float8(rpt1.height) as height_cm,
        null :: "TIMESTAMP" as height_recorded_tm,
        float8(rpt1.weight) as weight_kg,
        null :: "TIMESTAMP" as weight_recorded_tm,
        case
            when (
                (
                    (
                        (rpt1.height isnull)
                        or (rpt1.height = 0)
                    )
                    or (rpt1.weight isnull)
                )
                or (rpt1.weight = 0)
            ) then null :: float8
            else (rpt1.weight / ((rpt1.height / 100) ^ 2))
        end as bmi,
        null :: "TIMESTAMP" as hosp_admit_dt,
        null :: "TIMESTAMP" as hosp_disch_dt,
        (
            case
                when (btrim(rpt1.patientclass) = 'DM' :: "NVARCHAR") then 'Day MEDICINE' :: "NVARCHAR"
                when (btrim(rpt1.patientclass) = 'DS' :: "NVARCHAR") then 'Day Surgery' :: "NVARCHAR"
                when (btrim(rpt1.patientclass) = 'ED' :: "NVARCHAR") then 'EMERGENCY DEPARTMENT' :: "NVARCHAR"
                when (btrim(rpt1.patientclass) = 'IP' :: "NVARCHAR") then 'Inpatient' :: "NVARCHAR"
                when (btrim(rpt1.patientclass) = 'OP' :: "NVARCHAR") then 'Outpatient' :: "NVARCHAR"
                when (btrim(rpt1.patientclass) = 'PAT' :: "NVARCHAR") then 'PATIENT ADMIT' :: "NVARCHAR"
                when (btrim(rpt1.patientclass) = 'PTTH' :: "NVARCHAR") then 'PATIENT 23 HOURS' :: "NVARCHAR"
                when (btrim(rpt1.patientclass) = 'TTH' :: "NVARCHAR") then '23 HOURS' :: "NVARCHAR"
                when (btrim(rpt1.patientclass) = '' :: "NVARCHAR") then "NVARCHAR"(null :: "VARCHAR")
                else rpt1.patientclass
            end
        ) :: nvarchar(500) as patient_class,
        (rpt1.performedprocedure) :: nvarchar(500) as anes_proc_name,
        (rpt1.attendinganes) :: nvarchar(200) as primary_anes,
        (rpt1.primarysurgeon) :: nvarchar(200) as primary_surgeon,
        ('' :: "VARCHAR") :: varchar(500) as surgical_service,
        rpt1.asastatus as asa_status,
        int4(rpt1.emergency) as emerg_stat_ind,
        rpt3.anesthesiastart as anes_start_tm,
        rpt3.anesthesiaend as anes_end_tm,
        (
            date_part(
                'EPOCH' :: "VARCHAR",
                (rpt3.anesthesiaend - rpt3.anesthesiastart)
            ) / 60
        ) as anes_dur_min,
        rpt3.procedurestart as proc_start_tm,
        rpt3.procedureend as proc_close_tm,
        (
            date_part(
                'EPOCH' :: "VARCHAR",
                (rpt3.procedureend - rpt3.procedurestart)
            ) / 60
        ) as surgery_dur_min,
        (rpt1.operatingroom) :: nvarchar(200) as room_name,
        (null :: "VARCHAR") :: varchar(300) as department,
        ('N/A' :: "VARCHAR") :: varchar(3) as epic_cancel_ind,
        ('N/A' :: "VARCHAR") :: varchar(3) as epic_paper_ind,
        ('N/A' :: "VARCHAR") :: varchar(3) as epic_standby_ind,
        ('N/A' :: "VARCHAR") :: varchar(3) as valid_case_ind
    from
        (
            (
                (
                    (
                        {{source('cdw', 'compurecord_report_001')}} as rpt1
                        left join {{source('cdw', 'compurecord_report_003')}} as rpt3 on ((rpt1.internalcaseid = rpt3.internalcaseid))
                    )
                    left join (
                        select
                            patient.pat_key,
                            patient.prov_key,
                            patient.loc_key,
                            patient.geo_key,
                            patient.pat_id,
                            patient.full_nm,
                            patient.last_nm,
                            patient.first_nm,
                            patient.middle_nm,
                            patient.addr_line1,
                            patient.addr_line2,
                            patient.city,
                            patient."STATE",
                            patient.county,
                            patient.country,
                            patient.zip,
                            patient.home_ph,
                            patient.work_ph,
                            patient.email_addr,
                            patient.restricted_ind,
                            patient.epiccare_pat_ind,
                            patient.pat_stat,
                            patient.dob,
                            patient.sex,
                            patient.ethnic_grp,
                            patient.marital_stat,
                            patient.religion,
                            patient.lang,
                            patient.ssn,
                            patient.reg_dt,
                            patient.reg_stat,
                            patient.mother_pat_id,
                            patient.father_pat_id,
                            patient.pat_mrn_id,
                            patient.death_dt,
                            patient.ped_birth_len_in_inches,
                            patient.ped_birth_len_in_cm,
                            patient.ped_birth_wt_in_oz,
                            patient.ped_birth_wt_in_kg,
                            patient.ped_gest_age,
                            patient.rec_state,
                            patient.interpreter_needed_ind,
                            patient.send_text_ind,
                            patient.test_pat_ind,
                            patient.cur_rec_ind,
                            patient.create_dt,
                            patient.create_by,
                            patient.upd_dt,
                            patient.upd_by
                        from
                            {{source('cdw', 'patient')}} as patient
                        where
                            (patient.pat_key <> 8934299)
                    ) pat on (
                        (
                            rpt1.medicalrecordnumber = "NVARCHAR"(pat.pat_mrn_id)
                        )
                    )
                )
                left join (
                    select
                        dictrace.dict_nm,
                        pr.pat_key
                    from
                        (
                            {{source('cdw', 'patient_race_ethnicity')}} as pr
                            join {{source('cdw', 'cdw_dictionary')}} as dictrace on ((pr.dict_race_ethnic_key = dictrace.dict_key))
                        )
                    where
                        (
                            (pr.race_ind = 1)
                            and (pr.seq_num = 1)
                        )
                ) race on ((pat.pat_key = race.pat_key))
            )
            left join (
                select
                    dictethnic.dict_nm,
                    pe.pat_key
                from
                    (
                        {{source('cdw', 'patient_race_ethnicity')}} as pe
                        join {{source('cdw', 'cdw_dictionary')}} as dictethnic on ((pe.dict_race_ethnic_key = dictethnic.dict_key))
                    )
                where
                    (
                        (pe.ethnic_ind = 1)
                        and (pe.seq_num = 1)
                    )
            ) ethnic on ((pat.pat_key = ethnic.pat_key))
        )
    where
        (
            (rpt1.casenumber notnull)
            and (
                rpt1.servicedate < '2007-06-17 00:00:00' :: "TIMESTAMP"
            )
        )
)