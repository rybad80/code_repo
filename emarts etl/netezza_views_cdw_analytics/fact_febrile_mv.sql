select
    t.pat_key,
    t.visit_key,
    t.age_days,
    case
        when (t.age_days <= 28) then 1
        when (t.age_days > 28) then 0
        else null :: int4
    end as less_29_days_ind,
    case
        when (lp.lp_location_key notnull) then lp.lp_location_key
        when (3 notnull) then 3
        else null :: int4
    end as lp_location_key,
    case
        when (
            (
                date_part(
                    'EPOCH' :: "VARCHAR",
                    (anti.anti_dt - t.ed_arrival)
                ) * 60
            ) < 1
        ) then null :: int8
        else (
            date_part(
                'EPOCH' :: "VARCHAR",
                (anti.anti_dt - t.ed_arrival)
            ) * 60
        )
    end as arrive_to_anti_min,
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (uc.uc_specimen_dt - t.ed_arrival)
        ) * 60
    ) as arrive_to_uc_min,
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (lp.lp_taken_dt - t.ed_arrival)
        ) * 60
    ) as arrive_to_lp_min,
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (md.md_eval_dt - t.ed_arrival)
        ) * 60
    ) as arrive_to_eval_min,
    t.discharge_dt,
    to_number(
        to_char(t.discharge_dt, 'YYYYMMDD' :: "VARCHAR"),
        '99999999' :: "VARCHAR"
    ) as discharge_dt_key,
    case
        when (
            "NUMERIC"(t.readmit_ip_days) <= '7' :: numeric(1, 0)
        ) then 1
        else 0
    end as readmit_ed_7_days_ip,
    case
        when (
            "NUMERIC"(t.readmit_ed_hours) <= '72' :: numeric(2, 0)
        ) then 1
        else 0
    end as readmit_ed_72_hours_ed,
    case
        when (
            (t.age_days < 22)
            and (meds.med_total = '7' :: numeric(1, 0))
        ) then 1
        when (
            (t.age_days < 22)
            and (meds.med_total <> '7' :: numeric(1, 0))
        ) then 0
        when (
            (
                (t.age_days >= 22)
                and (t.age_days <= 28)
            )
            and (meds.med_total > '5' :: numeric(1, 0))
        ) then 1
        when (
            (
                (t.age_days >= 22)
                and (t.age_days <= 28)
            )
            and (meds.med_total < '6' :: numeric(1, 0))
        ) then 0
        when (
            (t.age_days > 28)
            and (meds.med_total > '3' :: numeric(1, 0))
        ) then 1
        when (
            (t.age_days > 28)
            and (meds.med_total < '4' :: numeric(1, 0))
        ) then 0
        else 0
    end as appr_medication,
    case
        when (t.age_days < 22) then 0
        when (
            (t.age_days >= 22)
            and (t.age_days <= 28)
        ) then 1
        when (t.age_days > 28) then 2
        else null :: int4
    end as age_group_key,
    case
        when (
            case
                when (t.ed_depart = t.discharge_dt) then null :: "NUMERIC"
                else round(
                    "NUMERIC"(
                        date_part(
                            'HOURS' :: "VARCHAR",
                            (t.discharge_dt - t.ed_depart)
                        )
                    ),
                    2
                )
            end notnull
        ) then case
            when (t.ed_depart = t.discharge_dt) then null :: "NUMERIC"
            else round(
                "NUMERIC"(
                    date_part(
                        'HOURS' :: "VARCHAR",
                        (t.discharge_dt - t.ed_depart)
                    )
                ),
                2
            )
        end
        when (0 notnull) then '0' :: numeric
        else null :: "NUMERIC"
    end as ip_los_hrs,
    case
        when (prv.ed_prov_key notnull) then prv.ed_prov_key
        when (0 notnull) then '0' :: int8
        else null :: int8
    end as ed_prov_key,
    case
        when (t.discharge_dt = t.ed_depart) then '0' :: int8
        else case
            when (ipprv.ip_prov_key notnull) then ipprv.ip_prov_key
            when (0 notnull) then '0' :: int8
            else null :: int8
        end
    end as ip_prov_key
from
    (
        (
            (
                (
                    (
                        (
                            (
                                (
                                    select
                                        a.pat_key,
                                        a.visit_key,
                                        a.age_days,
                                        a.ed_arrival,
                                        a.ed_depart,
                                        a.discharge_dt,
                                        a.return_dt,
                                        a.return_diag,
                                        case
                                            when (a.ed_depart = a.discharge_dt) then null :: int8
                                            when (a.return_dt notnull) then date_part(
                                                'DAYS' :: "VARCHAR",
                                                (a.return_dt - a.discharge_dt)
                                            )
                                            else null :: int8
                                        end as readmit_ip_days,
                                        case
                                            when (
                                                (a.return_dt notnull)
                                                and (a.ed_depart = a.discharge_dt)
                                            ) then date_part(
                                                'HOURS' :: "VARCHAR",
                                                (a.return_dt - a.discharge_dt)
                                            )
                                            else null :: int8
                                        end as readmit_ed_hours
                                    from
                                        (
                                            select
                                                distinct "*RSS*"."#PAT_MRN_ID#0XF5668450" as pat_mrn_id,
                                                "*RSS*"."#ENC_ID#0XF5668554" as enc_id,
                                                "*RSS*"."#PAT_KEY#0XF5668658" as pat_key,
                                                "*RSS*"."#VISIT_KEY#0XF566875C" as visit_key,
                                                "*RSS*"."#FULL_NM#0XF5668860" as full_nm,
                                                "*RSS*"."#DOB#0XF5668964" as date_of_birth,
                                                "*RSS*"."#DATE_PART#0XF5668A68" as age_days,
                                                "*RSS*"."#TRI_DIAG_1#0XF5668D10" as tri_diag_1,
                                                "*RSS*"."#TRI_DIAG_2#0XF5668E14" as tri_diag_2,
                                                "*RSS*"."#TRI_DIAG_3#0XF5668F18" as tri_diag_3,
                                                "*RSS*"."#TRI_DIAG_4#0XF566901C" as tri_diag_4,
                                                "*RSS*"."#?COLUMN?#0XF5669120" as feb_ord_set,
                                                "*RSS*"."#ADT_ARRVL_DT#0XF5669360" as ed_arrival,
                                                "*RSS*"."#ED_DPART_DT#0XF5669464" as ed_depart,
                                                "*RSS*"."#HOSP_DISCHRG_DT#0XF5669568" as discharge_dt,
                                                lead("*RSS*"."#ADT_ARRVL_DT#0XF5669360", 1) over (
                                                    partition by "*RSS*"."#PAT_KEY#0XF5668658"
                                                    order by
                                                        "*RSS*"."#ADT_ARRVL_DT#0XF5669360"
                                                ) as return_dt,
                                                lead("*RSS*"."#TRI_DIAG_1#0XF5668D10", 1) over (
                                                    partition by "*RSS*"."#PAT_KEY#0XF5668658"
                                                    order by
                                                        "*RSS*"."#ADT_ARRVL_DT#0XF5669360"
                                                ) as return_diag,
                                                dense_rank() over (
                                                    partition by "*RSS*"."#PAT_KEY#0XF5668658"
                                                    order by
                                                        "*RSS*"."#ADT_ARRVL_DT#0XF5669360"
                                                ) as visit_number
                                            from
                                                (
                                                    select
                                                        p.pat_mrn_id as "#PAT_MRN_ID#0XF5668450",
                                                        v.enc_id as "#ENC_ID#0XF5668554",
                                                        v.pat_key as "#PAT_KEY#0XF5668658",
                                                        v.visit_key as "#VISIT_KEY#0XF566875C",
                                                        p.full_nm as "#FULL_NM#0XF5668860",
                                                        p.dob as "#DOB#0XF5668964",
                                                        t.tri_diag_1 as "#TRI_DIAG_1#0XF5668D10",
                                                        t.tri_diag_2 as "#TRI_DIAG_2#0XF5668E14",
                                                        t.tri_diag_3 as "#TRI_DIAG_3#0XF5668F18",
                                                        t.tri_diag_4 as "#TRI_DIAG_4#0XF566901C",
                                                        v.adt_arrvl_dt as "#ADT_ARRVL_DT#0XF5669360",
                                                        v.ed_dpart_dt as "#ED_DPART_DT#0XF5669464",
                                                        visit.hosp_dischrg_dt as "#HOSP_DISCHRG_DT#0XF5669568",
                                                        date_part('DAY' :: "VARCHAR", (v.adt_arrvl_dt - p.dob)) as "#DATE_PART#0XF5668A68",
                                                        case
                                                            when (q.visit_key notnull) then 'Y' :: "VARCHAR"
                                                            else null :: "VARCHAR"
                                                        end as "#?COLUMN?#0XF5669120"
                                                    from
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            (
                                                                                {{source('cdw', 'visit_addl_info')}} as v
                                                                                join {{source('cdw', 'patient')}} as p on ((v.pat_key = p.pat_key))
                                                                            )
                                                                            join {{source('cdw', 'visit_reason')}} as r on ((v.visit_key = r.visit_key))
                                                                        )
                                                                        join {{source('cdw', 'visit')}} as visit on ((v.visit_key = visit.visit_key))
                                                                    )
                                                                    join {{source('cdw', 'master_reason_for_visit')}} as d on ((r.rsn_key = d.rsn_key))
                                                                )
                                                                join (
                                                                    select
                                                                        t.visit_key,
                                                                        min(
                                                                            case
                                                                                when (t.seq_num = 1) then t.rsn_nm
                                                                                else null :: "VARCHAR"
                                                                            end
                                                                        ) over (
                                                                            partition by t.visit_key rows between unbounded preceding
                                                                            and unbounded following
                                                                        ) as tri_diag_1,
                                                                        min(
                                                                            case
                                                                                when (t.seq_num = 2) then t.rsn_nm
                                                                                else null :: "VARCHAR"
                                                                            end
                                                                        ) over (
                                                                            partition by t.visit_key rows between unbounded preceding
                                                                            and unbounded following
                                                                        ) as tri_diag_2,
                                                                        min(
                                                                            case
                                                                                when (t.seq_num = 3) then t.rsn_nm
                                                                                else null :: "VARCHAR"
                                                                            end
                                                                        ) over (
                                                                            partition by t.visit_key rows between unbounded preceding
                                                                            and unbounded following
                                                                        ) as tri_diag_3,
                                                                        min(
                                                                            case
                                                                                when (t.seq_num = 4) then t.rsn_nm
                                                                                else null :: "VARCHAR"
                                                                            end
                                                                        ) over (
                                                                            partition by t.visit_key rows between unbounded preceding
                                                                            and unbounded following
                                                                        ) as tri_diag_4
                                                                    from
                                                                        (
                                                                            select
                                                                                v.visit_key,
                                                                                v.rsn_key,
                                                                                t.rsn_nm,
                                                                                v.seq_num
                                                                            from
                                                                                (
                                                                                    {{source('cdw', 'visit_reason')}} as v
                                                                                    join {{source('cdw', 'master_reason_for_visit')}} as t on ((v.rsn_key = t.rsn_key))
                                                                                )
                                                                        ) t
                                                                ) t on ((v.visit_key = t.visit_key))
                                                            )
                                                            left join (
                                                                select
                                                                    distinct p.visit_key
                                                                from
                                                                    {{source('cdw', 'procedure_order')}} as p
                                                                where
                                                                    (
                                                                        p.ptcl_key in ('97918' :: int8, '98021' :: int8, '97269' :: int8)
                                                                    )
                                                            ) q on ((v.visit_key = q.visit_key))
                                                        )
                                                    where
                                                        (
                                                            (
                                                                (
                                                                    v.adt_arrvl_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                                                )
                                                                and (
                                                                    d.rsn_id in (
                                                                        '3019' :: int8,
                                                                        '3049' :: int8,
                                                                        '3050' :: int8,
                                                                        '47' :: int8
                                                                    )
                                                                )
                                                            )
                                                            and (
                                                                date_part('DAY' :: "VARCHAR", (v.adt_arrvl_dt - p.dob)) < 57
                                                            )
                                                        )
                                                    group by
                                                        p.pat_mrn_id,
                                                        v.enc_id,
                                                        v.pat_key,
                                                        v.visit_key,
                                                        p.full_nm,
                                                        p.dob,
                                                        v.adt_arrvl_dt,
                                                        v.ed_dpart_dt,
                                                        visit.hosp_dischrg_dt,
                                                        t.tri_diag_1,
                                                        t.tri_diag_2,
                                                        t.tri_diag_3,
                                                        t.tri_diag_4,
                                                        q.visit_key
                                                ) "*RSS*"
                                            order by
                                                "*RSS*"."#PAT_MRN_ID#0XF5668450",
                                                "*RSS*"."#TRI_DIAG_1#0XF5668D10"
                                        ) a
                                ) t
                                left join (
                                    select
                                        mo.visit_key,
                                        min(ma.action_dt) as anti_dt,
                                        va.ed_dpart_dt as ed_depart_dt
                                    from
                                        (
                                            (
                                                (
                                                    {{source('cdw', 'medication_order')}} as mo
                                                    join {{source('cdw', 'medication')}} as med on ((mo.med_key = med.med_key))
                                                )
                                                join {{source('cdw', 'medication_administration')}} as ma on ((mo.med_ord_key = ma.med_ord_key))
                                            )
                                            join {{source('cdw', 'visit_addl_info')}} as va on ((mo.visit_key = va.visit_key))
                                        )
                                    where
                                        (
                                            upper(med.generic_nm) ~~ like_escape('%CEFOTAXIME%' :: "VARCHAR", '\' :: "VARCHAR")
                                        )
                                    group by
                                        mo.visit_key,
                                        va.ed_dpart_dt
                                    having
                                        (min(ma.action_dt) <= va.ed_dpart_dt)
                                ) anti on ((t.visit_key = anti.visit_key))
                            )
                            left join (
                                select
                                    a.visit_key,
                                    min(a.md_eval_dt) as md_eval_dt,
                                    min(a.decision_to_admit_dt) as decision_to_admit_dt,
                                    min(a.md_report_dt) as md_report_dt
                                from
                                    (
                                        select
                                            v.visit_key,
                                            case
                                                when (
                                                    (me.event_id = 111)
                                                    or (me.event_id = 300121)
                                                ) then min(v.event_dt)
                                                else null :: "TIMESTAMP"
                                            end as md_eval_dt,
                                            case
                                                when (me.event_id = 231) then min(v.event_dt)
                                                else null :: "TIMESTAMP"
                                            end as decision_to_admit_dt,
                                            case
                                                when (me.event_id = 300100) then min(v.event_dt)
                                                else null :: "TIMESTAMP"
                                            end as md_report_dt
                                        from
                                            (
                                                {{source('cdw', 'visit_ed_event')}} as v
                                                join {{source('cdw', 'master_event_type')}} as me on ((me.event_type_key = v.event_type_key))
                                            )
                                        where
                                            (
                                                (v.event_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP")
                                                and (
                                                    me.event_id in (
                                                        '111' :: int8,
                                                        '231' :: int8,
                                                        '300121' :: int8,
                                                        '300100' :: int8
                                                    )
                                                )
                                            )
                                        group by
                                            v.visit_key,
                                            me.event_id
                                    ) a
                                group by
                                    a.visit_key
                            ) md on ((t.visit_key = md.visit_key))
                        )
                        left join (
                            select
                                min(po.specimen_taken_dt) as uc_specimen_dt,
                                po.visit_key
                            from
                                {{source('cdw', 'procedure_order')}} as po
                            where
                                (
                                    (
                                        (
                                            upper(po.proc_ord_nm) ~~ like_escape('%URINE%CULT%' :: "VARCHAR", '\' :: "VARCHAR")
                                        )
                                        and (
                                            po.specimen_taken_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                        )
                                    )
                                    or (
                                        (
                                            upper(po.proc_ord_nm) ~~ like_escape('%CULT%URINE%' :: "VARCHAR", '\' :: "VARCHAR")
                                        )
                                        and (
                                            po.specimen_taken_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                        )
                                    )
                                )
                            group by
                                po.visit_key
                        ) uc on ((t.visit_key = uc.visit_key))
                    )
                    left join (
                        select
                            a.visit_key,
                            a.specimen_taken_dt as lp_taken_dt,
                            a.lp_location as lp_location_key
                        from
                            (
                                select
                                    po.visit_key,
                                    po.proc_key,
                                    po.proc_ord_nm,
                                    vi.ed_dpart_dt as ed_depart_dt,
                                    po.specimen_taken_dt,
                                    case
                                        when (po.specimen_taken_dt < vi.ed_dpart_dt) then 0
                                        when (
                                            (po.specimen_taken_dt > vi.ed_dpart_dt)
                                            and (
                                                (
                                                    (po.proc_key = 272949)
                                                    or (po.proc_key = 274735)
                                                )
                                                or (
                                                    (po.proc_key = 257830)
                                                    or (po.proc_key = 284391)
                                                )
                                            )
                                        ) then 1
                                        when (
                                            (po.specimen_taken_dt > vi.ed_dpart_dt)
                                            and (po.proc_key = 266898)
                                        ) then 2
                                        else 2
                                    end as lp_location
                                from
                                    (
                                        (
                                            {{source('cdw', 'procedure_order')}} as po
                                            join {{source('cdw', 'cdw_dictionary')}} as cd on ((po.dict_ord_stat_key = cd.dict_key))
                                        )
                                        join {{source('cdw', 'visit_addl_info')}} as vi on ((po.visit_key = vi.visit_key))
                                    )
                                where
                                    (
                                        (
                                            (
                                                po.proc_key in (
                                                    '272949' :: int8,
                                                    '266898' :: int8,
                                                    '274735' :: int8,
                                                    '257830' :: int8,
                                                    '284391' :: int8
                                                )
                                            )
                                            and (
                                                po.specimen_taken_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                            )
                                        )
                                        and (cd.src_id <> '4' :: numeric(1, 0))
                                    )
                            ) a,
                            (
                                select
                                    po.visit_key,
                                    max(po.specimen_taken_dt) as lp_max_dt
                                from
                                    (
                                        {{source('cdw', 'procedure_order')}} as po
                                        join {{source('cdw', 'cdw_dictionary')}} as cd on ((po.dict_ord_stat_key = cd.dict_key))
                                    )
                                where
                                    (
                                        (
                                            (
                                                po.proc_key in (
                                                    '272949' :: int8,
                                                    '266898' :: int8,
                                                    '274735' :: int8,
                                                    '257830' :: int8,
                                                    '284391' :: int8
                                                )
                                            )
                                            and (
                                                po.specimen_taken_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                            )
                                        )
                                        and (cd.src_id <> '4' :: numeric(1, 0))
                                    )
                                group by
                                    po.visit_key
                            ) b
                        where
                            (
                                (a.visit_key = b.visit_key)
                                and (a.specimen_taken_dt = b.lp_max_dt)
                            )
                    ) lp on ((t.visit_key = lp.visit_key))
                )
                left join (
                    select
                        a.visit_key,
                        a.prov_key as ed_prov_key,
                        c.full_nm as ed_full_nm
                    from
                        (
                            select
                                distinct ph.visit_key,
                                ph.attnd_to_dt,
                                ph.prov_key
                            from
                                {{source('cdw', 'visit_provider_hist')}} as ph
                            where
                                (
                                    (ph.ed_attnd_ind = 1)
                                    and (
                                        ph.attnd_from_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                    )
                                )
                        ) a,
                        (
                            select
                                distinct ph.visit_key,
                                max(ph.attnd_to_dt) as max_ed_dt
                            from
                                {{source('cdw', 'visit_provider_hist')}} as ph
                            where
                                (
                                    (ph.ed_attnd_ind = 1)
                                    and (
                                        ph.attnd_from_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                    )
                                )
                            group by
                                ph.visit_key
                        ) b,
                        (
                            select
                                p.prov_key,
                                p.full_nm
                            from
                                {{source('cdw', 'provider')}} as p
                        ) c
                    where
                        (
                            (
                                (a.visit_key = b.visit_key)
                                and (a.attnd_to_dt = b.max_ed_dt)
                            )
                            and (a.prov_key = c.prov_key)
                        )
                ) prv on ((t.visit_key = prv.visit_key))
            )
            left join (
                select
                    a.visit_key,
                    a.prov_key as ip_prov_key,
                    c.full_nm as ip_prov_nm
                from
                    (
                        select
                            distinct ph.visit_key,
                            ph.attnd_to_dt,
                            ph.prov_key
                        from
                            {{source('cdw', 'visit_provider_hist')}} as ph
                        where
                            (
                                (ph.ed_attnd_ind = 0)
                                and (
                                    ph.attnd_from_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                )
                            )
                    ) a,
                    (
                        select
                            distinct ph.visit_key,
                            min(ph.attnd_to_dt) as max_ed_dt
                        from
                            {{source('cdw', 'visit_provider_hist')}} as ph
                        where
                            (
                                (ph.ed_attnd_ind = 0)
                                and (
                                    ph.attnd_from_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                )
                            )
                        group by
                            ph.visit_key
                    ) b,
                    (
                        select
                            p.prov_key,
                            p.full_nm
                        from
                            {{source('cdw', 'provider')}} as p
                    ) c
                where
                    (
                        (
                            (a.visit_key = b.visit_key)
                            and (a.attnd_to_dt = b.max_ed_dt)
                        )
                        and (a.prov_key = c.prov_key)
                    )
            ) ipprv on ((t.visit_key = ipprv.visit_key))
        )
        left join (
            select
                b.visit_key,
                sum(b.med_total) as med_total
            from
                (
                    select
                        distinct a.visit_key,
                        a.med_total
                    from
                        (
                            select
                                mo.visit_key,
                                mo.ptcl_key,
                                min(ma.action_dt) as taken_dt,
                                case
                                    when (
                                        substr(upper(med.generic_nm), 1, 3) = 'CEF' :: "VARCHAR"
                                    ) then 4
                                    when (
                                        substr(upper(med.generic_nm), 1, 3) = 'AMP' :: "VARCHAR"
                                    ) then 2
                                    when (
                                        substr(upper(med.generic_nm), 1, 3) = 'ACY' :: "VARCHAR"
                                    ) then 1
                                    else null :: int4
                                end as med_total
                            from
                                (
                                    (
                                        (
                                            (
                                                {{source('cdw', 'medication_order')}} as mo
                                                join {{source('cdw', 'medication')}} as med on ((mo.med_key = med.med_key))
                                            )
                                            join {{source('cdw', 'medication_administration')}} as ma on ((mo.med_ord_key = ma.med_ord_key))
                                        )
                                        join {{source('cdw', 'visit_addl_info')}} as va on ((mo.visit_key = va.visit_key))
                                    )
                                    join {{source('cdw', 'cdw_dictionary')}} as cd on ((mo.dict_ord_stat_key = cd.dict_key))
                                )
                            where
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        va.adt_arrvl_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                                    )
                                                    and (
                                                        upper(med.generic_nm) ~~ like_escape('%CEFOTAXIME%' :: "VARCHAR", '\' :: "VARCHAR")
                                                    )
                                                )
                                                and (ma.action_dt > va.ed_dpart_dt)
                                            )
                                            and (cd.src_id <> '4' :: numeric(1, 0))
                                        )
                                        or (
                                            (
                                                (
                                                    (
                                                        va.adt_arrvl_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                                    )
                                                    and (
                                                        upper(med.generic_nm) ~~ like_escape('%AMPICILLIN%' :: "VARCHAR", '\' :: "VARCHAR")
                                                    )
                                                )
                                                and (ma.action_dt > va.ed_dpart_dt)
                                            )
                                            and (cd.src_id <> '4' :: numeric(1, 0))
                                        )
                                    )
                                    or (
                                        (
                                            (
                                                (
                                                    va.adt_arrvl_dt >= '2011-01-22 00:00:00' :: "TIMESTAMP"
                                                )
                                                and (
                                                    upper(med.generic_nm) ~~ like_escape('%ACYCLOVIR%' :: "VARCHAR", '\' :: "VARCHAR")
                                                )
                                            )
                                            and (ma.action_dt > va.ed_dpart_dt)
                                        )
                                        and (cd.src_id <> '4' :: numeric(1, 0))
                                    )
                                )
                            group by
                                mo.visit_key,
                                va.ed_dpart_dt,
                                med.generic_nm,
                                mo.ptcl_key,
                                va.adt_arrvl_dt
                        ) a
                ) b
            group by
                b.visit_key
        ) meds on ((t.visit_key = meds.visit_key))
    )
where
    (t.discharge_dt notnull)
