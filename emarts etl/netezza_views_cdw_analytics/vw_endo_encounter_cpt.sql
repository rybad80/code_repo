(
    select
        v.visit_key as "Encounter Key",
        (had.cpt_cd) :: varchar(200) as "CPT Code",
        case
            when (
                (
                    (
                        (had.cpt_cd = '99251' :: "VARCHAR")
                        or (had.cpt_cd = '99252' :: "VARCHAR")
                    )
                    or (
                        (had.cpt_cd = '99253' :: "VARCHAR")
                        or (had.cpt_cd = '99254' :: "VARCHAR")
                    )
                )
                or (had.cpt_cd = '99255' :: "VARCHAR")
            ) then 'Endocrine Consult Group' :: "VARCHAR"
            when (
                (
                    (
                        (had.cpt_cd = '98960' :: "VARCHAR")
                        or (had.cpt_cd = '98961' :: "VARCHAR")
                    )
                    or (
                        (had.cpt_cd = '98960' :: "VARCHAR")
                        or (had.cpt_cd = '95250' :: "VARCHAR")
                    )
                )
                or (had.cpt_cd = '95251' :: "VARCHAR")
            ) then 'Endocrine Education Group' :: "VARCHAR"
            else null :: "VARCHAR"
        end as "Endocrine CPT Grouper"
    from
        (
            (
                (
                    (
                        (
                            {{source('cdw', 'visit')}} as v
                            join {{source('cdw', 'visit_addl_info')}} as vai on ((v.visit_key = vai.visit_key))
                        )
                        join {{source('cdw', 'hospital_account_visit')}} as hav on ((vai.visit_key = hav.visit_key))
                    )
                    join {{source('cdw', 'hospital_account')}} as ha on ((hav.hsp_acct_key = ha.hsp_acct_key))
                )
                join {{source('cdw', 'hospital_account_diag_icd')}} as had on ((had.hsp_acct_key = ha.hsp_acct_key))
            )
            join {{source('cdw', 'department')}} as d on ((v.dept_key = d.dept_key))
        )
    where
        (
            (v.contact_dt_key >= 20090701)
            and (
                (
                    had.cpt_cd in (
                        ('97802' :: "VARCHAR") :: varchar(20),
                        ('97803' :: "VARCHAR") :: varchar(20),
                        ('97804' :: "VARCHAR") :: varchar(20),
                        ('95250' :: "VARCHAR") :: varchar(20),
                        ('95251' :: "VARCHAR") :: varchar(20),
                        ('99860' :: "VARCHAR") :: varchar(20),
                        ('99861' :: "VARCHAR") :: varchar(20),
                        ('78006' :: "VARCHAR") :: varchar(20),
                        ('78018' :: "VARCHAR") :: varchar(20),
                        ('78015' :: "VARCHAR") :: varchar(20),
                        ('78016' :: "VARCHAR") :: varchar(20),
                        ('78020' :: "VARCHAR") :: varchar(20),
                        ('79005' :: "VARCHAR") :: varchar(20),
                        ('76536' :: "VARCHAR") :: varchar(20),
                        ('71020' :: "VARCHAR") :: varchar(20),
                        ('71250' :: "VARCHAR") :: varchar(20),
                        ('70543' :: "VARCHAR") :: varchar(20),
                        ('60100' :: "VARCHAR") :: varchar(20),
                        ('60699' :: "VARCHAR") :: varchar(20),
                        ('60200' :: "VARCHAR") :: varchar(20),
                        ('60210' :: "VARCHAR") :: varchar(20),
                        ('60212' :: "VARCHAR") :: varchar(20),
                        ('60260' :: "VARCHAR") :: varchar(20),
                        ('60240' :: "VARCHAR") :: varchar(20),
                        ('60220' :: "VARCHAR") :: varchar(20),
                        ('60225' :: "VARCHAR") :: varchar(20),
                        ('60252' :: "VARCHAR") :: varchar(20),
                        ('60512' :: "VARCHAR") :: varchar(20),
                        ('98960' :: "VARCHAR") :: varchar(20),
                        ('95250' :: "VARCHAR") :: varchar(20),
                        ('95251' :: "VARCHAR") :: varchar(20),
                        ('98961' :: "VARCHAR") :: varchar(20),
                        ('98962' :: "VARCHAR") :: varchar(20),
                        ('99251' :: "VARCHAR") :: varchar(20),
                        ('99252' :: "VARCHAR") :: varchar(20),
                        ('99253' :: "VARCHAR") :: varchar(20),
                        ('99254' :: "VARCHAR") :: varchar(20),
                        ('99255' :: "VARCHAR") :: varchar(20)
                    )
                )
                or (
                    (d.specialty = 'ENDOCRINOLOGY' :: "VARCHAR")
                    or (ha.dict_pri_svc_key = 8840)
                )
            )
        )
)
union
(
    select
        pb.visit_key as "Encounter Key",
        pb.cpt_cd as "CPT Code",
        case
            when (
                (
                    (
                        (pb.cpt_cd = '99251' :: "VARCHAR")
                        or (pb.cpt_cd = '99252' :: "VARCHAR")
                    )
                    or (
                        (pb.cpt_cd = '99253' :: "VARCHAR")
                        or (pb.cpt_cd = '99254' :: "VARCHAR")
                    )
                )
                or (pb.cpt_cd = '99255' :: "VARCHAR")
            ) then 'Endocrine Consult Group' :: "VARCHAR"
            when (
                (
                    (
                        (pb.cpt_cd = '98960' :: "VARCHAR")
                        or (pb.cpt_cd = '98961' :: "VARCHAR")
                    )
                    or (
                        (pb.cpt_cd = '98960' :: "VARCHAR")
                        or (pb.cpt_cd = '95250' :: "VARCHAR")
                    )
                )
                or (pb.cpt_cd = '95251' :: "VARCHAR")
            ) then 'Endocrine Education Group' :: "VARCHAR"
            else null :: "VARCHAR"
        end as "Endocrine CPT Grouper"
    from
        (
            {{source('cdw', 'pb_transaction')}} as pb
            join {{source('cdw', 'department')}} as d on ((pb.dept_key = d.dept_key))
        )
    where
        (
            (pb.svc_dt_key >= 20090701)
            and (
                (d.specialty = 'ENDOCRINOLOGY' :: "VARCHAR")
                or (
                    pb.cpt_cd in (
                        ('97802' :: "VARCHAR") :: varchar(200),
                        ('97803' :: "VARCHAR") :: varchar(200),
                        ('97804' :: "VARCHAR") :: varchar(200),
                        ('95250' :: "VARCHAR") :: varchar(200),
                        ('95251' :: "VARCHAR") :: varchar(200),
                        ('99860' :: "VARCHAR") :: varchar(200),
                        ('99861' :: "VARCHAR") :: varchar(200),
                        ('78006' :: "VARCHAR") :: varchar(200),
                        ('78018' :: "VARCHAR") :: varchar(200),
                        ('78015' :: "VARCHAR") :: varchar(200),
                        ('78016' :: "VARCHAR") :: varchar(200),
                        ('78020' :: "VARCHAR") :: varchar(200),
                        ('79005' :: "VARCHAR") :: varchar(200),
                        ('76536' :: "VARCHAR") :: varchar(200),
                        ('71020' :: "VARCHAR") :: varchar(200),
                        ('71250' :: "VARCHAR") :: varchar(200),
                        ('70543' :: "VARCHAR") :: varchar(200),
                        ('60100' :: "VARCHAR") :: varchar(200),
                        ('60699' :: "VARCHAR") :: varchar(200),
                        ('60200' :: "VARCHAR") :: varchar(200),
                        ('60210' :: "VARCHAR") :: varchar(200),
                        ('60212' :: "VARCHAR") :: varchar(200),
                        ('60260' :: "VARCHAR") :: varchar(200),
                        ('60240' :: "VARCHAR") :: varchar(200),
                        ('60220' :: "VARCHAR") :: varchar(200),
                        ('60225' :: "VARCHAR") :: varchar(200),
                        ('60252' :: "VARCHAR") :: varchar(200),
                        ('60512' :: "VARCHAR") :: varchar(200),
                        ('98960' :: "VARCHAR") :: varchar(200),
                        ('95250' :: "VARCHAR") :: varchar(200),
                        ('95251' :: "VARCHAR") :: varchar(200),
                        ('98961' :: "VARCHAR") :: varchar(200),
                        ('98962' :: "VARCHAR") :: varchar(200),
                        ('99251' :: "VARCHAR") :: varchar(200),
                        ('99252' :: "VARCHAR") :: varchar(200),
                        ('99253' :: "VARCHAR") :: varchar(200),
                        ('99254' :: "VARCHAR") :: varchar(200),
                        ('99255' :: "VARCHAR") :: varchar(200)
                    )
                )
            )
        )
)