with encounter_types as (
    select
        cd.dict_key as enc_type_key,
        cd.dict_nm as encounter_type
    from
        {{source('cdw', 'cdw_dictionary')}} as cd
    where
        (
            (
                cd.dict_nm in (
                    ('CONFIDENTIAL VISIT' :: "VARCHAR") :: varchar(500),
                    ('EMERGENCY' :: "VARCHAR") :: varchar(500),
                    ('HOSPITAL' :: "VARCHAR") :: varchar(500),
                    ('HOSPITAL ENCOUNTER' :: "VARCHAR") :: varchar(500),
                    ('INPATIENT' :: "VARCHAR") :: varchar(500),
                    ('OFFICE VISIT' :: "VARCHAR") :: varchar(500),
                    ('OUTPATIENT' :: "VARCHAR") :: varchar(500),
                    ('SUNDAY OFFICE VISIT' :: "VARCHAR") :: varchar(500),
                    ('SURGERY' :: "VARCHAR") :: varchar(500),
                    ('AUDIOLOGY VISIT' :: "VARCHAR") :: varchar(500)
                )
            )
            and (
                cd.dict_cat_nm = 'CLARITY_DISP_ENC_TYPE' :: "VARCHAR"
            )
        )
)
select
    distinct vi.pat_key,
    pg.street_lat_deg_y,
    pg.street_long_deg_x
from
    (
        (
            (
                encounter_types et
                join {{source('cdw', 'visit')}} as vi on ((et.enc_type_key = vi.dict_enc_type_key))
            )
            join {{source('cdw', 'patient')}} as pa on ((vi.pat_key = pa.pat_key))
        )
        join {{source('cdw', 'patient_geographical_spatial_info')}} as pg on (
            (
                (vi.pat_key = pg.pat_key)
                and (pg.seq_num = 0)
            )
        )
    )
where
    (
        (
            (
                vi.appt_made_dt >= "TIMESTAMP"((date('now(0)' :: "VARCHAR") - 1))
            )
            and (
                pa."STATE" in (
                    ('PENNSYLVANIA' :: "VARCHAR") :: varchar(50),
                    ('NEW JERSEY' :: "VARCHAR") :: varchar(50),
                    ('DELAWARE' :: "VARCHAR") :: varchar(50)
                )
            )
        )
        and (pg.accuracy_score > 0)
    )
