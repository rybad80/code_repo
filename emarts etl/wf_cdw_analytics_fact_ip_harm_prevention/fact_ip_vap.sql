select
hai_event_id,
visit_key,
pat_key,
dept_key,
room_key,
bed_key,
inf_surv_key,
dict_svc_key,
cast(dept_id as bigint) as dept_id,
cast(enc_id as numeric(14, 3)) as enc_id,
cast(pat_mrn_id as varchar(25)) as pat_mrn_id,
cast(pat_last_nm as varchar(100)) as pat_last_nm,
cast(pat_first_nm as varchar(100)) as pat_first_nm,
cast(pat_full_nm as varchar(200)) as pat_full_nm,
pat_dob,
pat_sex,
cast(svc_nm as varchar(50)) as svc_nm,
cast(room_nm as varchar(200)) as room_nm,
cast(room_num as varchar(100)) as room_num,
cast(bed_nm as varchar(300)) as bed_nm,
cast(num_days_admit_to_event as integer) as num_days_admit_to_event,
cast(pathogen_code_1 as varchar(6)) as pathogen_code_1,
cast(pathogen_desc_1 as varchar(500)) as pathogen_desc_1,
cast(pathogen_code_2 as varchar(6)) as pathogen_code_2,
cast(pathogen_desc_2 as varchar(500)) as pathogen_desc_2,
cast(pathogen_code_3 as varchar(6)) as pathogen_code_3,
cast(pathogen_desc_3 as varchar(500)) as pathogen_desc_3,
cast((coalesce(birthwt, 0)) as numeric(16, 5)) as birth_wt_in_grams,
cast(birth_wt_code as varchar(14)) as birth_wt_code,
admit_dt,
conf_dt,
event_dt,
cast(international_ind as byteint) as international_ind,
cast(reportable_ind as byteint) as reportable_ind,
current_timestamp as create_dt,
'DBT' as create_by,
current_timestamp as upd_dt
from (
    select
        coalesce(h.eventid, -1) as hai_event_id,
        coalesce(v.visit_key, -1) as visit_key,
        coalesce(h.pat_key, -1) as pat_key,
        coalesce(m.historical_dept_key, d.dept_key, -1) as dept_key,
        coalesce(v.room_key, -1) as room_key,
        coalesce(v.bed_key, -1) as bed_key,
        coalesce(s.inf_surv_key, -1) as inf_surv_key,
        coalesce(v.adt_svc_key, -2) as dict_svc_key,
        coalesce(m.historical_dept_id, d.dept_id, -1) as dept_id,
        v.enc_id,
        p.pat_mrn_id,
        p.last_nm as pat_last_nm,
        p.first_nm as pat_first_nm,
        p.full_nm as pat_full_nm,
        p.dob as pat_dob,
        p.sex as pat_sex,
        v.adt_svc_nm as svc_nm,
        v.room_nm,
        v.room_num,
        v.bed_nm,
        h.admtoevntdays as num_days_admit_to_event,
        h.pathogen1 as pathogen_code_1,
        h.pathogendesc1 as pathogen_desc_1,
        h.pathogen2 as pathogen_code_2,
        h.pathogendesc2 as pathogen_desc_2,
        h.pathogen3 as pathogen_code_3,
        h.pathogendesc3 as pathogen_desc_3,
        h.birthwt,
        trim(coalesce(h.birthwtcode, 'Missing')) as birth_wt_code,
        h.admitdate as admit_dt,
        s.conf_dt,
        h.eventdate as event_dt,
        coalesce(v.international_ind, -2) as international_ind,
        1 as reportable_ind,
        row_number() over (partition by h.eventid
                             order by case when v.dept_key = h.dept_key then 1 else 0 end desc,
                                      coalesce(v.bed_key, 0) desc,
                                      v.adt_svc_key desc
         ) as rownum
    from
        {{ source('cdw_analytics', 'metrics_hai') }} as h
        left join {{ source('cdw', 'patient') }} as p on p.pat_key = h.pat_key
        left join {{ ref('stg_visit_event_service') }} as v on v.pat_key = h.pat_key and h.eventdate between v.enter_dt and v.exit_dt
        left join {{ source('cdw', 'infection_surveillance') }} as s on h.eventid = s.inf_surv_id
        left join {{ source('cdw', 'department') }} as d on d.dept_key = h.dept_key
        left join {{ ref('master_harm_prevention_dept_mapping') }} as m
            on m.harm_type = 'VAP'
            and m.current_dept_key = d.dept_key
            and h.eventdate between m.start_dt and m.end_dt
            and m.denominator_only_ind = 0
    where
        h.hai_type = 'VAP'
) as vap --noqa: L025
where rownum = 1
and event_dt < '2022-07-01'
