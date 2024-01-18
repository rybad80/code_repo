/*Pull out OGTTs done on CF patients. Sources we are pulling from are the order names
and the procedure names. Returns patient key and OGTT date*/
-- Get order names and flag as OGTT if they refer to glucose and either tolerance or hours
select
    cf_pat_visits.pat_key,
    date_trunc('day', proc_ord.specimen_taken_dt) as ogtt_date,
    extract(year from proc_ord.specimen_taken_dt) as ogtt_cy,
    case
        when lower(proc_ord.proc_ord_desc) like '%ogtt%'
            then 1
        when lower(proc_ord.proc_ord_desc) not like '%glucose%'
            then 0
        when lower(proc_ord.proc_ord_desc) like '%tol%'
            then 1
        when lower(proc_ord.proc_ord_desc) like '%hr%'
            then 1
        when lower(proc_ord.proc_ord_desc) like '%hour%'
            then 1
        else 0
    end as ogtt_ind
from {{ref('stg_cf_base')}} as cf_pat_visits
inner join {{source('cdw', 'procedure_order')}} as proc_ord
    on cf_pat_visits.pat_key = proc_ord.pat_key
    and proc_ord.specimen_taken_dt is not null
where --ogtt_date >= date('2022-01-01')
    ogtt_date >= date('2022-01-01')
group by cf_pat_visits.pat_key, ogtt_date, ogtt_cy, ogtt_ind
union all
-- Pull all procedures done with Glucose and Tolerance/Hr in the procedure name
-- Procedures only named Glucose are checked to be part of OGTT Order Set
select
    cf_pat_visits.pat_key,
    date_trunc('day', proc.specimen_taken_date) as ogtt_date,
    extract(year from proc.specimen_taken_date) as ogtt_cy,
    1 as ogtt_ind
from {{ref('stg_cf_base')}} as cf_pat_visits
inner join {{ref('procedure_order_clinical')}} as proc
    on cf_pat_visits.pat_key = proc.pat_key
    and ((lower(proc.procedure_name) like '%glucose%'
            and proc.orderset_name = 'PULMONARY GLUCOSE TOLERANCE CF SPECIFIC ORDER SET')
        or proc.procedure_name = 'GLUCOSE X3+INSULIN X3 -LC'
        or (lower(proc.procedure_name) like '%glucose%'
            and (lower(proc.procedure_name) like '%tol%'
                or lower(proc.procedure_name) like '%hr%'
                or lower(proc.procedure_name) like '%hour%')
            )
        )
    and proc.order_status = 'Completed'
    and proc.specimen_taken_date is not null
where -- ogtt_date >= date('2022-01-01')
    ogtt_date >= date('2022-01-01')
group by cf_pat_visits.pat_key, ogtt_date, ogtt_cy
