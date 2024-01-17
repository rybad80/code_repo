select
pat.full_nm as patient,
pat.pat_key,
pat.pat_mrn_id,
pat.dob,
cast(case when lower(narrative.ord_narr) like 'complications:%'
then substr(lower(narrative.ord_narr), instr(lower(narrative.ord_narr), 'complications', 1))
when lower(narrative.ord_narr) like '%guardian:%'
then substr(lower(narrative.ord_narr), instr(lower(narrative.ord_narr), 'guardian:', 1))
else null
end as varchar(256)) as ord_narr,
case when lower(procorder.proc_ord_nm) like '%biopsy%'
or lower(procorder.proc_ord_nm) like '%cryotherapy%'
or lower(procorder.proc_ord_nm) like '%excision%'
or lower(procorder.proc_ord_desc) like '%biopsy%'
or lower(procorder.proc_ord_desc) like '%cryotherapy%'
or lower(procorder.proc_ord_desc) like '%excision%'
or lower(procorder.proc_ord_desc) like '%destruc%'
or lower(procorder.proc_ord_desc) like '%shav%'
or lower(procorder.proc_ord_desc) like '%punch%'
or lower(procorder.proc_ord_desc) like '%exc skin%'
then 1
else 0 end as sel_procedure,
department.dept_nm,
visit.enc_id,
visit.visit_key,
date(visit.appt_dt) as service_dt,
serv_prov.prov_key as service_provider_key,
serv_prov.full_nm as service_prov,
bill_prov.prov_key as bill_provider_key,
bill_prov.full_nm as billing_prov,
procorder.rslt_dt,
procedure.cpt_cd, -- CURRENT PROCEDURAL TERMINOLOGY
dict.dict_nm as order_status,
narrative.seq_num as narrative_seq_num
from
    {{source('cdw', 'procedure_order')}} as procorder
inner join
    {{source('cdw', 'procedure')}} as procedure
        on procedure.proc_key = procorder.proc_key
left join
    {{source('cdw', 'procedure_order_narrative')}} as narrative
        on procorder.proc_ord_key = narrative.proc_ord_key
        and (narrative.ord_narr like '%Complications%'
        or narrative.ord_narr like '%Guardian:%')
inner join
    {{source('cdw', 'visit')}} as visit
        on procorder.visit_key = visit.visit_key
inner join
    {{source('cdw', 'patient')}} as pat
        on procorder.pat_key = pat.pat_key
inner join
    {{source('cdw', 'cdw_dictionary')}} as dict
        on procorder.dict_ord_stat_key = dict.dict_key
inner join
    {{source('cdw', 'provider')}} as serv_prov
        on serv_prov.prov_key = visit.visit_prov_key
inner join
    {{source('cdw', 'provider')}} as bill_prov
        on bill_prov.prov_key = procorder.bill_prov_key
inner join
    {{source('cdw', 'department')}} as department
        on department.dept_key = visit.dept_key
where
    lower(department.specialty) = 'dermatology'
group by
    pat.full_nm,
    pat.pat_key,
    pat.pat_mrn_id,
    pat.dob,
    ord_narr,
    sel_procedure,
    department.dept_nm,
    visit.enc_id,
    visit.visit_key,
    service_dt,
    service_provider_key,
    service_prov,
    bill_provider_key,
    billing_prov,
    procorder.rslt_dt,
    procedure.cpt_cd,
    order_status,
    narrative_seq_num
