{{ config(meta = {
    'critical': true
}) }}

/*First appearance of COVID dx and History of COVID dx
on problem list
Granularity: Patient*/
select
    patient_problem_list.pat_key,
    min(case when lower(diagnosis.icd10_cd) = 'z86.16'
        then patient_problem_list.noted_dt end)
    as covid_resolved_noted_date,
    min(case when lower(diagnosis.icd10_cd) != 'z86.16'
        then patient_problem_list.noted_dt end)
    as covid_noted_date
from
    {{source('cdw', 'patient_problem_list')}} as patient_problem_list
    inner join {{source('cdw', 'diagnosis')}} as diagnosis
        on diagnosis.dx_key = patient_problem_list.dx_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_prob_stat
        on dict_prob_stat.dict_key = patient_problem_list.dict_prob_stat_key
where
    lower(dict_prob_stat.dict_nm) != 'deleted'
    and lower(diagnosis.icd10_cd) in (
        'z86.16', --History of COVID
        'u07.1') -- COVID-19
group by
    patient_problem_list.pat_key
