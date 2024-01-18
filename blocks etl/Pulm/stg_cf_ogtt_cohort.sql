-- Take cohort and join OGTT information accordingly
select
    cf_pat_visits.pat_key,
    cf_pat_visits.cy,
    pat_all.mrn,
    pat_all.patient_name,
    -- Lets us track what patients were below cohort age at period start
    cf_pat_visits.age_years as min_age,
    pat_all.race,
    pat_all.ethnicity,
    pat_all.sex,
    pat_all.preferred_language,
    pat_all.mailing_zip,
    -- OGTTs should be completed once per year
    -- If multiple OGTTs were completed, the earlier results were likely flawed
    max(ogtt.ogtt_date) as ogtt_date,
    ogtt.ogtt_cy
from {{ref('stg_cf_base')}} as cf_pat_visits
inner join {{ref('stg_patient')}} as pat_all
    on cf_pat_visits.pat_key = pat_all.pat_key
inner join {{ref('stg_cf_exclusion')}} as exclude_pat
    on cf_pat_visits.pat_key = exclude_pat.pat_key
left join {{ref('stg_cf_ogtt_admin')}} as ogtt
    on cf_pat_visits.pat_key = ogtt.pat_key
    and ogtt.ogtt_ind = 1
    and cf_pat_visits.cy = ogtt.ogtt_cy
where exclude_ind = 0
group by
    cf_pat_visits.pat_key,
    cf_pat_visits.cy,
    pat_all.mrn,
    pat_all.patient_name,
    cf_pat_visits.age_years,
    pat_all.race,
    pat_all.ethnicity,
    pat_all.sex,
    pat_all.preferred_language,
    pat_all.mailing_zip,
    ogtt.ogtt_cy
