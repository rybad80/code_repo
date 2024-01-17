with prbc_gran as (
select cohort.pat_mrn_id,
cohort.pat_key,
--, proc_ord_nm
--, proc_ord_desc
--, cpt_cd
census_dt,
date(census_dt) - date(proc_ord_create_dt) as time_to_census,
case when time_to_census between 0 and 3 then 1 else 0 end as keep_ind,
case when cpt_cd in('500200700', '500200702') then 1 else 0 end as prbc_ind

from {{ ref('stg_picu_central_line_cohort') }} as cohort
--join CDWUAT..visit vis on cohort.visit_key = vis.visit_key
inner join {{ source('cdw', 'procedure_order') }} as po on po.visit_key = cohort.visit_key
inner join {{ source('cdw', 'cdw_dictionary') }} as d on d.dict_key = po.dict_ord_stat_key
     where cpt_cd in('500200700', '500200702') --lower(proc_ord_desc) like '%ip bld prod%' 
         and d.src_id = 5 --completed
	--	and cohort_ind = 1
group by 1, 2, 3, 4, 5, 6
)

select distinct pat_mrn_id, pat_key, census_dt, prbc_ind
from prbc_gran
where prbc_ind = 1
