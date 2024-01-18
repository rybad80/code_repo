select
    pat.pat_key,
    isnull(lda.visit_key, 0) as visit_key,
    dep.dept_key,
    date(lda.census_dt) as event_dt,
    'FACT_IP_LDA_FOLEY' as denominator_source
from
    {{ source('cdw', 'fact_ip_lda_foley') }} as lda
    inner join {{ source('cdw', 'patient') }} as pat on pat.pat_key = lda.pat_key
    inner join {{ source('cdw', 'department') }} as dep on dep.dept_key = lda.dept_key
    inner join {{ source('cdw', 'master_bed') }} as master_bed on master_bed.bed_key = lda.bed_key
group by
    pat.pat_key,
    lda.visit_key,
    dep.dept_key,
    date(lda.census_dt)
