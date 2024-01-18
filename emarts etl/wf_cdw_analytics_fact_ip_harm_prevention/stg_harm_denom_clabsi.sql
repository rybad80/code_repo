with census_limited as (
    select
        patient.pat_key,
        department.dept_key,
        isnull(fact_census_occ.visit_key, 0) as visit_key,
        date(fact_census_occ.census_dt) as event_dt
    from
        {{ source('cdw', 'fact_census_occ') }} as fact_census_occ
    inner join {{ source('cdw', 'patient') }} as patient
        on fact_census_occ.pat_key = patient.pat_key
    inner join {{ source('cdw', 'department') }} as department
        on fact_census_occ.dept_key = department.dept_key
    where
        fact_census_occ.hr_0 is not null
        and department.dept_id not in (
            101001069, -- ED
            10292012, -- PERIOP
            10201512, --EDECU
            101003001, --KOPH ED
            101003004, -- KOPH 3 Surgery
            101001617 --1 East Observation
        )
)

select
    census_limited.pat_key,
    census_limited.visit_key,
    census_limited.dept_key,
    census_limited.event_dt,
    'FACT_CENSUS' as denominator_source
from
    census_limited
inner join {{ ref('stg_harm_denom_clabsi_lda_combined') }} as stg_harm_denom_clabsi_lda_combined
    on census_limited.visit_key = stg_harm_denom_clabsi_lda_combined.visit_key
where
    date_trunc('day', stg_harm_denom_clabsi_lda_combined.first_access) <= census_limited.event_dt
    and (
        stg_harm_denom_clabsi_lda_combined.place_dt is null
        or census_limited.event_dt >= stg_harm_denom_clabsi_lda_combined.place_dt
    )
    and (
        stg_harm_denom_clabsi_lda_combined.remove_dt is null
        or census_limited.event_dt <= stg_harm_denom_clabsi_lda_combined.remove_dt
    )
    and (
        census_limited.event_dt < '2018-07-01'
        or (
            census_limited.event_dt >= '2018-07-01'
            and upper(stg_harm_denom_clabsi_lda_combined.lda_desc) not like '%INTRACARDIAC%'
        )
    )
group by
    census_limited.pat_key,
    census_limited.visit_key,
    census_limited.dept_key,
    census_limited.event_dt
