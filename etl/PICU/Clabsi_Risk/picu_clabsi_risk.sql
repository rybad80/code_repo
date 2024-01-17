with risk_cohort as (
    select
    cohort.pat_mrn_id,
    cohort.pat_key,
    cohort.lda_id,
    cohort.place_dt,
    cohort.line_grouping,
    cohort.visit_key,
    cohort.hosp_admit_dt,
    cohort.census_dt,
    cohort.broviac_ind,
    cohort.num_lines,
    cohort.mult_line_ind,
    date(cohort.census_dt) - date(place_dt) + 1 as dwell_time,
    coalesce(hx_clabsi_ind, 0) as hx_clabsi_ind,
    coalesce(bhconsult.bh_consult_ind, 0) as bh_consult_ind,
    coalesce(tpa_ind, 0) as tpa_ind,
    coalesce(tpn_ind, 0) as tpn_ind,
    coalesce(vaso_ind, 0) as vaso_ind,
    coalesce(bh_meds_ind, 0) as bh_meds_ind,
    coalesce(bh_prn_meds_ind, 0) as bh_prn_meds_ind,
    coalesce(inv_vent_ind, 0) as inv_vent_ind,
    coalesce(ostomy_ind, 0) as ostomy_ind,
    coalesce(prematurity_ind, 0) as prematurity_ind,
    case when bh_consult_ind = 1 or bh_meds_ind = 1 or bh_prn_meds_ind = 1 then 1 else 0 end as bh_ind,
    coalesce(prbc_ind, 0) as prbc_ind,
    case when dwell_time between 0 and 7 then '0-7 days'
          when dwell_time between 8 and 14 then '8-14 days'
          when dwell_time between 15 and 29 then '15-29 days'
          when dwell_time between 30 and 59 then '30-59 days'
          when dwell_time between 60 and 89 then '60-89 days'
          when dwell_time >= 90 then '90+ days' end as dwell_cat

    from {{ ref('stg_picu_central_line_cohort') }} as cohort
        left join {{ ref('stg_picu_risk_bhconsults') }} as bhconsult on
        cohort.pat_key = bhconsult.pat_key and cohort.census_dt = bhconsult.census_dt
        left join
            {{ ref('stg_picu_risk_meds') }} as meds
                on cohort.pat_key = meds.pat_key and cohort.census_dt = meds.census_dt
        left join
            {{ ref('stg_picu_risk_inv_vent') }} as inv_vent on
        cohort.pat_key = inv_vent.pat_key and cohort.census_dt = inv_vent.census_dt
        left join {{ ref('stg_picu_risk_ostomy') }} as ostomy on
        cohort.pat_key = ostomy.pat_key and cohort.census_dt = ostomy.census_dt
        left join {{ ref('stg_picu_risk_prematurity') }} as prematurity on
        cohort.pat_key = prematurity.pat_key and cohort.census_dt = prematurity.census_dt
    left join {{ ref('stg_picu_risk_hxclabsi') }} as hx
        on cohort.pat_key = hx.pat_key and cohort.census_dt = hx.census_dt
    left join {{ ref('stg_picu_risk_prbc') }} as prbc
        on cohort.pat_key = prbc.pat_key and cohort.census_dt = prbc.census_dt
),

claccess as (
    select
        cohort.pat_mrn_id,
        cohort.pat_key,
        cohort.census_dt,
        rec_dt,
        coalesce(cl_acc.cum_cl_access, 0) as num_cl_access,
        lag(num_cl_access, 1, 0) over (partition by cohort.pat_mrn_id order by census_dt) as cl_access_lag_one,
        lag(num_cl_access, 2, 0) over (partition by cohort.pat_mrn_id order by census_dt) as cl_access_lag_two,
        num_cl_access + cl_access_lag_one + cl_access_lag_two as cl_access_sum
    from
        {{ ref('stg_picu_central_line_cohort') }} as cohort
        left join {{ ref('picu_cl_access') }} as cl_acc
        on cl_acc.pat_key = cohort.pat_key and cohort.census_dt = cl_acc.rec_dt
    group by 1, 2, 3, 4, 5
)

select distinct risk_cohort.*,
    claccess.rec_dt,
    claccess.num_cl_access,
    claccess.cl_access_sum
from risk_cohort
    left join claccess
        on claccess.pat_key = risk_cohort.pat_key
        and claccess.census_dt = risk_cohort.census_dt
