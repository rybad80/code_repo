with havi_pop_days as (
    select
        pat_key,
        visit_key,
        dept_key,
        event_dt,
        'FACT_FINANCIAL_STATISTIC' as denominator_source,
        'N/A' as division,
        sum(charge_qty) as denominator_value
    from (
        select
            coalesce(unit_move.current_dept_key, cc_to_dept.historical_dept_key, -1) as dept_key,
            stat_nm as measure_type,
            stat_measure as charge_qty,
            cc.cost_cntr_key,
            h.pat_key,
            h.pri_visit_key as visit_key,
            fs.post_dt as event_dt
        from (
            select
                fs.fs_acct_key as hsp_acct_key,
                fs.cost_cntr_key,
                fs.proc_key,
                to_date(fs.post_dt_key, 'YYYYMMDD') as post_dt,
                fs.stats_cd,
                fs.stat_measure,
                ms.stat_nm,
                case fs.patient_type when 'IP' then 'Inpatient' when 'OP' then 'Outpatient' end as account_type
            from
                {{source('cdw', 'fact_financial_statistic')}} as fs
                inner join {{source('cdw', 'master_statistic')}} as ms on fs.stats_cd = ms.stat_cd
            where
                fs.credit_gl_num != 'OSEEEEEEEE'
            union distinct
            select
                fs.fs_acct_key as hsp_acct_key,
                fs.cost_cntr_key,
                fs.proc_key,
                to_date(fs.post_dt_key, 'YYYYMMDD') as post_dt,
                fs.stats_cd,
                fs.stat_measure,
                'OP Patient Days' as stat_nm,
                case fs.patient_type when 'IP' then 'Inpatient' when 'OP' then 'Outpatient' end as account_type
            from
                {{source('cdw', 'fact_financial_statistic')}} as fs
            where
                fs.credit_gl_num != 'OSEEEEEEEE'
                and fs.patient_type = 'OP'
                and fs.stats_cd = 8
        ) as fs
        left join {{source('cdw', 'cost_center')}} as cc on fs.cost_cntr_key = cc.cost_cntr_key
        left join {{source('cdw', 'procedure')}} as px on fs.proc_key = px.proc_key
        left join {{source('cdw', 'hospital_account')}} as h on h.hsp_acct_key = fs.hsp_acct_key
        left join {{ref('master_harm_prevention_dept_mapping')}} as cc_to_dept
            on cc_to_dept.harm_type = 'HAVI'
            and cc_to_dept.current_cost_cntr_cd = cc.gl_comp
            and fs.post_dt between cc_to_dept.start_dt and cc_to_dept.end_dt
            and cc_to_dept.denominator_only_ind = 1
            and cc_to_dept.unit_move_ind = 0
        left join {{ref('master_harm_prevention_dept_mapping')}} as unit_move
            on unit_move.harm_type = 'HAVI'
            and unit_move.historical_dept_key = coalesce(cc_to_dept.historical_dept_key, -1)
            and fs.post_dt between unit_move.start_dt and unit_move.end_dt
            and unit_move.unit_move_ind = 1
    ) as t1 --noqa: L025
    where
        measure_type like '%Patient Days%'
        and dept_key != -1
    group by
        pat_key,
        visit_key,
        dept_key,
        event_dt
),
ssi as (
    select
         a.pat_key,
        isnull(a.admit_visit_key, 0) as visit_key,
        isnull(a.dept_key, 0) as dept_key,
        date(a.surgical_dt) as event_dt,
        a.division,
        count(*) as denominator_value,
        'FACT_IP_SSI_OR_PROCEDURES' as denominator_source
    from (
        select distinct
            ssi_or_procs.surgical_dt,
            patient.pat_key,
            provider.prov_id,
            ssi_or_procs.nhsn_category,
            ssi_or_procs.admit_visit_key,
            ssi_or_procs.division,
            coalesce(m.historical_dept_key, visit.dept_key) as dept_key
        from
            {{ source('cdw', 'fact_ip_ssi_or_procedures') }} as ssi_or_procs
            inner join {{ source('cdw', 'patient') }} as patient on patient.pat_key = ssi_or_procs.pat_key
            inner join {{ source('cdw', 'provider') }} as provider on provider.prov_key = ssi_or_procs.surgeon_provider_key
            inner join {{ source('cdw', 'visit') }} as visit on visit.visit_key = ssi_or_procs.surgery_visit_key
            left join {{ ref('master_harm_prevention_dept_mapping') }} as m
                on m.harm_type = 'SSI'
                and m.current_dept_key = visit.dept_key
                and date(ssi_or_procs.surgical_dt) between m.start_dt and m.end_dt
                and m.denominator_only_ind = 1
                and m.unit_move_ind = 0
        where
            ssi_or_procs.chop_main_ind = 1
   ) as a
    group by
        a.pat_key,
        a.dept_key,
        a.admit_visit_key,
        date(a.surgical_dt),
        a.division
),
prev_6nw as (
    select
        ve.visit_key,
        date(ve.eff_event_dt) as eff_event_dt,
        max(d.dept_key) as current_dept_key,
        min(d_prev.dept_key) as prev_dept_key
    from {{ source('cdw', 'visit_event') }} as ve
    inner join {{ source('cdw', 'department') }} as d on d.dept_key = ve.dept_key
    inner join {{ source('cdw', 'visit_event') }} as ve_prev on ve_prev.visit_event_key = ve.xfer_in_visit_event_key
    inner join {{ source('cdw', 'department') }} as d_prev on d_prev.dept_key = ve_prev.dept_key
    where d.dept_id = 58 -- 6NW
    and ve.xfer_in_visit_event_key != 0
    group by
        ve.visit_key,
        date(ve.eff_event_dt)
),
clabsi as (
    select
        pat_key,
        visit_key,
        dept_key,
        event_dt,
        denominator_source
    from
       {{ ref('stg_harm_denom_clabsi') }}
),
cauti as (
    select
        pat_key,
        visit_key,
        dept_key,
        event_dt,
        denominator_source
    from
       {{ ref('stg_harm_denom_cauti') }}
),
vap as (
    select
        pat_key,
        visit_key,
        dept_key,
        event_dt,
        denominator_source
    from
        {{ ref('stg_harm_denom_vap') }}
),
cauti_clabsi_vap as (
    select
        a.harm_type,
        a.pat_key,
        a.visit_key,
        coalesce(unit_move.historical_dept_key, m.historical_dept_key, d.dept_key) as dept_key,
        a.event_dt,
        a.denominator_source,
        a.denominator_value,
        case when prev_6nw.prev_dept_key is null then 1 else 0 end as compare_to_hai_pop_days_ind,
        'N/A' as division
    from (
        select
'CLABSI' as harm_type,
pat_key,
visit_key,
dept_key,
event_dt,
1 as denominator_value,
denominator_source
from clabsi
        union distinct
        select
'CAUTI' as harm_type,
pat_key,
visit_key,
dept_key,
event_dt,
1 as denominator_value,
denominator_source
from cauti
        union distinct
        select
'VAP' as harm_type,
pat_key,
visit_key,
dept_key,
event_dt,
1 as denominator_value,
denominator_source
from vap
        union distinct
        -- These values are the min dates in FACT_IP_HARM_EVENT_ALL for denominators for CAUTI, CLABSI, HAVI
        -- So, anything before those dates, we need to use HAI_POP_DAYS        
        select
            hai_type as harm_type,
            -1 as pat_key,
            -1 as visit_key,
            dept_key,
            date(line_dt_key) as event_dt,
            days_value as denominator_value,
            'HAI_POPULATION_DAYS' as denominator_source
        from {{ source('cdw', 'hai_population_days') }}
        where
            coalesce(days_value, 0) > 0
            and (
                (hai_type = 'CAUTI' and line_dt_key < 20130101)
                or (hai_type = 'CLABSI' and line_dt_key < 20130115)
                or (hai_type = 'VAP' and line_dt_key < 20110120)
            )
    ) as a
    left join prev_6nw on prev_6nw.visit_key = a.visit_key and prev_6nw.eff_event_dt = date(a.event_dt) and prev_6nw.current_dept_key = a.dept_key
    left join {{ source('cdw', 'department') }} as d on d.dept_key = coalesce(prev_6nw.prev_dept_key, a.dept_key)
    left join {{ ref('master_harm_prevention_dept_mapping') }} as m --remove mapping
        on m.harm_type = a.harm_type
        and m.current_dept_id = d.dept_id
        and a.event_dt between m.start_dt and m.end_dt
        and m.denominator_only_ind = 1
        and m.unit_move_ind = 0
    left join {{ ref('master_harm_prevention_dept_mapping') }} as unit_move
        on unit_move.harm_type = a.harm_type
        and unit_move.current_dept_key = coalesce(m.historical_dept_key, d.dept_key)
        and a.event_dt between unit_move.start_dt and unit_move.end_dt
        and unit_move.unit_move_ind = 1
),
ue as (
    select
        pat_key,
        visit_key,
        dept_key,
        event_dt,
        denominator_source,
        sum(overnight_ind) as denominator_value
    from
        {{ ref('stg_harm_denom_ue') }}
    where
        --If keys are used to link numerator and denominator, this should be done downstream
        overnight_ind = 1
     --   and event_dt >= '2021-01-19' -- program start
    group by
        pat_key,
        visit_key,
        dept_key,
        event_dt,
        denominator_source
),
all_indicators as (
    select
harm_type,
pat_key,
visit_key,
dept_key,
event_dt,
denominator_source,
denominator_value,
division,
compare_to_hai_pop_days_ind
from cauti_clabsi_vap
    union distinct
    select
'SSI',
pat_key,
visit_key,
dept_key,
event_dt,
denominator_source,
denominator_value,
division,
1 as compare_to_hai_pop_days_ind
from ssi
    union distinct
    select
'HAVI',
pat_key,
visit_key,
dept_key,
event_dt,
denominator_source,
denominator_value,
division,
1 as compare_to_hai_pop_days_ind
from havi_pop_days
    union distinct
    select
'HAPI',
pat_key,
visit_key,
dept_key,
event_dt,
denominator_source,
denominator_value,
division,
1 as compare_to_hai_pop_days_ind
from havi_pop_days
    union distinct
    select
'Falls with Injury',
pat_key,
visit_key,
dept_key,
event_dt,
denominator_source,
denominator_value,
division,
1 as compare_to_hai_pop_days_ind
from havi_pop_days
    union distinct
    select
'VTE',
pat_key,
visit_key,
dept_key,
event_dt,
denominator_source,
denominator_value,
division,
1 as compare_to_hai_pop_days_ind
from havi_pop_days
    union distinct
    -- 7/1/18: Only take PIVIE denominator from 2/1/15 since numerator starts here
    select
'PIVIE',
pat_key,
visit_key,
dept_key,
event_dt,
denominator_source,
denominator_value,
division,
1 as compare_to_hai_pop_days_ind
from havi_pop_days where event_dt >= '2015-02-01'
    union distinct
    select
'UE',
pat_key,
visit_key,
dept_key,
event_dt,
denominator_source,
denominator_value,
--unsure about the following
'N/A' as division,
0 as compare_to_hai_pop_days_ind
from ue
)
select
    isnull(a.visit_key, 0) as visit_key,
    date(a.event_dt) as harm_event_dt,
    a.pat_key,
    a.dept_key,
    coalesce(dept_groups_by_date.mstr_dept_grp_chop_key, dept_groups_imputation.mstr_dept_grp_chop_key) as mstr_dept_grp_key,
    coalesce(dept_groups_by_date.chop_dept_grp_nm, dept_groups_imputation.chop_dept_grp_nm) as dept_grp_nm,
    coalesce(dept_groups_by_date.chop_dept_grp_abbr, dept_groups_imputation.chop_dept_grp_abbr) as dept_grp_abbr,
    visit.enc_id,
    -1 as harm_id,
    a.harm_type,
    a.denominator_source as data_source,
    upper(a.division) as division,
    cast('N/A' as varchar(50)) as pathogen_code_1,
    cast('N/A' as varchar(50)) as pathogen_code_2,
    cast('N/A' as varchar(50)) as pathogen_code_3,
    cast(0 as integer) as numerator_value,
    cast(a.denominator_value as integer) as denominator_value,
    null as harm_conf_dt,
    visit.hosp_admit_dt,
    visit.hosp_dischrg_dt,
    a.compare_to_hai_pop_days_ind,
    coalesce(race_eth.pat_race_ethnicity, 'blank') as pat_race_ethnicity,
    coalesce(pref_lang.pat_pref_lang, 'blank') as pat_pref_lang
from
    all_indicators as a
    left join {{ source('cdw', 'visit') }} as visit on visit.visit_key = a.visit_key
    left join {{ source('cdw', 'department') }} as d on d.dept_key = a.dept_key
    left join {{ref('stg_harm_dept_grp')}} as dept_groups_by_date
        on dept_groups_by_date.dept_key = a.dept_key
            and date(
                a.event_dt
            ) = dept_groups_by_date.dept_align_dt
    left join {{ref('stg_harm_dept_grp')}} as dept_groups_imputation
        on dept_groups_imputation.dept_key = a.dept_key
            and dept_groups_imputation.depts_seq_num = 1
    left join {{ ref('stg_realdata_race_eth') }} as race_eth on race_eth.pat_key = a.pat_key
    left join {{ ref('stg_realdata_pref_lang') }} as pref_lang on pref_lang.pat_key = a.pat_key
    inner join {{ ref('stg_harm_dept_ip_op')}} as ip on a.dept_key = ip.dept_key
where
    ip.ip_unit_ind = 1
    or a.harm_type in (
        'SSI', --uses divisions
        'UE' --include 7 West A PICU for UE
    )
