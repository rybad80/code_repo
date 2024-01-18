with last_diab_type as ( --Type of Diabetes
    select
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key,
        row_number() over (
            partition by
                stg_diabetes_population_active.diabetes_reporting_month,
                stg_diabetes_population_active.patient_key
            order by
                stg_diabetes_flowsheets.recorded_date desc
        ) as fs_rn,
        stg_diabetes_flowsheets.recorded_date,
        stg_diabetes_flowsheets.meas_val as diab_type
    from
        {{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
        inner join {{ ref('stg_diabetes_flowsheets') }} as stg_diabetes_flowsheets
            on stg_diabetes_population_active.patient_key = stg_diabetes_flowsheets.patient_key
                and stg_diabetes_flowsheets.recorded_date < stg_diabetes_population_active.diabetes_reporting_month
    where
        stg_diabetes_flowsheets.fs_type = 'diab_type'
),

dx_date as ( --Date of Diagnosis
    select
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key,
        min(cast(date('1840-12-31') + cast(stg_diabetes_flowsheets.meas_val as int) as varchar(16))) as dx_date,
        year(dx_date) as dx_year
    from
        {{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
        inner join {{ ref('stg_diabetes_flowsheets') }} as stg_diabetes_flowsheets
            on stg_diabetes_population_active.patient_key = stg_diabetes_flowsheets.patient_key
                and stg_diabetes_flowsheets.recorded_date < stg_diabetes_population_active.diabetes_reporting_month
    where
        stg_diabetes_flowsheets.fs_type = 'dx date'
    group by
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key
),

dx_year as ( --Year of Diagnosis: a retired ICR flowsheet FOR historical visits
select
	stg_diabetes_population_active.diabetes_reporting_month,
	stg_diabetes_population_active.patient_key,
	--remove dx_year entered values that were not in 4-digit year format:
	min(case
        when cast(stg_diabetes_flowsheets.meas_val as int) >= 1980
            and cast(stg_diabetes_flowsheets.meas_val as int) <= year(current_date)
            then cast(stg_diabetes_flowsheets.meas_val as int)
    end) as dx_year,
    year(stg_diabetes_population_active.diabetes_reporting_month) - dx_year as duration_year
from
	{{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
	inner join {{ ref('stg_diabetes_flowsheets') }} as stg_diabetes_flowsheets
        on stg_diabetes_population_active.patient_key = stg_diabetes_flowsheets.patient_key
            and stg_diabetes_flowsheets.recorded_date < stg_diabetes_population_active.diabetes_reporting_month
            and stg_diabetes_flowsheets.recorded_date
                >= stg_diabetes_population_active.diabetes_reporting_month - interval('15 month')
where
	stg_diabetes_flowsheets.fs_type = 'endo date'
group by
	stg_diabetes_population_active.diabetes_reporting_month,
	stg_diabetes_population_active.patient_key
),

fs_prov as ( --Primary Diabetes Provider (NP), pull LAST name based ON prov_id that USER selected IN SDE
    select
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key,
        row_number() over (
            partition by
                stg_diabetes_population_active.diabetes_reporting_month,
                stg_diabetes_population_active.patient_key
            order by stg_diabetes_flowsheets.recorded_date desc
        ) as fs_rn,
        stg_diabetes_flowsheets.meas_val as fs_prov,
        max(stg_diabetes_flowsheets.np_prov_key) as np_prov_key
    from
        {{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
        inner join {{ ref('stg_diabetes_flowsheets') }} as stg_diabetes_flowsheets
            on stg_diabetes_population_active.patient_key = stg_diabetes_flowsheets.patient_key
                and stg_diabetes_flowsheets.recorded_date < stg_diabetes_population_active.diabetes_reporting_month
    where
        stg_diabetes_flowsheets.fs_type = 'np'
    group by
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key,
        stg_diabetes_flowsheets.recorded_date,
        stg_diabetes_flowsheets.meas_val
),

fs_prov_last as ( --most recent NP per patient
    select
        fs_prov.diabetes_reporting_month,
        fs_prov.patient_key,
        fs_prov.fs_rn,
        fs_prov.fs_prov,
        fs_prov.np_prov_key,
        max(worker.ad_login) as np_ad_login
    from
        fs_prov
        /*linked with last_name because old data intake didn't include prov_key
        (manually mapping based on excel sheet)*/
        left join {{ ref('worker') }} as worker
            on worker.prov_key = fs_prov.np_prov_key
    where
        fs_prov.fs_rn = 1
    group by
        fs_prov.diabetes_reporting_month,
        fs_prov.patient_key,
        fs_prov.fs_rn,
        fs_prov.fs_prov,
        fs_prov.np_prov_key
),

fs_team_last as (
    select
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key,
        row_number() over (
            partition by
                stg_diabetes_population_active.diabetes_reporting_month,
                stg_diabetes_population_active.patient_key
            order by
                stg_diabetes_flowsheets.recorded_date desc
        ) as fs_rn,
        case
            when stg_diabetes_flowsheets.meas_val in (
                'Philly- Monday Meerkats',
				'Philly- Tuesday Turtles',
				'Philly- Wednesday Wallabies',
				'Philly- Thursday Tigers')
			then 'Buerger'
            when stg_diabetes_flowsheets.meas_val != 'Team not assigned'
                and stg_diabetes_flowsheets.meas_val is not null
			then 'Satellite' else 'Team not assigned'
        end as team_group,
        case
            when stg_diabetes_flowsheets.meas_val != 'Team not assigned'
                and stg_diabetes_flowsheets.meas_val is not null
			then stg_diabetes_flowsheets.meas_val else 'Team not assigned'
        end as team_detail
    from
        {{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
        inner join {{ ref('stg_diabetes_flowsheets') }} as stg_diabetes_flowsheets
            on stg_diabetes_population_active.patient_key = stg_diabetes_flowsheets.patient_key
                and stg_diabetes_flowsheets.recorded_date < stg_diabetes_population_active.diabetes_reporting_month
    where
        stg_diabetes_flowsheets.fs_type = 'team'
),

fs_diab_regimen as (
    select
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key,
        dense_rank() over (
            partition by
                stg_diabetes_population_active.diabetes_reporting_month,
                stg_diabetes_population_active.patient_key
            order by
                stg_diabetes_flowsheets.recorded_date desc
        ) as fs_rn,
        group_concat(stg_diabetes_flowsheets.meas_val) as fs_diab_regimen_last,
        stg_diabetes_flowsheets.recorded_date
    from
        {{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
        inner join {{ ref('stg_diabetes_flowsheets') }} as stg_diabetes_flowsheets
            on stg_diabetes_population_active.patient_key = stg_diabetes_flowsheets.patient_key
                and stg_diabetes_flowsheets.recorded_date < stg_diabetes_population_active.diabetes_reporting_month
    where
        stg_diabetes_flowsheets.fs_type = 'diab_regimen'
    group by
        stg_diabetes_population_active.diabetes_reporting_month,
        stg_diabetes_population_active.patient_key,
        stg_diabetes_flowsheets.recorded_date
)

select
    stg_diabetes_population_active.diabetes_reporting_month,
    stg_diabetes_population_active.report_card_4mo_pat_category,
	stg_diabetes_population_active.patient_key,
    stg_diabetes_population_active.pat_key,
    stg_diabetes_recent_visit_indicators.last_visit_type,
	stg_diabetes_recent_visit_indicators.last_encounter_type,
	stg_diabetes_recent_visit_indicators.last_prov,
	stg_diabetes_recent_visit_indicators.last_prov_type,
	stg_diabetes_recent_visit_indicators.last_visit_date,
	stg_diabetes_recent_visit_indicators.last_15mo_md_visit_ind,
	stg_diabetes_recent_visit_indicators.last_4mo_mdnp_visit_ind,
	stg_diabetes_recent_visit_indicators.last_15mo_edu_visit_ind,
	last_diab_type.diab_type as diabetes_type, --LAST edit ON report point
    fs_team_last.team_group as last_seen_team_group,
	fs_team_last.team_detail as last_seen_team_detail,
	fs_prov_last.fs_prov as last_seen_np,
	fs_prov_last.np_ad_login as last_seen_np_ad_login,
	fs_diab_regimen.fs_diab_regimen_last as diab_regimen, --LAST edit ON report point
    dx_date.dx_date,
    dx_year.dx_year,
    dx_year.duration_year,
    stg_diabetes_recent_visit_indicators.usnwr_submission_year
from
    {{ ref('stg_diabetes_population_active') }} as stg_diabetes_population_active
	left join dx_date
        on dx_date.patient_key = stg_diabetes_population_active.patient_key
            and dx_date.diabetes_reporting_month = stg_diabetes_population_active.diabetes_reporting_month
	left join dx_year
        on dx_year.patient_key = stg_diabetes_population_active.patient_key
            and dx_year.diabetes_reporting_month = stg_diabetes_population_active.diabetes_reporting_month
	left join fs_prov_last
        on fs_prov_last.patient_key = stg_diabetes_population_active.patient_key
            and fs_prov_last.diabetes_reporting_month = stg_diabetes_population_active.diabetes_reporting_month
	left join fs_team_last
        on fs_team_last.patient_key = stg_diabetes_population_active.patient_key
            and fs_team_last.diabetes_reporting_month = stg_diabetes_population_active.diabetes_reporting_month
            and fs_team_last.fs_rn = '1'
	left join last_diab_type
        on last_diab_type.patient_key = stg_diabetes_population_active.patient_key
            and last_diab_type.diabetes_reporting_month = stg_diabetes_population_active.diabetes_reporting_month
            and last_diab_type.fs_rn = '1'
	left join fs_diab_regimen
        on fs_diab_regimen.patient_key = stg_diabetes_population_active.patient_key
            and fs_diab_regimen.diabetes_reporting_month
                = stg_diabetes_population_active.diabetes_reporting_month
            and fs_diab_regimen.fs_rn = '1'
	left join {{ ref('stg_diabetes_recent_visit_indicators') }} as stg_diabetes_recent_visit_indicators
        on stg_diabetes_recent_visit_indicators.patient_key = stg_diabetes_population_active.patient_key
            and stg_diabetes_recent_visit_indicators.diabetes_reporting_month
                = stg_diabetes_population_active.diabetes_reporting_month
