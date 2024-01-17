with
visit_level_changes as (
select cohort.visit_key,
date(max(case when fa.flowsheet_id = 40071670 then fa.recorded_date else null end)) as most_recent_chg,
date(max(case when fa.flowsheet_id = 400718571 then fa.recorded_date else null end)) as most_recent_linen_change
from {{ ref('stg_vas_rounds_cohort_visits') }} as cohort
inner join {{ ref('flowsheet_all') }} as fa on cohort.visit_key = fa.visit_key
where
(fa.flowsheet_id =  40071670 and upper(fa.meas_val) like '%CHLOR%') --CHG 
or fa.flowsheet_id = 400718571 --linen change
group by cohort.visit_key
)

select ea.visit_key, ea.current_cicu_bed_name,
max(ea.patient_name) as pt_name,
max(ea.csn_number) as csn_number,
max(ea.mrn) as mrn,
max(ea.hospital_admit_date) as hospital_admit_date,
coalesce(max(ea.hospital_discharge_date), current_date) as end_date,
sum(
    case when vas_rounds_line_data.line_cat = 'Central' and remove_dt is null then num_lumens else 0 end
) as total_central_lumens,
sum(
    case when vas_rounds_line_data.line_cat = 'Midline' and remove_dt is null then num_lumens else 0 end
) as total_midline_lumens,
sum(case when ic_ind = 1  and remove_dt is null then num_lumens else 0 end) as total_intracardiac_lumes,
sum(
    case when vas_rounds_line_data.line_cat = 'PIV'  and remove_dt is null then num_lumens else 0 end
) as total_peripheral_lumens,
sum(
    case when vas_rounds_line_data.line_cat = 'Arterial'  and remove_dt is null then num_lumens else 0 end
) as total_arterial_lumens,
visit_level_changes.most_recent_chg,
visit_level_changes.most_recent_linen_change
from {{ ref('stg_vas_rounds_cohort_visits') }} as ea
left join {{ ref('stg_vas_rounds_line_data') }} as vas_rounds_line_data on
 ea.visit_key = vas_rounds_line_data.visit_key
left join visit_level_changes on vas_rounds_line_data.visit_key = visit_level_changes.visit_key
where ea.hospital_admit_date >= '2019-01-01'
group by
    ea.visit_key,
    visit_level_changes.most_recent_chg,
    visit_level_changes.most_recent_linen_change,
    ea.current_cicu_bed_name
