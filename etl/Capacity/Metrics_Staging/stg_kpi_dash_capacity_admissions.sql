select
    capacity_ip_census_cohort.visit_key,
    capacity_ip_census_cohort.inpatient_census_admit_date,
    capacity_ip_census_cohort.admission_service,
    capacity_ip_census_cohort.admission_department_name,
    capacity_ip_census_cohort.admission_department_group_name,
    capacity_ip_census_cohort.admission_department_center_abbr,
    capacity_ip_census_cohort.ed_ind,
    capacity_ed_ip_admit_intervals.ed_mrft_to_admit_target_ind,
    case
      when capacity_transport_admit_intervals.visit_key is not null then 1
      else 0
    end as transport_ind,
    capacity_transport_admit_intervals.intake_to_enroute_target_ind

from
  {{ref('capacity_ip_census_cohort')}} as capacity_ip_census_cohort
    left join {{ref('capacity_ed_ip_admit_intervals')}} as capacity_ed_ip_admit_intervals
    on capacity_ed_ip_admit_intervals.visit_key = capacity_ip_census_cohort.visit_key
    left join {{ref('capacity_transport_admit_intervals')}} as capacity_transport_admit_intervals
    on capacity_transport_admit_intervals.visit_key = capacity_ip_census_cohort.visit_key
