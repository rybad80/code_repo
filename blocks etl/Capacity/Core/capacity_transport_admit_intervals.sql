with transport_admissions as (
    -- in very rare cases an admission visit key can be duplicated
    select
        admit_visit_key,
        intake_date,
        service_accepted_date,
        transport_assigned_date,
        enroute_date,
        patient_contact_at_bedside_date,
        depart_referring_facility_date,
        team_available_date,
        intake_to_assigned_mins,
        intake_to_enroute_mins,
        assigned_to_enroute_mins,
        accepted_to_enroute_mins,
        bedside_arrive_to_bedside_depart_mins,
        facility_arrive_to_facility_depart_mins,
        intake_to_available_mins,
        lt60_ind as intake_to_enroute_target_ind,
        row_number() over (
             partition by admit_visit_key
             order by service_accepted_date desc
        ) as row_num
    from
        {{ref('transport_encounter_all')}}
    where
        cetdem_ind = 1
        and lower(transport_type) like '%inbound%'
        and lower(transport_type) not like '%interfacility%'
)

select
    capacity_ip_census_cohort.visit_key,
    capacity_ip_census_cohort.visit_event_key,
    capacity_ip_census_cohort.pat_key,
    capacity_ip_census_cohort.dept_key,
    capacity_ip_census_cohort.mrn,
    capacity_ip_census_cohort.csn,
    capacity_ip_census_cohort.patient_name,
    capacity_ip_census_cohort.dob,
    transport_admissions.intake_date,
    transport_admissions.service_accepted_date,
    transport_admissions.transport_assigned_date,
    transport_admissions.enroute_date,
    transport_admissions.patient_contact_at_bedside_date,
    transport_admissions.depart_referring_facility_date,
    capacity_ip_census_cohort.hospital_admit_date,
    capacity_ip_census_cohort.inpatient_census_admit_date,
    capacity_ip_census_cohort.ed_ind,
    capacity_ip_census_cohort.admission_department_center_abbr,
    transport_admissions.team_available_date,
    case
        when
            transport_admissions.intake_to_assigned_mins >= 0
        then
            transport_admissions.intake_to_assigned_mins
    end as intake_to_assigned_mins,
    case
        when
            transport_admissions.intake_to_enroute_mins >= 0
        then
            transport_admissions.intake_to_enroute_mins
    end as intake_to_enroute_mins,
    case
        when
            transport_admissions.assigned_to_enroute_mins >= 0
        then
            transport_admissions.assigned_to_enroute_mins
    end as assigned_to_enroute_mins,
    case
        when
            transport_admissions.accepted_to_enroute_mins >= 0
        then
            transport_admissions.accepted_to_enroute_mins
    end as accepted_to_enroute_mins,
    case
        when
            transport_admissions.bedside_arrive_to_bedside_depart_mins >= 0
        then
            transport_admissions.bedside_arrive_to_bedside_depart_mins
    end as bedside_arrive_to_bedside_depart_mins,
    case
        when
            transport_admissions.facility_arrive_to_facility_depart_mins >= 0
        then
            transport_admissions.facility_arrive_to_facility_depart_mins
    end as facility_arrive_to_facility_depart_mins,
    case
        when
            transport_admissions.intake_to_available_mins >= 0
        then
            transport_admissions.intake_to_available_mins
    end as intake_to_available_mins,
    intake_to_enroute_target_ind -- CHOP Enterprise Transport Dispatch Efficiency %
from
    {{ref('capacity_ip_census_cohort')}} as capacity_ip_census_cohort
    inner join transport_admissions
        on transport_admissions.admit_visit_key = capacity_ip_census_cohort.visit_key
        and row_num = 1
