{{ config(meta = {
    'critical': true
}) }}

-- Determines Covid-19 Patients who either 
--     1) Had an active infection during encounter (see stage table)
--     3) Were treated in the PSTU or SIU
-- Granularity - Visit Level    

with dx_pos_test as (
    select
        encounter_all.pat_key,
        encounter_all.visit_key,
        min(case
            when encounter_all.inpatient_ind = 1
                then stg_outbreak_covid_active_infections.start_date_ip
            else stg_outbreak_covid_active_infections.start_date end)
        as covid_start_date,
        max(case
            when encounter_all.inpatient_ind = 1
                then case when stg_outbreak_covid_active_infections.end_date_ip is null then 1 else 0 end
            when stg_outbreak_covid_active_infections.end_date is null then 1
            else 0
            end)
        as covid_active_ind,
        case when covid_active_ind = 0
            then max(case
                when encounter_all.inpatient_ind = 1
                    then stg_outbreak_covid_active_infections.end_date_ip
                else stg_outbreak_covid_active_infections.end_date end)
        end as covid_end_date,
        max(case when stg_outbreak_covid_active_infections.source_desc
            = 'positive covid test'
                then 1 else 0 end) as positive_covid_test_ind,
        max(case when stg_outbreak_covid_active_infections.source_desc
            = 'diagnosis'
                then 1 else 0 end) as covid_diagnosis_ind,
        max(case when stg_outbreak_covid_active_infections.source_desc
            = 'bugsy'
                then 1 else 0 end) as covid_bugsy_ind
    from
        {{ref('encounter_all')}} as encounter_all
        inner join {{ref('stg_outbreak_covid_active_infections')}}
            as stg_outbreak_covid_active_infections
            on stg_outbreak_covid_active_infections.pat_key
            = encounter_all.pat_key
    where
        encounter_all.encounter_date >= '2020-3-1'
        and encounter_all.cancel_noshow_ind = 0
        and (
        /*IP condition - encounter overlaps with active infection*/
        (encounter_all.inpatient_ind = 1
            and (encounter_all.hospital_admit_date
                between stg_outbreak_covid_active_infections.start_date_ip
                and coalesce(
                    stg_outbreak_covid_active_infections.end_date_ip, current_date)
                or stg_outbreak_covid_active_infections.start_date_ip
                between encounter_all.hospital_admit_date
                and coalesce(encounter_all.hospital_discharge_date, current_date)))
        /*Non-IP condition - day of encounter overlaps with active infection*/
        or (encounter_all.inpatient_ind = 0
            and encounter_all.encounter_date
            between date(stg_outbreak_covid_active_infections.start_date)
            and coalesce(date(stg_outbreak_covid_active_infections.end_date), current_date))
        )
    group by
        encounter_all.pat_key,
        encounter_all.visit_key
),

pstu_siu as (
    select
        pat_key,
        visit_key,
        max(case when unit_name = 'siu' then 1 else 0 end) as siu_ind,
        max(case when unit_name = 'pstu' then 1 else 0 end) as pstu_ind
    from
        {{ref('outbreak_covid_isolation')}}
     group by
        pat_key,
        visit_key
)

select
    coalesce(dx_pos_test.pat_key, pstu_siu.pat_key) as pat_key,
    coalesce(dx_pos_test.visit_key, pstu_siu.visit_key) as visit_key,
    dx_pos_test.covid_start_date,
    dx_pos_test.covid_end_date,
    dx_pos_test.covid_active_ind,
    coalesce(dx_pos_test.positive_covid_test_ind, 0)
    as positive_covid_test_ind,
    coalesce(dx_pos_test.covid_diagnosis_ind, 0) as covid_diagnosis_ind,
    coalesce(dx_pos_test.covid_bugsy_ind, 0) as covid_bugsy_ind,
    case when pstu_siu.visit_key is not null then 1 else 0
    end as pstu_siu_team_ind,
    coalesce(pstu_siu.pstu_ind, 0) as pstu_ind,
    coalesce(pstu_siu.siu_ind, 0) as siu_ind,
    encounter_all.inpatient_ind,
    encounter_inpatient.admission_department_center_abbr,
    --noqa: PRS
    trim(trailing ', ' from case when positive_covid_test_ind = 1 then 'covid test positive, ' else '' end
      || case when covid_diagnosis_ind = 1 then 'diagnosis, ' else '' end
      || case when covid_bugsy_ind = 1 then 'bugsy, ' else '' end
      || case when pstu_siu_team_ind = 1 then 'pstu or siu ind, ' else '' end
      ) as source_summary
from
    dx_pos_test
    full join pstu_siu on pstu_siu.visit_key = dx_pos_test.visit_key
    inner join {{ref('encounter_all')}} as encounter_all
        on encounter_all.visit_key
        = coalesce(dx_pos_test.visit_key, pstu_siu.visit_key)
    left join {{ref('encounter_inpatient')}} as encounter_inpatient
        on encounter_inpatient.visit_key
        = coalesce(dx_pos_test.visit_key, pstu_siu.visit_key)
