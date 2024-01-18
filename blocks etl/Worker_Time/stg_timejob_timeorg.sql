{{ config(meta = {
    'critical': true
}) }}

/* stg_timejob_timeorg
gather Kronos job category (timejob) discrete fields and the path and IDs for the
organizations related to the timereport (kronos_wfctotal record)
and set the safty obs instance INDs for the scenarios when derived from this
*/
select
    kronos_job_org.wfcjoborgid  as job_organization_id,
    kronos_job_org.orgpathtxt as timereport_org_path,
    kronos_job_org.wfcjobid  as timejob_id,
    kronos_job.wfcjobnm as timejob_abbreviation,
    kronos_job.wfcjobdsc  as timejob_name,
    case when
        timejob_abbreviation = 'Safety Obs' -- 'Safety Observation'
        then 1 else 0
    end as orgpath_safety_obs_ind,
    case when
        timejob_abbreviation = 'MOOR' -- 'Meals Out of Room'
        then 1 else 0
    end as orgpath_meal_out_of_room_ind,
    case when
        timejob_abbreviation = 'Charge' -- 'Charge Nurse'
        then 1 else 0
    end as charge_ind,
    case when
        timejob_abbreviation = 'BHC Charge' -- 'Charge role for safety observations BHC staff'
        then 1 else 0
    end as orgpath_bhc_charge_ind,
    kronos_job_org.orgpathdsctxt as timereport_org_path_description,
    kronos_job_org.laboracctid as joborg_labor_accounting_id,
    kronos_job_org.lev1orgidsid as timeorg_level_1_id,
    kronos_job_org.lev2orgidsid as timeorg_level_2_id,
    kronos_job_org.lev3orgidsid as timeorg_level_3_id,
    kronos_job_org.lev1orgidsid as timeorg_level_4_id,
    kronos_job_org.lev5orgidsid as timeorg_level_5_id,
    kronos_job_org.lev6orgidsid as timeorg_level_6_id
from
    {{ source('kronos_ods', 'wfcjoborg') }} as kronos_job_org
    left join {{ source('kronos_ods', 'kronos_wfcjob') }} as kronos_job
        on kronos_job_org.wfcjobid  = kronos_job.wfcjobid
