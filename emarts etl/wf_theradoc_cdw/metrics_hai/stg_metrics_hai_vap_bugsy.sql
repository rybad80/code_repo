with surg as (
    select
        infection_surveillance_surgery.inf_surv_key,
        infection_surveillance_surgery.log_key,
        infection_surveillance_surgery.surg_rank,
        row_number() over (
            partition by infection_surveillance_surgery.inf_surv_key
            order by infection_surveillance_surgery.surg_rank asc
        ) as rn

    from
        {{ref('infection_surveillance_surgery')}} as infection_surveillance_surgery
),

micro as (
    select
        infection_surveillance.inf_surv_key,
        infection_surveillance.pat_key,
        infection_surveillance.dept_key,
        infection_surveillance.inf_surv_id,
        infection_surveillance_micro.inf_micro_nm,
        infection_surveillance_micro.test_nm,
        infection_surveillance_micro.micro_rank,
        infection_surveillance_micro.organism_nm,
        row_number() over (
                            partition by infection_surveillance.inf_surv_key
                            order by
                                case
                                    when upper(infection_surveillance_micro.inf_micro_nm) like '%PCR%' then -2
                                    when upper(infection_surveillance_micro.inf_micro_nm) like '%CULTURE%' then -1
                                    else 0
                                end,
        infection_surveillance_micro.micro_rank asc) as rn

    from
        {{ref('infection_surveillance')}} as infection_surveillance
        inner join {{ref('infection_surveillance_micro')}} as infection_surveillance_micro
            on infection_surveillance_micro.inf_surv_key = infection_surveillance.inf_surv_key
),

surv_order_organism as (
    select distinct
        inf_surv_key,
        pat_key,
        dept_key,
        inf_surv_id,
        inf_micro_nm,
        organism_nm,
        max(decode(rn, 1, organism_nm, null))
            over (partition by inf_surv_key) as organism_01,
        max(decode(rn, 2, organism_nm, null))
            over (partition by inf_surv_key) as organism_02,
        max(decode(rn, 3, organism_nm, null))
            over (partition by inf_surv_key) as organism_03,
        max(decode(rn, 4, organism_nm, null))
            over (partition by inf_surv_key) as organism_04
    from
        micro
)

select distinct
    visit.visit_key as visit_key,
    patient.pat_key as pat_key,
    department.dept_key as dept_key,
    'VAP'::varchar(10) as hai_type,
    master_date.dt_key as event_dt_key,
    infection_surveillance.inf_surv_id as eventid,
    patient.pat_mrn_id::bigint as patid,
    upper(patient.first_nm)::varchar(50) as patgname,
    upper(patient.last_nm)::varchar(50) as patsurname,
    infection_surveillance.nhsn_exp_loc_cd::varchar(7) as location,
    null::varchar(50) as or_location,
    bugsy_custom_infection_classes.eventtype::char(4) as eventtype,
    infection_surveillance.inf_dt as eventdate,
    visit.enc_id::bigint as comment_fld,
    case
        when upper(surv_order_organism.organism_01) = 'POSITIVE' then 'POSITIVE FOR ' || upper(surv_order_organism.inf_micro_nm)
        else upper(surv_order_organism.organism_01)
    end::varchar(500) as pathogendesc1,
    case
        when upper(surv_order_organism.organism_02) = 'POSITIVE' then 'POSITIVE FOR ' || upper(surv_order_organism.inf_micro_nm)
        else upper(surv_order_organism.organism_02)
    end::varchar(500) as pathogendesc2,
    case
        when upper(surv_order_organism.organism_03) = 'POSITIVE' then 'POSITIVE FOR ' || upper(surv_order_organism.inf_micro_nm)
        else upper(surv_order_organism.organism_03)
    end::varchar(500) as pathogendesc3,
    bugsy_custom_infection_classes.centralline,
    bugsy_custom_infection_classes.umbcatheter::varchar(6) as umbcatheter,
    bugsy_custom_infection_classes.urinarycath::varchar(10) as urinarycath,
    bugsy_custom_infection_classes.ventused::varchar(6) as ventused,
    (infection_surveillance.inf_dt::date - visit.hosp_admit_dt::date)::bigint as admtoevntdays,
    visit.hosp_admit_dt as admitdate,
    round(days_between(infection_surveillance.inf_dt, patient.dob) / 365.0, 2)::numeric(7, 4) as ageatevent,
    patient.ped_birth_wt_in_kg * 1000 as birthwt,
    case
        when (patient.ped_birth_wt_in_kg * 1000) <= 750 then 'A'
        when (patient.ped_birth_wt_in_kg * 1000) > 750 and (patient.ped_birth_wt_in_kg * 1000) <= 1000 then 'B'
        when (patient.ped_birth_wt_in_kg * 1000) > 1000 and (patient.ped_birth_wt_in_kg * 1000) <= 1500 then 'C'
        when (patient.ped_birth_wt_in_kg * 1000) > 1500 and (patient.ped_birth_wt_in_kg * 1000) <= 2500 then 'D'
        when (patient.ped_birth_wt_in_kg * 1000) > 2500 then 'E'
    end as birthwtcode,
    case
        when (patient.ped_birth_wt_in_kg * 1000) <= 750 then 'A'
        when (patient.ped_birth_wt_in_kg * 1000) > 750 and (patient.ped_birth_wt_in_kg * 1000) <= 1000 then 'B'
        when (patient.ped_birth_wt_in_kg * 1000) > 1000 and (patient.ped_birth_wt_in_kg * 1000) <= 1500 then 'C'
        when (patient.ped_birth_wt_in_kg * 1000) > 1500 and (patient.ped_birth_wt_in_kg * 1000) <= 2500 then 'D'
        when (patient.ped_birth_wt_in_kg * 1000) > 2500 then 'E'
    end as birthwtcodedesc,
    null as coatedcath,
    case
        when infection_surveillance.work_status = 'COMPLETE' then 'Y'
        when infection_surveillance.work_status != 'COMPLETE' then 'N'
    end as completedflag,
    bugsy_custom_infection_classes.contribdeath,
    null::varchar(9) as devinsertdate,
    null::varchar(7) as devinsertloc,
    bugsy_custom_infection_classes.died,
    patient.dob as dob,
    bugsy_custom_infection_classes.eventtypedesc::varchar(33) as eventtypedesc,
    patient.sex as gender,
    bugsy_custom_infection_classes.implant,
    surg.log_key::varchar(7) as linkedproc,
    infection_surveillance.nhsn_exp_loc_cd::varchar(21) as loclabel,
    infection_surveillance.upd_dt as modifydate,
    null::varchar(5) as multiproc,
    max(case
            when infection_surveillance.op_ind = 1 then 'Y'
            when infection_surveillance.op_ind != 1 then 'N'
    end) over (partition by infection_surveillance.inf_surv_id) as outpatient,
    max(case
            when surv_order_organism.organism_01 is not null then 'Y'
            when surv_order_organism.organism_01 is null then 'N'
    end) over (partition by infection_surveillance.inf_surv_id) as pathidentified,
    pathogen_lookup1.pathogen::varchar(50) as pathogen1,
    pathogen_lookup2.pathogen::varchar(50) as pathogen2,
    pathogen_lookup3.pathogen::varchar(50) as pathogen3,
    bugsy_custom_infection_classes.permcentralline,
    null as port,
    null as postproc,
    null::varchar(5) as proccode,
    null::varchar(36) as proccodedesc,
    null as procdate

from
    {{ref('infection_surveillance')}} as infection_surveillance
    left join {{ref('infection_surveillance_visit')}} as infection_surveillance_visit
        on infection_surveillance_visit.inf_surv_key = infection_surveillance.inf_surv_key
    left join {{ref('visit')}} as visit
        on visit.visit_key = infection_surveillance_visit.visit_key
    left join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = infection_surveillance.pat_key
    left join {{source('cdw', 'department')}} as department
        on department.dept_key = infection_surveillance.dept_key
    left join {{source('cdw', 'master_date')}} as master_date
        on master_date.full_dt = infection_surveillance.inf_dt
    left join surg
        on surg.inf_surv_key = infection_surveillance.inf_surv_key
            and surg.rn = 1
    left join {{ref('infection_surveillance_micro')}} as infection_surveillance_micro
        on infection_surveillance_micro.inf_surv_key = infection_surveillance.inf_surv_key
    left join surv_order_organism
        on infection_surveillance.inf_surv_key = surv_order_organism.inf_surv_key
    left join {{ref('bugsy_custom_infection_classes')}} as bugsy_custom_infection_classes
        on infection_surveillance.inf_surv_id = bugsy_custom_infection_classes.c54_td_ica_surv_id
    left join {{ref('metrics_hai_pathogen_lookup')}} as pathogen_lookup1
        on upper(surv_order_organism.organism_01) = upper(pathogen_lookup1.organism_nm)
    left join {{ref('metrics_hai_pathogen_lookup')}} as pathogen_lookup2
        on upper(surv_order_organism.organism_02) = upper(pathogen_lookup2.organism_nm)
    left join {{ref('metrics_hai_pathogen_lookup')}} as pathogen_lookup3
        on upper(surv_order_organism.organism_03) = upper(pathogen_lookup3.organism_nm)

where
    (infection_surveillance_visit.seq_num is null or infection_surveillance_visit.seq_num = 1)
    and infection_surveillance.work_status = 'COMPLETE'
    and infection_surveillance.inf_acq_type = 'HOSPITAL-ASSOCIATED'
    and upper(infection_surveillance.assigned_to_icp) not like '%SIGN%'
    and infection_surveillance.assigned_to_icp not like '%OTHER%'
    and infection_surveillance.assigned_to_icp not like '%PRESENT%'
    and infection_surveillance.create_by = 'BUGSY'
    and bugsy_custom_infection_classes.infection_event = 'Healthcare-Associated Infection'
    and bugsy_custom_infection_classes.eventtype = 'PNEU'
    and bugsy_custom_infection_classes.lda_associated_yn = 'Y'
