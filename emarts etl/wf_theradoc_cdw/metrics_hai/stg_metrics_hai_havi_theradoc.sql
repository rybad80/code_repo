with exclusion as (
    select
        infection_surveillance_class.inf_surv_key,
        1 as exclusion_ind

    from
        {{ref('infection_surveillance_class')}} as infection_surveillance_class

    where
        upper(infection_surveillance_class.inf_surv_cls_nm) like '%NOT A RELEVANT INFECTION%'

    group by
        infection_surveillance_class.inf_surv_key
),

surg as (
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

infclass as (
    select
        infection_surveillance_class.inf_surv_key,
        max(
            case
                when infection_surveillance_class.inf_surv_cls_nm like '%Pneumonia%' then 'HAVI'
                when infection_surveillance_class.inf_surv_cls_nm like '%EENT%' then 'HAVI'
                when infection_surveillance_class.inf_surv_cls_nm like '%GI%' then 'HAVI'
                when infection_surveillance_class.inf_surv_cls_nm like '%LRI%' then 'HAVI'
        end) as hai_type,
        max(
            case
                when infection_surveillance_class.inf_surv_cls_nm like '%Pneumonia%' then 'PNEU'
                when infection_surveillance_class.inf_surv_cls_nm like '%EENT%' then 'EENT'
                when infection_surveillance_class.inf_surv_cls_nm like '%GI%' then 'GI'
                when infection_surveillance_class.inf_surv_cls_nm like '%LRI%' then 'LRI'
        end) as event_type,
        max(case when infection_surveillance_class.inf_surv_cls_nm like '%Upper respiratory%' then 'Y' end) as uri_ind,
        max(case when infection_surveillance_class.inf_surv_cls_nm like '%Central-line associated%' then 'Y' end) as centralline,
        max(case when upper(infection_surveillance_class.inf_surv_cls_ansr) like '%UMB%' then 'Y' end::varchar(6)) as umbcatheter,
        max(
            case
                when infection_surveillance_class.inf_surv_cls_ansr = 'uti_urin_cath_rem_pr_coll' then 'REMOVE'
                when infection_surveillance_class.inf_surv_cls_ansr = 'uti_urin_cath_plc_at_coll' then 'IN PLACE'
        end::varchar(10)) as urinarycath,
        max(case when infection_surveillance_class.inf_surv_cls_nm like '%Ventilator-associated%' then 'Y' end::varchar(6)) as ventused,
        max(
            case
                when infection_surveillance_class.inf_surv_cls_ansr like '%perm%' then 'Y'
                when infection_surveillance_class.inf_surv_cls_ansr != 'perm' then 'N'
        end) as permcentralline,
        max(case when infection_surveillance_class.inf_surv_cls_nm like '%contributed to death%' then 'Y' end) as contribdeath,
        max(
            case
                when infection_surveillance_class.inf_surv_cls_nm = 'Patient died' then 'Y'
                when infection_surveillance_class.inf_surv_cls_nm != 'Patient died' then 'N'
        end) as died,
        max(case when upper(infection_surveillance_class.inf_surv_cls_nm) = 'IMPLANT' then 'Y' end) as implant

    from
        {{ref('infection_surveillance_class')}} as infection_surveillance_class

    group by
        infection_surveillance_class.inf_surv_key
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
    visit.visit_key,
    patient.pat_key,
    dept.dept_key,
    infclass.hai_type::varchar(10) as hai_type,
    master_date.dt_key as event_dt_key,
    surv.inf_surv_id as eventid,
    patient.pat_mrn_id::bigint as patid,
    upper(patient.first_nm)::varchar(50) as patgname,
    upper(patient.last_nm)::varchar(50) as patsurname,
    surv.nhsn_exp_loc_cd::varchar(7) as location,
    null::varchar(50) as or_location,
    trim(infclass.event_type)::char(4) as eventtype,
    surv.inf_dt as eventdate,
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
    null as centralline,
    infclass.umbcatheter,
    infclass.urinarycath,
    infclass.ventused,
    (surv.inf_dt::date - visit.hosp_admit_dt::date)::bigint as admtoevntdays,
    visit.hosp_admit_dt as admitdate,
    round(days_between(surv.inf_dt, patient.dob) / 365.0, 2)::numeric(7, 4) as ageatevent,
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
        when surv.work_status = 'COMPLETE' then 'Y'
        when surv.work_status != 'COMPLETE' then 'N'
    end as completedflag,
    infclass.contribdeath,
    null::varchar(9) as devinsertdate,
    null::varchar(7) as devinsertloc,
    infclass.died,
    patient.dob,
    infclass.event_type::varchar(33) as eventtypedesc,
    patient.sex as gender,
    infclass.implant,
    surg.log_key::varchar(7) as linkedproc,
    --SURG.SURG_RANK,
    surv.nhsn_exp_loc_cd::varchar(21) as loclabel,
    surv.upd_dt as modifydate,
    null::varchar(5) as multiproc,
    max(
        case
            when surv.op_ind = 1 then 'Y'
            when surv.op_ind != 1 then 'N'
    end) over (partition by surv.inf_surv_id) as outpatient,
    --SURV.OP_IND,
    max(
        case
            when surv_order_organism.organism_01 is not null then 'Y'
            when surv_order_organism.organism_01 is null then 'N'
    end) over (partition by surv.inf_surv_id) as pathidentified,
    pathogen_lookup1.pathogen::varchar(50) as pathogen1,
    pathogen_lookup2.pathogen::varchar(50) as pathogen2,
    pathogen_lookup3.pathogen::varchar(50) as pathogen3,
    infclass.permcentralline,
    null as port,
    null as postproc,
    null::varchar(5) as proccode,
    null::varchar(36) as proccodedesc,
    null as procdate

from
    {{ref('infection_surveillance')}} as surv
    left join {{ref('infection_surveillance_visit')}} as survvisit
        on survvisit.inf_surv_key = surv.inf_surv_key
    left join {{ref('visit')}} as visit
        on visit.visit_key = survvisit.visit_key
    left join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = surv.pat_key
    left join {{source('cdw', 'department')}} as dept
        on dept.dept_key = surv.dept_key
    left join {{source('cdw', 'master_date')}} as master_date
        on master_date.full_dt = surv.inf_dt
    left join surg
        on surg.inf_surv_key = surv.inf_surv_key
            and surg.rn = 1
    left join {{ref('infection_surveillance_micro')}} as survorder
        on survorder.inf_surv_key = surv.inf_surv_key
    inner join infclass
        on infclass.inf_surv_key = surv.inf_surv_key
            and infclass.hai_type = 'HAVI'
    left join surv_order_organism
        on surv.inf_surv_key = surv_order_organism.inf_surv_key
    left join exclusion
        on exclusion.inf_surv_key = surv.inf_surv_key
    left join {{ref('metrics_hai_pathogen_lookup')}} as metrics_hai_pathogen_lookup
        on upper(survorder.organism_nm) = upper(metrics_hai_pathogen_lookup.organism_nm)
    left join {{ref('metrics_hai_pathogen_lookup')}} as pathogen_lookup1
        on upper(surv_order_organism.organism_01) = upper(pathogen_lookup1.organism_nm)
    left join {{ref('metrics_hai_pathogen_lookup')}} as pathogen_lookup2
        on upper(surv_order_organism.organism_02) = upper(pathogen_lookup2.organism_nm)
    left join {{ref('metrics_hai_pathogen_lookup')}} as pathogen_lookup3
        on upper(surv_order_organism.organism_03) = upper(pathogen_lookup3.organism_nm)

where (survvisit.seq_num is null or survvisit.seq_num = 1)
    and surv.work_status = 'COMPLETE'
    and surv.create_by = 'THERADOC'
    and surv.inf_acq_type = 'HOSPITAL-ASSOCIATED'
    and upper(surv.assigned_to_icp) not like '%SIGN%'
    and surv.assigned_to_icp not like '%OTHER%'
    and surv.assigned_to_icp not like '%PRESENT%'
    and exclusion_ind is null
    -- include records positive for NHSN viral or have both an EENT and URI, exclude records where a ventilator was used
    and (
        metrics_hai_pathogen_lookup.pathogen in ('ADV', 'PFLU', 'RHNV', 'RSV', 'FLUA', 'FLUB', 'VIRUS', 'H1N1', 'RESPV', 'ASTTU', 'ENTRO', 'NORO', 'ROTA', 'CORTU', 'META', 'HMPV', 'SAPO', 'CORV')
        or (infclass.event_type = 'EENT' and infclass.uri_ind = 'Y')
    )
    and infclass.ventused is null
