with drg as (
    select
        cast(hsp_acct_mult_drgs.hsp_account_id as numeric) as hsp_account_id,
        cast(hsp_acct_mult_drgs.drg_id as varchar(20)) as drg_id,
        dense_rank() over (
            partition by hsp_acct_mult_drgs.hsp_account_id
            order by identity_id_type.id_type_name desc, hsp_acct_mult_drgs.line
        ) as recrank
    from
        {{source('clarity_ods','hsp_acct_mult_drgs')}} as hsp_acct_mult_drgs
        left join {{source('clarity_ods','identity_id_type')}} as identity_id_type
            on identity_id_type.id_type = hsp_acct_mult_drgs.drg_id_type_id
        inner join {{ref('dim_hospital_drg')}} as dim_hospital_drg
            on dim_hospital_drg.drg_id = hsp_acct_mult_drgs.drg_id
            and lower(identity_id_type.id_type_name) like 'drg apr%'
        inner join {{source('clarity_ods','hsp_account')}} as hsp_account
            on hsp_account.hsp_account_id = hsp_acct_mult_drgs.hsp_account_id
    where
        substr(dim_hospital_drg.drg_number, 4, 3) not in (
            '540',  /* CESAREAN DELIVERY */
            '541',  /* VAGINAL DELIVERY W STERILIZATION &/OR D&C */
            '542',  /* VAGINAL DELIVERY W COMPLICATING PROCEDURES EXC STERILIZATION &/OR D&C */
            '546',  /* OTHER O.R. PROC FOR OBSTETRIC DIAGNOSES EXCEPT DELIVERY DIAGNOSES */
            '560',  /* VAGINAL DELIVERY */
            '561',  /* POSTPARTUM & POST ABORTION DIAGNOSES W/O PROCEDURE */
            '563',  /* PRETERM LABOR */
            '565',  /* FALSE LABOR */
            '566',  /* OTHER ANTEPARTUM DIAGNOSES */
            '950',  /* EXTENSIVE PROCEDURE UNRELATED TO PRINCIPAL DIAGNOSIS */
            '951',  /* MODERATELY EXTENSIVE PROCEDURE UNRELATED TO PRINCIPAL DIAGNOSIS */
            '952',  /* NONEXTENSIVE PROCEDURE UNRELATED TO PRINCIPAL DIAGNOSIS  */
            '956'   /* UNGROUPABLE */
        )
    and coalesce(disch_date_time, current_date) >= '2017-07-01'

)

select
    cast(encounter_inpatient.hsp_account_id as numeric) as hsp_account_id,
    coalesce(
       proclkup_drg_id,
       proclkupspec_drg_id,
       dxlkup_drg_id
    ) as drg_id
     from
        {{ ref('encounter_inpatient') }} as encounter_inpatient
        left join ( --noqa: L042
            select
                hsp_account_id
            from
                {{source('clarity_ods','hsp_acct_mult_drgs')}} as hsp_acct_mult_drgs
                inner join {{source('clarity_ods','identity_id_type')}} as identity_id_type
                    on identity_id_type.id_type = hsp_acct_mult_drgs.drg_id_type_id
                    and lower(identity_id_type.id_type_name) like 'drg apr%'
            group by
                hsp_account_id
        ) as hardrg on encounter_inpatient.hsp_account_id = hardrg.hsp_account_id
        /* would get if drg loaded for har (from clarity for inpatient, from phis for obs)
        but in this part of the union, we are getting ones that do not have this corresponding record
        have to try the procedure and dx derived drg look-ups since the set drg for the har is not present */
        left join  ( --noqa: L042
            select
                fact_hospital_account_diagnosis.hsp_account_id,
                max(lookup_drg_procedure_xref.drg_id) as proclkup_drg_id
            from
                {{ref('fact_hospital_account_diagnosis')}} as fact_hospital_account_diagnosis
                inner join {{ref('lookup_drg_procedure_xref')}} as lookup_drg_procedure_xref
                    on fact_hospital_account_diagnosis.icd_code = lookup_drg_procedure_xref.icd_code
                    and fact_hospital_account_diagnosis.icd10_ind
                    = lookup_drg_procedure_xref.icd10_ind
            where /* use only the first proc ref cd */
                fact_hospital_account_diagnosis.line = 1
            group by
                fact_hospital_account_diagnosis.hsp_account_id
        ) as procdrg on encounter_inpatient.hsp_account_id = procdrg.hsp_account_id
        left join ( --noqa: L042
            select
                fact_hospital_account_diagnosis.hsp_account_id,
                max(lookup_drg_procedure_xref.drg_id) as proclkupspec_drg_id
            from
                {{ref('fact_hospital_account_diagnosis')}} as fact_hospital_account_diagnosis
                inner join {{ref('lookup_drg_procedure_xref')}} as lookup_drg_procedure_xref
                    on fact_hospital_account_diagnosis.icd_code = lookup_drg_procedure_xref.icd_code
                    and fact_hospital_account_diagnosis.icd10_ind
                    = lookup_drg_procedure_xref.icd10_ind
                inner join {{ref('dim_hospital_drg')}} as dim_hospital_drg
                    on lookup_drg_procedure_xref.drg_id = dim_hospital_drg.drg_id
                    and lower(dim_hospital_drg.drg_name) = 'tonsil & adenoid procedures'
                group by
                    fact_hospital_account_diagnosis.hsp_account_id
        ) as procdrgspec on encounter_inpatient.hsp_account_id = procdrgspec.hsp_account_id
        left join  ( --noqa: L042
            select
                encounter_inpatient.hsp_account_id,
                lookup_drg_diagnosis_xref.drg_id as dxlkup_drg_id
            from
                {{ref('encounter_inpatient')}} as encounter_inpatient
                inner join {{ref('stg_dx_visit_diagnosis_long')}} as stg_dx_visit_diagnosis_long
                    on stg_dx_visit_diagnosis_long.pat_enc_csn_id = encounter_inpatient.csn
                    and stg_dx_visit_diagnosis_long.src = 'HSP_ACCT_DX_LIST'
                    and stg_dx_visit_diagnosis_long.line = 1
                    and encounter_inpatient.hsp_account_id is not null
                inner join {{ref('lookup_drg_diagnosis_xref')}} as lookup_drg_diagnosis_xref
                    on stg_dx_visit_diagnosis_long.dx_id = lookup_drg_diagnosis_xref.dx_id
        ) as dxdrg on encounter_inpatient.hsp_account_id = dxdrg.hsp_account_id
     where
        lower(encounter_inpatient.hsp_acct_patient_class) = 'observation'
        and coalesce(encounter_inpatient.hospital_discharge_date, current_date) >= '2017-07-01'
        /* the assigned drg for the har is not present  */
        and hardrg.hsp_account_id is null
        and (/* but it could be derived by procedure ref_cd or by dx  */
            proclkup_drg_id is not null
            or proclkupspec_drg_id is not null
            or dxlkup_drg_id is not null
        )

    union

    select
        hsp_account_id,
        drg_id
    from
        drg
    where
        recrank = 1
