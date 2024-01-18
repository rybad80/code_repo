{{ config(meta = {
    'critical': true
}) }}

with general_cohort as (-- bulk of visits use visit type or encounter type
    select
        visit_key,
        provider_key
    from
        {{ ref('stg_encounter') }}
    where
        department_id not in (
            101012165, -- 'bgr aadp multi d clnc', has its own logic
            1015002 -- 'interpreter departm*', not a real visit, always scheduled with another real visit
        )
        and (
            encounter_type_id = 76 --telemedecine
            or lower(visit_type) like '%video%visit%'
            or department_id in (
                101026010, -- telehealth urgent care
                101026013, -- buc telehlth urg care
                101026014, -- kop telehlth urg care
                101026015, -- bwv telehlth urg care
                101026016, -- hvford tlhlth urg care
                101035001, -- school telehealth
                101035002 -- employee benefits
            )
            or visit_type_id in 
            (  -- phil scribano child abuse visits in hunting park same thing as "care clinic new patient"
                '2124', -- VIDEO VISIT FOLLOW UP
				'2088', -- VIDEO VISIT NEW
				'2181', -- CARE CLINIC NEW PATIENT
				'2727', -- VIDEO VISIT EMPLOYEE
				'2566', -- VIDEO VISIT CARE MANAGEMENT
				'2127', -- VIDEO VISIT PAE
				'2146', -- VIDEO VISIT BH FOL UP 30
				'3027', -- VIDEO VISIT COMBINED
				'3029', -- VIDEO VISIT PRIMARY CARE
				'21261', -- VIDEO VISIT POST OP
				'3086', -- OFFICE VISIT W PSYCHIATRY
				'2150', -- VIDEO VISIT URGENT CARE
				'2318', -- VIDEO VISIT DIABETES
				'2500', -- VIDEO VISIT EMER MED
				'2546', -- ENGIN VIDEO VISIT NEW
				'2548', -- ENGIN VIDEO VISIT FOL UP
				'24951', -- VIDEO VISIT BH FOL UP
				'24941', -- VIDEO VISIT BH NEW
				'2746', -- VIDEO VISIT MC NEURO FOL UP
				'2755', -- VIDEO VISIT SLEEP FOL UP
				'2754', -- VIDEO VISIT SLEEP NEW
				'2784', -- VIDEO VISIT FP FOL UP
				'27851', -- VIDEO VISIT FP NEW
				'2790', -- VIDEO VISIT
				'27621', -- VID VIS TRANSITION NEW
				'27981', -- VID VIS TRANSITION F/U
				'2712', -- OASIS RECERT VIDEO VISIT
				'27221', -- NURSING VIDEO REVISIT
				'28161', -- VIDEO VISIT SECOND OP
				'2818', -- VIDEO VISIT INPATIENT FOL
				'2806', -- PHP VIDEO FOLLOW-UP
				'2805', -- PHP VIDEO NEW
				'10069', -- MYCHOP VIDEO VST PRIMARY CARE
				'28071', -- PHP VIDEO POST OP
				'2837', -- LIPID VIDEO VISIT FOLLOW UP
				'2846', -- VIDEO VISIT NEW LIPID
				'30811', -- VIDEO VISIT AIRWAY
				'3175', -- VIDEO VISIT FOL UP RARE LUNG
				'3176', -- VIDEO VISIT NEW RARE LUNG
				'31861', -- VIDEO VISIT BARIATRIC NEW
				'3187', -- VIDEO VISIT BARIATRIC FOL UP
				'31881', -- VIDEO NEW PT ORIENTATION
				'31621', -- VIDEO VISIT MC GI FOL UP
				'31631', -- VIDEO VISIT MC PULM FOL UP
				'27551', -- VIDEO VISIT SLEEP FOL UP
				'2124', -- VIDEO VISIT FOLLOW UP
				'3238', -- POSTNATAL CONSULT VIDEO VISIT
				'3249', -- PRENATAL VIDEO VISIT
				'3277', -- ENGIN VIDEO VISIT GC ONLY
				'3278', -- ENGIN VIDEO VISIT FU INTRNL
				'3295', -- VIDEO VISIT PASS
				'33171', -- VIDEO VISIT POST OP BARIATRICS
				'34311', -- ADOLESCENT WELL VIDEO VISIT
				'34691', -- MYCHOP VIDEO VISIT BH FOL UP
				'35331', -- VIDEO VISIT PSYCHOTHERAPY
				'3534', -- VIDEO NEW PSYCH EVAL
				'3566', -- NEW RESEARCH VIDEO VISIT
				'35671', -- RESEARCH VIDEO VISIT FOLLOW UP
				'35681', -- NEW RESEARCH TELEPHONE VISIT
				'3570', -- RESEARCH TELEPHONE VISIT F/U
				'3594', -- VIDEO VISIT NEW PREP CONSULT
				'36131', -- VIDEO VISIT LAB FU
				'3639', -- VIDEO KETOGENETIC FOL UP
				'3575', -- GROUP VIDEO VISIT BH
				'3640', -- VIDEO VISIT BH FOL UP 45
				'3696', -- CATCH TELEMED
				'2792', -- INJECTION
				'2988', -- GROWTH HORMONE
				'2795', -- INFINITY PUMP
				'2987', -- STRESS DOSE STEROIDS
				'2791', -- GLUCOMETER
				'3079', -- VIDEO VISIT TEACHING
				'2152', -- Telephone Visit [2152
				'2126', -- Video Visit Post Op [2126
				'2308', -- Video Visit Epic Cal - added June 2020
				'2785', -- Video Visit FP(Family Planning) New
				'2722', -- NURSING VIDEO REVISIT
				'2495', -- VIDEO VISIT BH FOL UP
				'2494', -- VIDEO VISIT BH NEW
				'2816', -- VIDEO VISIT SECOND OP
				'2762', -- VID VIS TRANSITION NEW
				'2798', -- VID VIS TRANSITION F/U
				'2807', -- PHP VIDEO POST OP
				'3186', -- VIDEO VISIT BARIATRIC NEW
				'3188', -- VIDEO NEW PT ORIENTATION
				'3162', -- VIDEO VISIT MC GI FOL UP
				'3163', -- VIDEO VISIT MC PULM FOL UP
				'3317', -- VIDEO VISIT POST OP BARIATRICS
				'3431', -- ADOLESCENT WELL VIDEO VISIT
				'3469', -- MYCHOP VIDEO VISIT BH FOL UP
				'35331', -- VIDEO VISIT PHYSCHOTHERAPY
				'3567' -- VIDEO VISIT
	   	        )          
        )
    group by
        visit_key,
        provider_key
),

mulitdisciplinary as (-- multidiciplinary visits have secondary encounter w/info re: providers involved
    select
        appt_visit_key.visit_key,
        row_number() over(partition by appt_visit_key.visit_key order by pat_enc_appt.line) as seq_num,
        dim_provider.provider_key
    from
        --search for "video visit" visit type from multidiciplinary clinic
        {{ ref('stg_encounter') }} as stg_encounter
        inner join {{ source('clarity_ods', 'pat_enc_appt') }} as pat_enc_appt
            on pat_enc_appt.pat_id = stg_encounter.pat_id
            and pat_enc_appt.contact_date = stg_encounter.encounter_date
        -- find associated visits from each dim_provider involved
        inner join {{ref('stg_encounter')}} as appt_visit_key
             on appt_visit_key.csn = pat_enc_appt.pat_enc_csn_id
        inner join {{ ref('dim_provider') }} as dim_provider
            on dim_provider.prov_id = pat_enc_appt.prov_id
    where
        stg_encounter.department_id = 101012165 --'bgr aadp multi d clnc'
        and lower(stg_encounter.visit_type) like 'video%visit%' --search for "video visit"
        --the appointment schedule slots should also be in bgr aadp multi d clnc
        and pat_enc_appt.department_id = 101012165
        and dim_provider.prov_id != '532666' --'aadp, filler prov'. the fake name used for "video visit".
        --shouldnt be counted bc these are not real visits
        --remove based on chart review. real visits have specific appointment time
        and pat_enc_appt.prov_start_time is not null
    group by
        appt_visit_key.visit_key,
        pat_enc_appt.line,
        dim_provider.provider_key
    ),

followup_telephone_encounters as (
    select
        stg_encounter.visit_key,
        stg_encounter.provider_key
    from
        {{ref('stg_encounter')}} as stg_encounter
        left join {{ref('stg_procedure_order_all_billing')}} as stg_procedure_order_all_billing
            on stg_encounter.csn = stg_procedure_order_all_billing.pat_enc_csn_id
    where
        stg_encounter.visit_type_id = '2152' -- telephone visit
        and (
            substr(stg_encounter.los_proc_cd, 1, 5) in ('99441', '99442', '99443') -- level of service
            or stg_procedure_order_all_billing.cpt_cd in ('99441', '99442', '99443') -- billed CPT code
        )
    group by
        stg_encounter.visit_key,
        stg_encounter.provider_key
),

inpatient_consults as (
    select
        stg_encounter_inpatient.visit_key,
        row_number() over(partition by hno_info.pat_enc_csn_id order by hno_info.note_id) as seq_num,
        dim_provider.provider_key
    from
        {{ source('clarity_ods', 'smrtdta_elem_data') }} as smrtdta_elem_data
        inner join {{ source('clarity_ods', 'hno_info') }} as hno_info
            on hno_info.note_id = smrtdta_elem_data.record_id_varchar
        inner join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
            on stg_encounter_inpatient.pat_enc_csn_id = hno_info.pat_enc_csn_id
        inner join {{ source('clarity_ods','clarity_emp') }} as clarity_emp
            on clarity_emp.user_id = hno_info.current_author_id
        inner join {{ ref('dim_provider') }} as dim_provider
            on dim_provider.prov_id = clarity_emp.prov_id
    where
        --used smart data element
        lower(smrtdta_elem_data.element_id) = 'chop#6176'
        and lower(smrtdta_elem_data.context_name) = 'note'
    group by
        stg_encounter_inpatient.visit_key,
        hno_info.pat_enc_csn_id,
        hno_info.note_id,
        dim_provider.provider_key
),

cohort as (
    select
        stg_encounter.visit_key,
        coalesce(
            mulitdisciplinary.seq_num,
            inpatient_consults.seq_num,
            1
            ) as encounter_provider_seq_num,
        case
            when general_cohort.visit_key is not null then 'general'
            when mulitdisciplinary.visit_key is not null then 'multidisciplinary'
            when followup_telephone_encounters.visit_key is not null then 'followup telephone'
            when inpatient_consults.visit_key is not null then 'inpatient consults'
        end as cohort_logic,
        coalesce(
            general_cohort.provider_key,
            mulitdisciplinary.provider_key,
            followup_telephone_encounters.provider_key,
            inpatient_consults.provider_key
        ) as provider_key
    from
        {{ref('stg_encounter')}} as stg_encounter
        left join general_cohort
            on general_cohort.visit_key = stg_encounter.visit_key
        left join mulitdisciplinary
            on mulitdisciplinary.visit_key = stg_encounter.visit_key
        left join followup_telephone_encounters
            on followup_telephone_encounters.visit_key = stg_encounter.visit_key
        left join inpatient_consults
            on inpatient_consults.visit_key = stg_encounter.visit_key
    where
        coalesce(
            general_cohort.visit_key,
            mulitdisciplinary.visit_key,
            followup_telephone_encounters.visit_key,
            inpatient_consults.visit_key
        ) is not null
)

select
    {{
        dbt_utils.surrogate_key([
            'cohort.visit_key',
            'cohort.encounter_provider_seq_num'
        ])
    }} as visit_provider_seq_key,
    cohort.visit_key,
    cohort.encounter_provider_seq_num,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.dob,
    stg_encounter.age_years,
    stg_encounter.encounter_date,
    cohort.cohort_logic,
    dim_provider.prov_id as provider_id,
    initcap(dim_provider.full_name) as provider_name,
    upper(dim_provider.provider_primary_specialty) as provider_specialty,
    stg_encounter.csn,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.pat_key,
    cohort.provider_key
from
    cohort
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = cohort.visit_key
    inner join {{ ref('dim_provider') }} as dim_provider
        on dim_provider.provider_key = cohort.provider_key
