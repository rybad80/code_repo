with all_meds as (

    select --noqa: L034
        cohort.pathway_infection_key,
        cohort.pat_key,
        cohort.visit_key,
        cohort.infection,
        asp_outpatient_prescription.abx_name as gen_nm_cln_sub,
        medication_order_administration.medication_order_create_date,
        asp_outpatient_prescription.sig_text_lower as sig,

        case when order_med.discon_time is null
            then to_char(current_timestamp, 'YYYY-MM-DD HH:MI:SS')
            else
                timezone(order_med.discon_time, 'UTC', 'America/New_York')
        end as discont_dt_or_today,

        extract(
            epoch from discont_dt_or_today --noqa: L027
            - medication_order_administration.medication_order_create_date
        ) / 60.0 as med_create_to_disct_min,

        --to get the latest order
        rank() over (
            partition by
                cohort.pathway_infection_key
            order by
                medication_order_administration.medication_order_create_date desc
        ) as med_ord_rank,

        --to get rid of the same med ordered twice
        row_number() over (
            partition by
                cohort.pathway_infection_key, gen_nm_cln_sub
            order by gen_nm_cln_sub
        ) as med_dup_rank,

        asp_outpatient_prescription.outpatient_duration_days as rx_days

    from {{ ref('stg_pathway_infection_cohort') }} as cohort --noqa: L031
        inner join {{ ref('asp_outpatient_prescription') }} as asp_outpatient_prescription --noqa: L031
            on cohort.visit_key = asp_outpatient_prescription.visit_key
        inner join {{ ref('medication_order_administration') }} as medication_order_administration
            on asp_outpatient_prescription.med_ord_key = medication_order_administration.med_ord_key
        inner join {{ source('clarity_ods', 'order_med') }} as order_med
            on asp_outpatient_prescription.medication_order_id = order_med.order_med_id
    where
        medication_order_administration.medication_end_date
            > medication_order_administration.medication_start_date
        and medication_order_administration.order_status != 'Canceled'
        and med_create_to_disct_min / 60.0 >= 24
        and gen_nm_cln_sub not in (
            'Linezolid',
            'Cefepime',
            'Meropenem',
            'Penicillin G',
            'Vancomycin',
            'Mupirocin',
            'Doxycycline'
        )
        --dict_nm not Anthelmintic, Antifungals, Antimalarial, 
        --Antimycobacterial agents, Antiviral
        and medication_order_administration.pharmacy_class_id not in (15, 11, 13, 9, 12)
        and medication_order_administration.therapeutic_class_id = 1001 --anti-infective agents
        and medication_order_administration.order_class != 'Historical Med'
        and rx_days is not null

)

select
    {{
        dbt_utils.surrogate_key([
            'pathway_infection_key',
            'gen_nm_cln_sub'
        ])
    }} as pathway_med_key,
    *
from all_meds
where
    med_ord_rank = 1
    and med_dup_rank = 1
