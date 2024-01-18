with
sw_dispos as (
select
    smart_data_element_all.sde_key,
    smart_data_element_all.visit_key,
    smart_data_element_all.concept_id,
    smart_data_element_all.sde_entered_employee,
    smart_data_element_all.entered_date as current_value_entered_date,
    smart_data_element_all.element_value as current_value
from
    {{ ref('smart_data_element_all') }} as smart_data_element_all
    inner join {{ ref('encounter_all') }} as encounter_all
        on encounter_all.visit_key = smart_data_element_all.visit_key
    where concept_id in(
        'CHOPBH#375', -- Placement Status
        'CHOPBH#405', -- Final Disposition
        'CHOPBH#379', -- Problems
        'CHOPBH#380', -- Background Disposition,
        'CHOPBH#433', -- Complex Dispo Y/N
        'CHOPBH#382', -- Complex Dispo Reason
        'CHOPBH#410', -- Bed Placement INP, IOP, PHP, Residential
        'CHOPBH#524', -- OP Referrals,  -- Only 1
        'CHOPBH#527', -- IOP referrals, -- None in set
        'CHOPBH#529', -- PHP Referrals
        'CHOPBH#531', -- INPATIENT Referrals,
        'CHOPBH#534') -- RTF Referrals
        and (encounter_all.ed_ind = 1 or encounter_all.inpatient_ind = 1)
),

sw_dispos_current_values as (
select distinct
    visit_key,
    concept_id,
    current_value
from
    sw_dispos
order by
    visit_key,
    concept_id,
    current_value
),

sw_dispos_current_values_concat_1 as (
select
    visit_key,
    cast(group_concat(case
    when concept_id = 'CHOPBH#379' then current_value end) as varchar(40))
    as sw_dispo_problems,
    cast(group_concat(case
    when concept_id = 'CHOPBH#380' then current_value end) as varchar(75))
    as sw_dispo_up_to_date,
    cast(group_concat(case
    when concept_id = 'CHOPBH#524' then current_value end) as varchar(75))
    as sw_final_dispo_op_sites,
    cast(group_concat(case
    when concept_id = 'CHOPBH#527' then current_value end) as varchar(50))
        as sw_final_dispo_iop_sites
from
    sw_dispos_current_values
group by
    visit_key
),

sw_dispos_current_values_concat_2 as (
select
    visit_key,
    cast(group_concat(case
    when concept_id = 'CHOPBH#529' then current_value end) as varchar(100))
        as sw_final_dispo_php_sites,
    cast(group_concat(case
      when concept_id = 'CHOPBH#531' then current_value end) as varchar(100))
        as sw_final_dispo_ip_sites,
    cast(group_concat(case
    when concept_id = 'CHOPBH#534' then current_value end) as varchar(50))
        as sw_final_dispo_rtf_sites,
    cast(group_concat(case
    when concept_id = 'CHOPBH#405' then current_value end) as varchar(75))
        as sw_final_dispo
from
    sw_dispos_current_values
group by
    visit_key
),

sw_dispos_current_values_concat as (
select
    sw_dispos_current_values_concat_1.visit_key,
    sw_dispo_problems,
    sw_dispo_up_to_date,
    sw_final_dispo,
    sw_final_dispo_op_sites,
    sw_final_dispo_iop_sites,
    sw_final_dispo_php_sites,
    sw_final_dispo_ip_sites,
    sw_final_dispo_rtf_sites
from
    sw_dispos_current_values_concat_1
    inner join sw_dispos_current_values_concat_2
    on sw_dispos_current_values_concat_2.visit_key = sw_dispos_current_values_concat_1.visit_key
),

sw_dispos_summary as (
    select
        sw_dispos.visit_key,
        max(
            case when sw_dispos.concept_id = 'CHOPBH#375' then sw_dispos.current_value end
        ) as sw_placement_status,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#379' and sw_dispos.current_value = 'Aggression'
            then 1 else 0 end) as problems_aggression_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#379' and sw_dispos.current_value = 'ASD'
            then 1 else 0 end) as problems_asd_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#379' and sw_dispos.current_value = 'Eating Disorder'
            then 1 else 0 end) as problems_eating_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#379' and sw_dispos.current_value = 'Elopement'
            then 1 else 0 end) as problems_elopement_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#379' and sw_dispos.current_value = 'Ingestion'
            then 1 else 0 end) as problems_ingestion_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#379' and sw_dispos.current_value = 'Other'
            then 1 else 0 end) as problems_other_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#379' and sw_dispos.current_value = 'SI'
            then 1 else 0 end) as problems_si_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#433' and sw_dispos.current_value = '1'
            then 1 else 0 end) as complex_dispo_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#382' and sw_dispos.current_value = 'Complex Medical Hx'
            then 1 else 0 end) as complex_dispo_med_history_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#382' and sw_dispos.current_value = 'DHS'
            then 1 else 0 end) as complex_dispo_dhs_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#382' and sw_dispos.current_value = 'Medical Equipment'
            then 1 else 0 end) as complex_dispo_med_equip_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#382' and sw_dispos.current_value = 'Other'
            then 1 else 0 end) as complex_dispo_other_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#382' and sw_dispos.current_value = 'RTF'
            then 1 else 0 end) as complex_dispo_rtf_ind,
        max(case when
            sw_dispos.concept_id = 'CHOPBH#382' and sw_dispos.current_value = 'Sexually Acting Out'
            then 1 else 0 end) as complex_dispo_sex_ind,
        max(case when
            (
                sw_dispos.concept_id = 'CHOPBH#405'
                and sw_dispos.current_value = 'Inpatient psychiatric hospitalization'
            )
            then 1 else 0 end) as sw_final_dispo_ip_ind,
        max(case when
            (
                sw_dispos.concept_id = 'CHOPBH#405'
                and sw_dispos.current_value = 'Continue in current outpatient services'
            )
            then 1 else 0 end) as sw_final_dispo_op_con_ind,
        max(case when
            (
                sw_dispos.concept_id = 'CHOPBH#405' and sw_dispos.current_value = 'Outpatient referrals provided'
            )
            then 1 else 0 end) as sw_final_dispo_op_ind,
        max(case when
            (
                sw_dispos.concept_id = 'CHOPBH#405' and sw_dispos.current_value = 'RTF'
            )
            then 1 else 0 end) as sw_final_dispo_rtf_ind,
        max(case when
            (
                sw_dispos.concept_id = 'CHOPBH#405' and sw_dispos.current_value = 'PHP'
            )
            then 1 else 0 end) as sw_final_dispo_php_ind,
        max(case when
            (
                sw_dispos.concept_id = 'CHOPBH#405' and sw_dispos.current_value = 'CRR'
            )
            then 1 else 0 end) as sw_final_dispo_crr_ind,
        max(case when
            (
                sw_dispos.concept_id = 'CHOPBH#405' and sw_dispos.current_value = 'No intervention required'
            )
            then 1 else 0 end) as sw_final_dispo_no_int_ind
    from
        sw_dispos
    group by
        sw_dispos.visit_key
)

select
    sw_dispos_summary.visit_key,
    sw_dispos_summary.sw_placement_status,
    sw_dispos_summary.problems_aggression_ind,
    sw_dispos_summary.problems_asd_ind,
    sw_dispos_summary.problems_eating_ind,
    sw_dispos_summary.problems_elopement_ind,
    sw_dispos_summary.problems_ingestion_ind,
    sw_dispos_summary.problems_other_ind,
    sw_dispos_summary.problems_si_ind,
    sw_dispos_summary.complex_dispo_ind,
    sw_dispos_summary.complex_dispo_med_history_ind,
    sw_dispos_summary.complex_dispo_dhs_ind,
    sw_dispos_summary.complex_dispo_med_equip_ind,
    sw_dispos_summary.complex_dispo_other_ind,
    sw_dispos_summary.complex_dispo_rtf_ind,
    sw_dispos_summary.complex_dispo_sex_ind,
    sw_dispos_summary.sw_final_dispo_ip_ind,
    sw_dispos_summary.sw_final_dispo_op_con_ind,
    sw_dispos_summary.sw_final_dispo_op_ind,
    sw_dispos_summary.sw_final_dispo_rtf_ind,
    sw_dispos_summary.sw_final_dispo_php_ind,
    sw_dispos_summary.sw_final_dispo_crr_ind,
    sw_dispos_summary.sw_final_dispo_no_int_ind,
    sw_dispos_current_values_concat.sw_dispo_problems,
    sw_dispos_current_values_concat.sw_final_dispo,
    sw_dispos_current_values_concat.sw_final_dispo_ip_sites,
    sw_dispos_current_values_concat.sw_final_dispo_op_sites,
    sw_dispos_current_values_concat.sw_final_dispo_iop_sites,
    sw_dispos_current_values_concat.sw_final_dispo_php_sites,
    sw_dispos_current_values_concat.sw_final_dispo_rtf_sites,
    sw_dispos_current_values_concat.sw_dispo_up_to_date
from
    sw_dispos_summary
    left join sw_dispos_current_values_concat
        on  sw_dispos_current_values_concat.visit_key = sw_dispos_summary.visit_key
