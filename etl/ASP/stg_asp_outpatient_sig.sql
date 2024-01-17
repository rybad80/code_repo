with sig_raw as (
    --convert sig data to numeric information
    select
        stg_asp_abx_all.visit_key,
        stg_asp_abx_all.med_ord_key,
        stg_asp_abx_all.medication_order_id,
        stg_asp_abx_all.medication_start_date,
        stg_asp_abx_all.medication_end_date,
        stg_asp_abx_all.abx_name,
        stg_asp_abx_all.drug_category,
        stg_asp_abx_all.drug_class,
        stg_asp_abx_all.drug_subclass,
        --Ordinary information about the sig
        lower(order_med_sig.sig_text)::varchar(1000) as sig_text_lower,
        --If the sig is multi-line, where does the split occur?
        regexp_instr(
            sig_text_lower,
            'then|followed by'
        ) as sig_then_idx,
        case when sig_then_idx = 0
            then sig_text_lower
            else substring(
                sig_text_lower,
                1,
                sig_then_idx - 1
            ) end as sig_first_form,
        /*First instruction from multi-line sig*/
        coalesce(
            regexp_extract(
                sig_first_form,
                '\b(for|x)\s+(the|next|\s+)*(\d+)'
                || '\s+(more\s+)?(day(s)?|dose(s)?)\b'
            ),
            regexp_extract(
                sig_first_form,
                '\bday(s)\s?#?\s?\d\s?(-|to)\s?\d\b'
            )
        ) as sig_first,
        --Days or doses from first line
        regexp_extract(
            sig_first,
            '\b\d+(?!.\d+)\b'
        )::integer as sig_num,
        --Unit from first line
        regexp_extract(
            sig_first,
            '(dose|day)'
        ) as sig_unit,
        /*Second half of multi-line sig*/
        case when sig_then_idx != 0
            then substring(
                sig_text_lower,
                sig_then_idx,
                length(order_med_sig.sig_text)
            ) end as sig_then_form,
        coalesce(
            regexp_extract(
                sig_then_form,
                '\b(for|x)\s+(the|next|\s+)*(\d+)'
                || '\s+(more\s+)?(day(s?)|dose(s)?)\b'
            ),
            regexp_extract(
                sig_then_form,
                '\bday(s)\s?#?\s?\d\s?(-|to)\s?\d\b'
            )
        ) as sig_then,
        --Days or doses from second line
        regexp_extract(
            sig_then,
            '\b\d+(?!.\d+)\b'
        )::integer as sig_then_num,
        --Unit from second line
        regexp_extract(
            sig_then,
            '(dose|day)'
        ) as sig_then_unit,
        --Last dose day from numbers
        regexp_extract(
            sig_text_lower,
            '\bday(s)\s?#?\s?\d\s?(-|to)\s?\d\b'
        ) as days_through,
        regexp_extract(
            days_through,
            '\d+$' --end of string
        )::integer as days_through_end,
        --find dosage, including written numbers
        replace(
            replace(
                replace(
                    replace(
                        replace(
                            replace(
                                replace(
                                    replace(
                                        replace(
                                            regexp_extract(
                                                sig_text_lower,
                                                '\b[\d\.(one|
                                                |two|
                                                |three|
                                                |four|
                                                |five|
                                                |six|
                                                |seven|
                                                |eight|
                                                |nine|
                                                |ten)]+\s?+(capsule\(s\)|
                                                |tablet\(s\)|
                                                |ml)+'
                                            ), 'one', '1'
                                        ), 'two', '2'
                                    ), 'three', '3'
                                ), 'four', '4'
                            ), 'five', '5'
                        ), 'six', '6'
                    ), 'seven', '7'
                ), 'eight', '8'
            ), 'nine', '9'
        ) as sig_dose,
        --numeric value
        regexp_extract(
            sig_dose,
            '\b(\d+\.)?\d+'
        )::numeric as sig_dose_num
    from
        {{ ref('stg_asp_abx_all') }} as stg_asp_abx_all
        inner join {{ source('clarity_ods', 'order_med_sig') }} as order_med_sig
            on stg_asp_abx_all.medication_order_id = order_med_sig.order_id
    where
        stg_asp_abx_all.order_status != 'Canceled'
        and stg_asp_abx_all.order_mode = 'Outpatient'
        and stg_asp_abx_all.medication_start_date >= '2013-07-01'
        and stg_asp_abx_all.drug_category is not null
)
select
    visit_key,
    med_ord_key,
    medication_order_id,
    medication_start_date,
    medication_end_date,
    abx_name,
    drug_category,
    drug_class,
    drug_subclass,
    sig_text_lower,
    sig_first,
    sig_num,
    sig_unit,
    sig_then_num,
    sig_then_unit,
    days_through_end,
    sig_dose_num
from
    sig_raw
