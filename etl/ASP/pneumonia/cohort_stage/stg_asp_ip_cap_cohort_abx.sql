--all encounters with specific antibiotics administered or prescribed
with antibiotic_name as (
    select
        visit_key,
        encounter_date,
        hospital_admit_date,
        med_ord_key,
        medication_order_id,
        administration_seq_number,
        abx_name,
        --Replace non-descriptive admin routes (including null) with order routes
        case when admin_route_group not in (
                'Other',
                'NOT APPLICABLE'
            ) then admin_route_group
            else order_route_group end as medication_route,
        case when order_mode = 'Outpatient'
            then 1 else 0 end as outpatient_med_ind,
        administration_date,
        medication_start_date,
        medication_end_date,
        --Inclusion critera for Inpatient CAP
        case when
            --outpatient medications
            order_mode = 'Outpatient'
            --inpatient administered post-admission
            or administration_date >= hospital_admit_date
            then 1 else 0 end as ip_or_rx_ind
    from
        {{ ref('stg_asp_abx_all')}}
    where
        --Antibiotics Desired
        (
            lower(abx_name) in (
                'amikacin',
                'amoxicillin',
                'amoxicillin clavulanate',
                'ampicillin',
                'ampicillin sulbactam',
                'azithromycin',
                'aztreonam',
                'cefazolin',
                'cefdinir',
                'cefepime',
                'cefixime',
                'cefotaxime',
                'cefpodoxime',
                'ceftaroline',
                'ceftazidime',
                'ceftazidime avibactam',
                'ceftolozane tazobactam',
                'ceftriaxone',
                'cephalexin',
                'ciprofloxacin',
                'clindamycin',
                'doxycycline',
                'ertapenem',
                'imipenem cilastatin',
                'levofloxacin',
                'linezolid',
                'meropenem',
                'minocycline',
                'moxifloxacin',
                'oxacillin',
                'penicillin g',
                'penicillin v',
                'piperacillin tazobactam',
                'polymyxin',
                'polymyxin trimethoprim',
                'sulfamethoxazole trimethoprim',
                'tobramycin'
            )
            --Only include IV Gentamicin and IV Vancomycin
            or (
                lower(abx_name) in (
                    'gentamicin',
                    'vancomycin'
                )
                and lower(medication_route) = 'intravenous'
            )
            --Exclude Topical Ofloxacin
            or (
                lower(abx_name) = 'ofloxacin'
                and lower(medication_route) != 'topical'
            )
        )
        and order_class != 'Historical Med'
        and order_status != 'Canceled'
)
select
    visit_key,
    encounter_date,
    med_ord_key,
    administration_seq_number,
    medication_order_id,
    cast(abx_name as varchar(100)) as abx_name,
    medication_route,
    ip_or_rx_ind,
    outpatient_med_ind,
    administration_date,
    min(administration_date) over(
        partition by
            visit_key,
            --administration date is null for all discharge medications
            outpatient_med_ind
    ) as first_abx_time,
    minutes_between(
        first_abx_time,
        administration_date
    ) / 60.0 as hrs_since_first_abx,
    max(administration_date) over(
        partition by
            visit_key,
            --administration date is null for all discharge medications
            outpatient_med_ind
    ) as last_abx_time,
    medication_start_date,
    medication_end_date,
    --for how many days was the medication active? Important for outpatient meds
    days_between(
        medication_end_date,
        medication_start_date
    ) as medication_duration_days,
    --how soon after admission was inpatient medication administered?
    minutes_between(
        administration_date,
        hospital_admit_date
    ) as admission_to_administration_minutes,
    case when admission_to_administration_minutes <= 60.0 * 48.0 --48 hrs
        then 1 else 0 end as first_48_hrs_ind
from
    antibiotic_name
