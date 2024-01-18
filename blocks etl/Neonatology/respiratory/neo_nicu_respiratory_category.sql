/* neo_nicu_respiratory_category.sql
This table captures all ventilation timestamps and categorizes them
based on how they are abstracted into the CHND.
We handle invasive and non-invasive timestamps separately to make mapping
cleaner, then combine them at the end.
*/

with invasive_rows as (
    select
        visit_key,
        recorded_date,
        /* case as varchar(30) to get around netezza row size limits */
        max(case when flowsheet_id = 40010942 then cast(meas_val as varchar(30)) end) as invasive_mode,
        max(case when flowsheet_id = 40010941 then cast(meas_val as varchar(30)) end) as invasive_device,
        max(case when flowsheet_id = 40000242 then cast(meas_val as varchar(30)) end) as resp_o2_device,
        max(case when flowsheet_id = 40002606 then cast(meas_val as varchar(30)) end) as hfjv_pip_set,
        max(case when flowsheet_id = 40010977 then cast(meas_val as varchar(30)) end) as hfov_amplitude_actual,
        max(case when flowsheet_id = 40002718 then cast(meas_val as varchar(30)) end) as non_invasive_mode
    from
        {{ ref('stg_neo_nicu_respiratory_flowsheet') }}
    group by
        visit_key,
        recorded_date
    having
        resp_o2_device in ('Ventilation~ Invasive', 'Invasive ventilation')
        and (
            invasive_mode is not null
            /* hfjv and hfov often come without invasive_mode flowsheet recordings
            so need to check their key measures separately */
            or hfjv_pip_set is not null
            or hfov_amplitude_actual is not null
        )
        /* We need to exclude timestamps that look like invasive AND non-invasive.
        There are only 3 total in CDW that also have resp_o2_device = invasive. */
        and non_invasive_mode is null
),

invasive_category as (
    select
        visit_key,
        recorded_date,
        case
            when hfov_amplitude_actual is not null and hfjv_pip_set is not null
                /* hopefully this never happens */
                then 'HFOV and HFJV?'
            when hfov_amplitude_actual is not null
                then 'HFOV'
            when hfjv_pip_set is not null
                then 'HFJV'
            else 'CONV'
        end as respiratory_support_category,
        'invasive' as respiratory_support_type,
        resp_o2_device,
        invasive_mode as mode, --noqa: L029
        invasive_device,
        hfjv_pip_set,
        hfov_amplitude_actual,
        null as non_invasive_interface,
        null as o2_flow_rate
    from
        invasive_rows
),

non_invasive_rows as (
    /* non-invasive ventilation includes rows with a non_invasive_mode (same as invasive ventilation), but
    also include nasal cannula and high flow nasal cannula (which do not have a non_invasive mode). */
    select
        visit_key,
        recorded_date,
        /* case as varchar(30) to get around netezza row size limits */
        max(case when flowsheet_id = 40000242 then cast(meas_val as varchar(30)) end) as resp_o2_device,
        max(case when flowsheet_id = 40002718 then cast(meas_val as varchar(30)) end) as non_invasive_mode,
        max(case when flowsheet_id = 40002720 then cast(meas_val as varchar(30)) end) as non_invasive_interface,
        max(case when flowsheet_id = 40000234 then meas_val_num end) as o2_flow_rate,
        max(case when flowsheet_id = 40010942 then cast(meas_val as varchar(30)) end) as invasive_mode
    from
        {{ ref('stg_neo_nicu_respiratory_flowsheet') }}
    group by
        visit_key,
        recorded_date
    having
        /* exclude rows that have any invasive_mode as these are few and far between and throw
        everything else off. These single rows wont impact the final output anyways. */
        invasive_mode is null
        /* we also exclude rows that simultaneously describe nasal cannula (or hfnc) and
        other non-invasive support */
        and (
            (
                /* all non-invasive except nasal cannula and high flow nasal cannula */
                resp_o2_device not in ('Ventilation~ Invasive', 'Invasive ventilation')
                and lower(resp_o2_device) not in ('nasal cannula', 'high flow nasal cannula')
                and non_invasive_mode is not null
            ) or (
                /* only nasal cannula and high flow nasal cannula */
                lower(resp_o2_device) in ('nasal cannula', 'high flow nasal cannula')
                and non_invasive_mode is null
                and o2_flow_rate is not null
            )
        )
),

non_invasive_category as (
    select
        visit_key,
        recorded_date,
        case
            when lower(resp_o2_device) in ('nasal cannula', 'high flow nasal cannula')
                and o2_flow_rate <= 2 then 'NC <= 2LPM'
            when lower(resp_o2_device) in ('nasal cannula', 'high flow nasal cannula')
                and o2_flow_rate > 2 then 'HFNC > 2LPM'
            when lower(resp_o2_device) in (
                'aerosol mask',
                'face tent',
                'hme-heat & moisture exchange',
                'hood',
                't-piece'
            ) then 'Hood'
            when lower(non_invasive_mode) in (
                'bipap/spontaneous/timed', 'bipap/spontaneous', 'bipap/timed', 'bilevel cpap'
            ) then 'BiPAP'
            when lower(non_invasive_mode) in ('cpap', 'cpap/c-flex') then 'CPAP'
            when lower(non_invasive_mode) in ('pcv', 'pcv+assist') then 'NIPPV'
            else 'Other'
        end as respiratory_support_category,
        'non-invasive' as respiratory_support_type,
        resp_o2_device,
        non_invasive_mode as mode, --noqa: L029
        null as invasive_device,
        null as hfjv_pip_set,
        null as hfov_amplitude_actual,
        non_invasive_interface,
        o2_flow_rate
    from
        non_invasive_rows
),

categories_combined as (
    /* we know that invasive_category and non_invasive_category are mutually exclusive
    so it is safe to union them together */
    select
        *
    from
        invasive_category

    union all

    select
        *
    from
        non_invasive_category
)

select
    categories_combined.visit_key,
    categories_combined.recorded_date,
    stg_neo_nicu_visit_demographics.patient_name,
    stg_neo_nicu_visit_demographics.mrn,
    stg_neo_nicu_visit_demographics.dob,
    stg_neo_nicu_visit_demographics.sex,
    stg_neo_nicu_visit_demographics.gestational_age_complete_weeks,
    stg_neo_nicu_visit_demographics.gestational_age_remainder_days,
    stg_neo_nicu_visit_demographics.birth_weight_grams,
    stg_neo_nicu_visit_demographics.hospital_admit_date,
    stg_neo_nicu_visit_demographics.hospital_discharge_date,
    categories_combined.respiratory_support_category,
    categories_combined.respiratory_support_type,
    categories_combined.resp_o2_device,
    categories_combined.mode,
    categories_combined.invasive_device,
    categories_combined.hfjv_pip_set,
    categories_combined.hfov_amplitude_actual,
    categories_combined.non_invasive_interface,
    categories_combined.o2_flow_rate,
    stg_neo_nicu_visit_demographics.pat_key
from
    categories_combined
    inner join {{ ref('stg_neo_nicu_visit_demographics') }} as stg_neo_nicu_visit_demographics
        on stg_neo_nicu_visit_demographics.visit_key = categories_combined.visit_key
