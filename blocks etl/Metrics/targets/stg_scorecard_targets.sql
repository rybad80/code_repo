{% set
    cardiac_targets = [
        {
            "visual_month": "visual_month",
            "drill_down_one": "drill_down_1",
            "metric_type": "metric_type",
            "month_target": "month_target",
            "metric_id": "metric_id",
            "table": ref('lookup_sl_dash_cardiac_center_targets')
        },
        {
            "visual_month": "budget_date",
            "drill_down_one": "cost_center_name_site_group",
            "metric_type": "'count'",
            "month_target": "budget",
            "metric_id": "'cardiac_dept_charges'",
            "table": ref('stg_cardiac_charges_targets')
        },
        {
            "visual_month": "visual_month",
            "drill_down_one": "drill_down",
            "metric_type": "'count'",
            "month_target": "month_target",
            "metric_id": "metric_id",
            "table": ref('stg_sl_dash_cardiac_admissions')
        },
        {
            "visual_month": "post_date_month",
            "drill_down_one": "drill_down",
            "metric_type": "metric_type",
            "month_target": "specialty_care_visit_budget",
            "metric_id": "metric_id",
            "table": ref('stg_cardiac_spec_visits_targets')
        },
        {
            "visual_month": "post_date_month",
            "drill_down_one": "drill_down",
            "metric_type": "'count'",
            "month_target": "patient_days_target",
            "metric_id": "metric_id",
            "table": ref('stg_cardiac_patient_days_targets')
        }

    ]
%}

{% set
    neo_targets = [
        {
            "visual_month": "post_date_month",
            "metric_type": "'count'",
            "month_target": "patient_days_target",
            "metric_id": "'neo_unit_pat_days'",
            "table": ref('stg_sl_dash_neo_finance_days_targets')
        },
        {
            "visual_month": "post_date_month",
            "metric_type": "'avg'",
            "month_target": "adc_target",
            "metric_id": "'neo_adc'",
            "table": ref('stg_sl_dash_neo_finance_days_targets')
        },
        {
            "visual_month": "budget_date",
            "drill_down_one": "cost_center_name",
            "drill_down_two": "cost_center_site_name",
            "metric_type": "'dollar'",
            "month_target": "budget",
            "metric_id": "'neo_dept_charges'",
            "table": ref('stg_sl_dash_neo_finance_charges_targets')
        }
    ]
%}

{% set
    onco_targets = [
        {
            "visual_month": "visual_month",
            "drill_down_one": "drill_down",
            "metric_type": "metric_type",
            "month_target": "month_target",
            "metric_id": "'onco_unit_adm'",
            "table": ref('stg_cancer_center_admissions')
        },
        {
            "visual_month": "post_date_month",
            "drill_down_one": "cost_center_name",
            "metric_type": "'count'",
            "month_target": "patient_days_target",
            "metric_id": "'onco_unit_pat_days'",
            "table": ref('stg_cancer_center_patient_days_targets')
        },
        {
            "visual_month": "visual_month",
            "drill_down_one": "drill_down_1",
            "metric_type": "metric_type",
            "month_target": "month_target",
            "metric_id": "metric_id",
            "table": ref('lookup_cancer_center_pfex_targets')
        },
        {
            "visual_month": "budget_date",
            "drill_down_one": "cost_center_name_site_group",
            "metric_type": "'count'",
            "month_target": "budget",
            "metric_id": "'onco_dept_charges'",
            "table": ref('stg_cancer_center_charges_targets')
        },
        {
            "visual_month": "visual_month",
            "drill_down_one": "drill_down",
            "metric_type": "metric_type",
            "month_target": "month_target",
            "metric_id": "'onco_op_visits'",
            "table": ref('stg_cancer_center_outpatient_visits')
        }
        
    ]
%}

{% set
    neuro_targets = [
        {
            "visual_month": "visual_month",
            "drill_down_one": "drill_down_1",
            "metric_type": "metric_type",
            "month_target": "month_target",
            "metric_id": "metric_id",
            "table": ref('lookup_neuroscience_pfex_targets')
        },
        {
            "visual_month": "post_date_month",
            "metric_type": "metric_type",
            "month_target": "specialty_care_visit_budget",
            "metric_id": "metric_id",
            "table": ref('stg_neuro_spec_visit_targets')
        }
        
    ]
%}

{% set
    specialty_care_targets = [
        {
            "visual_month": "post_date_month",
            "drill_down_one": "drill_down_one",
            "drill_down_two": "drill_down_two",
            "metric_type": "metric_type",
            "month_target": "specialty_care_visit_budget",
            "metric_id": "metric_id",
            "table": ref('stg_scorecard_spec_visit_targets')
        },
        {
            "visual_month": "visual_month",
            "drill_down_one": "drill_down_one",
            "drill_down_two": "drill_down_two",
            "metric_type": "metric_type",
            "month_target": "month_target",
            "metric_id": "metric_id",
            "table": ref('lookup_specialty_care_targets')
        }        
    ]
%}

{% set
    pc_targets = [
        {
            "visual_month": "visual_month",
            "drill_down_one": "drill_down_1",
            "metric_type": "metric_type",
            "month_target": "month_target",
            "metric_id": "metric_id",
            "table": ref('lookup_primary_care_targets')
        },
        {
            "visual_month": "post_date_month",
            "drill_down_one": "cost_center_name",
            "drill_down_two": "cost_center_site_name",
            "metric_type": "'count'",
            "month_target": "metric_budget_value",
            "metric_id": "'pc_growth_actual_visit_volume'",
            "table": ref('stg_care_network_pc_finance_visit_budget')
        }       
    ]
%}

{% set
    capacity_targets = [
        {
            "visual_month": "visual_month",
            "drill_down_one": "drill_down_one",
            "drill_down_two": "drill_down_two",
            "metric_type": "metric_type",
            "month_target": "month_target",
            "metric_id": "metric_id",
            "table": ref('lookup_capacity_targets')
        }
    ]
%}


{% set targets_filter = var('scorecard_metrics_filter') %}

{% set targets = cardiac_targets + neo_targets + onco_targets + neuro_targets + pc_targets + specialty_care_targets + capacity_targets%} --noqa: L016

{% if targets_filter == 'cardiac' %}
    {% set targets = cardiac_targets %}
{% elif targets_filter == 'neo' %}
    {% set targets = neo_targets %}
{% elif targets_filter == 'onco' %}
    {% set targets = onco_targets %}
{% elif targets_filter == 'neuro' %}
    {% set targets = neuro_targets %}
{% elif targets_filter == 'pc' %}
    {% set targets = pc_targets %}
{% elif targets_filter == 'specialty' %}
    {% set targets = specialty_care_targets %}
{% elif targets_filter == 'capacity' %}
    {% set targets = capacity_targets %}
{% endif %}


with target_data as (
    {%- for target in targets %}
    select
        {{ target.visual_month }} as visual_month,
        {{ target.drill_down_one|default('null') }} as drill_down_one,
        {{ target.drill_down_two|default('null') }} as drill_down_two,
        cast({{ target.month_target }} as numeric) as month_target,
        {{ target.metric_type }} as metric_type,
        {{ target.metric_id }} as metric_id
    from
        {{ target.table }}

    {% if not loop.last -%}
    union all
    {% endif %}

    {% endfor %}
)

select distinct
    target_data.*,
    calendar.f_yy as fy,
    calendar.c_yy as cy,
    calendar.c_mm,
    calendar.f_mm
from
    target_data
    inner join {{ source('cdw', 'master_date')}} as calendar
        on target_data.visual_month = date_trunc('month', calendar.full_dt)
