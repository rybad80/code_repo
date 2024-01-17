/* stg_nutrition_ip_flowsheet_pivot
This code transforms the flowsheet data into one row per patient per `timing`.

The complication is that these flowsheets don't all get entered with the same timestamp,
so we use this `timing` as a grouper when we pivot the data.

A `timing` can be the patients first overall recorded value, the final
recorded value, or the last recorded value on a weekly basis.
*/

{% set timings = {
    'initial': 'flowsheet_rn_visit_asc',
    'final': 'flowsheet_rn_visit_desc',
    'weekly': 'flowsheet_rn_week_desc',
} %}

{% for timing, timing_col in timings.items() %}
    select
        visit_key,
        {% if timing == 'weekly' %}
            c_wk_start_dt,
            c_wk_end_dt,
        {% else %}
            null as c_wk_start_dt,
            null as c_wk_end_dt,
        {% endif %}
        '{{ timing }}' as timing,
        {% for flowsheet_id, flowsheet_col in get_nutrition_ip_flowsheets().items() %}
            max(
                case
                    when flowsheet_id = {{ flowsheet_id }}
                    then {{ 'meas_val' if flowsheet_id == 400730782 else 'clean_meas_val_num' }}
                end
            ) as {{ flowsheet_col }}{{ ',' if not loop.last }}
        {% endfor %}
        
    from
        {{ ref('stg_nutrition_ip_flowsheet') }}

    where
        {{ timing_col }} = 1

    group by
        visit_key,
        {% if timing == 'weekly' %}
            c_wk_start_dt,
            c_wk_end_dt,
        {% endif %}
        timing
    
    {{ 'union' if not loop.last }}
{% endfor %}
