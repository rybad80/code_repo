with final as (
    select
        stg_calc.*,
        coalesce(
            lead(
                stg_calc.vis_session_start
            ) over (
                partition by stg_calc.visit_key order by stg_calc.vis_session_start
            ) - cast('1 second' as interval),
            stg_calc.hospital_discharge_date
        )as vis_session_end,
        --only partition by visit_key here
        sum(
            stg_calc.vis_points_difference
        ) over (
            partition by stg_calc.visit_key order by stg_calc.vis_session_start
        ) as running_vis_score,
        sum(
            stg_calc.vis_points_difference
        ) over (
            partition by stg_calc.visit_key, stg_calc.med
            order by stg_calc.vis_session_start
        ) as running_med_vis_score,

        sum(
            case when stg_calc.med = 'DOPAMINE'
                      then stg_calc.vis_points_difference else 0 end
        ) over (
            partition by stg_calc.visit_key
            order by stg_calc.vis_session_start
        ) as running_dopamine_vis_score,
        sum(
            case when stg_calc.med = 'DOBUTAMINE'
                      then stg_calc.vis_points_difference else 0 end
        ) over (
            partition by stg_calc.visit_key
            order by stg_calc.vis_session_start
        ) as running_dobutamine_vis_score,
        sum(
            case when stg_calc.med = 'EPINEPHRINE'
                      then stg_calc.vis_points_difference else 0 end
        ) over (
            partition by stg_calc.visit_key
            order by stg_calc.vis_session_start
        ) as running_epinephrine_vis_score,
        sum(
            case when stg_calc.med = 'MILRINONE'
                      then stg_calc.vis_points_difference else 0 end
        ) over (
            partition by stg_calc.visit_key
            order by stg_calc.vis_session_start
        ) as running_milrinone_vis_score,
        sum(
            case when stg_calc.med = 'VASOPRESSIN'
                      then stg_calc.vis_points_difference else 0 end
        ) over (
            partition by stg_calc.visit_key
            order by stg_calc.vis_session_start
        ) as running_vasopressin_vis_score,
        sum(
            case
                when stg_calc.med = 'NOREPINEPHRINE'
                     then stg_calc.vis_points_difference else 0
            end
        ) over (
            partition by stg_calc.visit_key
            order by stg_calc.vis_session_start
        ) as running_norepinephrine_vis_score,
        extract(
            epoch from (vis_session_end - stg_calc.vis_session_start) --noqa
        ) as session_length
    from {{ ref('stg_vasoactive_inotropic_score_calc') }}
    as stg_calc
    where stg_calc.rate_change_flag = 1
)

select
    final.visit_key,
    final.csn,
    final.mrn,
    final.patient_name,
    final.hospital_admit_date,
    final.med,
    final.vis_points_final as vis_points,
    final.vis_points_difference,
    final.running_vis_score,
    final.running_med_vis_score,
    final.running_dopamine_vis_score,
    final.running_dobutamine_vis_score,
    final.running_epinephrine_vis_score,
    final.running_milrinone_vis_score,
    final.running_vasopressin_vis_score,
    final.running_norepinephrine_vis_score,
    final.vis_session_start,
    final.vis_session_end,
    final.ma_dose,
    adt_department_group.department_group_name as current_department,
    row_number() over (
        partition by
            final.visit_key
        order by coalesce(final.vis_session_end, current_date) desc
    ) as vis_change_order,
    row_number() over (
        partition by
            final.visit_key, final.vis_session_start
        order by final.vis_session_end desc
    ) as same_event_order,
    case when final.running_vis_score > 20
        then 1 else 0 end as high_vis_score_ind
from final
left join
    {{ ref('adt_department_group') }} as adt_department_group on
        final.visit_key = adt_department_group.visit_key
        and adt_department_group.exit_date is null
