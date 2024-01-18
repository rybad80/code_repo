select
    x1."Encounter Key",
    x1."Arrive ED",
    x1."Depart ED",
    x1."Triage Start",
    x1."Triage End",
    x1."Assign RN",
    x1."Assign Resident NP",
    x1."Assign 1st Attending",
    x1."Registration Start",
    x1."Roomed ED",
    x1."Registration End",
    x1."ED Conference Review",
    x1."MD Evaluation",
    x1."Attending Evaluation",
    x1."After Visit Summary Printed",
    x1."MD Report",
    x1."Paged IP RN",
    x1."IP Bed Assigned",
    x1."Admission Form Bed Request",
    x1."Triage RN Name"
from
    (
        select
            ve.visit_key as "Encounter Key",
            row_number() over (
                partition by ve.visit_key
                order by
                    ve.pat_key
            ) as col1,
            min(
                case
                    when (et.event_id = '50' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Arrive ED",
            max(
                case
                    when (et.event_id = '95' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Depart ED",
            min(
                case
                    when (et.event_id = '205' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Triage Start",
            max(
                case
                    when (et.event_id = '210' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Triage End",
            min(
                case
                    when (et.event_id = '120' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Assign RN",
            min(
                case
                    when (et.event_id = '300121' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Assign Resident NP",
            min(
                case
                    when (et.event_id = '111' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Assign 1st Attending",
            min(
                case
                    when (et.event_id = '55' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Registration Start",
            min(
                case
                    when (et.event_id = '55' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Roomed ED",
            max(
                case
                    when (et.event_id = '220' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Registration End",
            max(
                case
                    when (et.event_id = '300711' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "ED Conference Review",
            min(
                case
                    when (et.event_id = '30020501' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "MD Evaluation",
            min(
                case
                    when (et.event_id = '30020502' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Attending Evaluation",
            min(
                case
                    when (et.event_id = '85' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "After Visit Summary Printed",
            min(
                case
                    when (et.event_id = '300100' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "MD Report",
            min(
                case
                    when (et.event_id = '300101' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Paged IP RN",
            min(
                case
                    when (et.event_id = '300105' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "IP Bed Assigned",
            min(
                case
                    when (et.event_id = '231' :: int8) then ve.event_dt
                    else null :: "TIMESTAMP"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Admission Form Bed Request",
            min(
                case
                    when (et.event_id = '205' :: int8) then emp.full_nm
                    else null :: "VARCHAR"
                end
            ) over (
                partition by ve.visit_key rows between unbounded preceding
                and unbounded following
            ) as "Triage RN Name"
        from
            (
                (
                    {{ source('cdw', 'visit_ed_event') }} ve
                    join {{ source('cdw', 'master_event_type') }} et on ((ve.event_type_key = et.event_type_key))
                )
                left join {{ source('cdw', 'employee') }} emp on ((ve.event_init_emp_key = emp.emp_key))
            )
        where
            (
                (
                    et.event_id in (
                        '50' :: int8,
                        '55' :: int8,
                        '95' :: int8,
                        '205' :: int8,
                        '210' :: int8,
                        '300121' :: int8,
                        '120' :: int8,
                        '111' :: int8,
                        '215' :: int8,
                        '220' :: int8,
                        '300711' :: int8,
                        '30020501' :: int8,
                        '30020502' :: int8,
                        '85' :: int8,
                        '300100' :: int8,
                        '300101' :: int8,
                        '300105' :: int8,
                        '231' :: int8,
                        '300112' :: int8
                    )
                )
                and (ve.visit_key <> -1)
            )
    ) as x1
where
    (x1.col1 = 1)