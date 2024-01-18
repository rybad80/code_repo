select
    x."Encounter Key",
    x."Patient Key",
    x."Encounter ID",
    x."Arrive ED" as "Arrive ED Timestamp",
    to_char(x."Arrive ED", 'YYYY-MM-DD' :: "VARCHAR") as "Arrive ED",
    to_number(
        to_char(x."Arrive ED", 'HH' :: "VARCHAR"),
        '99' :: "VARCHAR"
    ) as "Arrive ED Hour",
    x."Depart ED",
    x."EDECU Arrival Time",
    x."MD Eval to Pt Left ED Min",
    x."ED Length of Stay Min",
    x."EDECU Length of Stay Min",
    x."Arrival to Triage Min",
    x."Arrival to MD Eval Min",
    x."MD Eval to Bed Request Min",
    x."Bed Request to MD Report Min",
    x."Bed Request to Pt Left Min",
    x."MD Report to Pt Left Min",
    x."IP RN Paged to Pt Left Min",
    x."IP MD Paged to MD Report Min",
    x."EDECU Ind",
    x."ED Resuscitation Rm Use Ind",
    x."ED Disposition",
    x."ED General Disposition",
    x."ED Patients Presenting",
    x."ED Patients Seen",
    (
        (
            "NUMERIC"(
                date_part(
                    'EPOCH' :: "VARCHAR",
                    (
                        x."Arrive ED" - lag(x."Depart ED") over (
                            partition by x."Patient Key"
                            order by
                                to_char(x."Arrive ED", 'YYYY-MM-DD' :: "VARCHAR")
                        )
                    )
                )
            )
        ) :: numeric(20, 2) / '3600' :: numeric(4, 0)
    ) as "Revisit Hours",
    case
        when (x."ED General Disposition" = 'LWBS' :: "VARCHAR") then null :: int4
        when (
            lag(x."ED General Disposition") over (
                partition by x."Patient Key"
                order by
                    to_char(x."Arrive ED", 'YYYY-MM-DD' :: "VARCHAR")
            ) <> 'DISCHARGE' :: "VARCHAR"
        ) then null :: int4
        when (
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (
                                x."Arrive ED" - lag(x."Depart ED") over (
                                    partition by x."Patient Key"
                                    order by
                                        to_char(x."Arrive ED", 'YYYY-MM-DD' :: "VARCHAR")
                                )
                            )
                        )
                    )
                ) :: numeric(20, 2) / '3600' :: numeric(4, 0)
            ) <= '72' :: numeric(2, 0)
        ) then 1
        else null :: int4
    end as "72hr Revisit Ind",
    x."Room History"
from
    (
        select
            vw_ed."Encounter Key",
            vai.pat_key as "Patient Key",
            visit.enc_id as "Encounter ID",
            vai.cuml_room_nm as "Room History",
            vw_ed."Arrive ED",
            vw_ed."Depart ED",
            vw_ed."EDECU Arrival Time",
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (
                                case
                                    when (vw_ed."EDECU Arrival Time" notnull) then vw_ed."EDECU Arrival Time"
                                    else vw_ed."Depart ED"
                                end - vw_ed."Earliest MD Eval"
                            )
                        )
                    )
                ) :: numeric(20, 5) / '60' :: numeric(2, 0)
            ) as "MD Eval to Pt Left ED Min",
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (
                                case
                                    when (vw_ed."EDECU Arrival Time" notnull) then vw_ed."EDECU Arrival Time"
                                    else vw_ed."Depart ED"
                                end - vw_ed."Arrive ED"
                            )
                        )
                    )
                ) :: numeric(20, 2) / '60' :: numeric(2, 0)
            ) as "ED Length of Stay Min",
            case
                when (vw_ed."EDECU Arrival Time" notnull) then (
                    (
                        "NUMERIC"(
                            date_part(
                                'EPOCH' :: "VARCHAR",
                                (vw_ed."Depart ED" - vw_ed."EDECU Arrival Time")
                            )
                        )
                    ) :: numeric(20, 2) / '60' :: numeric(2, 0)
                )
                else "NUMERIC"(null :: int8)
            end as "EDECU Length of Stay Min",
            case
                when (sortrn."Encounter Key" notnull) then '0' :: numeric
                else (
                    (
                        "NUMERIC"(
                            date_part(
                                'EPOCH' :: "VARCHAR",
                                (vw_ed."Triage Start" - vw_ed."Arrive ED")
                            )
                        )
                    ) :: numeric(20, 2) / '60' :: numeric(2, 0)
                )
            end as "Arrival to Triage Min",
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (vw_ed."Earliest MD Eval" - vw_ed."Arrive ED")
                        )
                    )
                ) :: numeric(20, 2) / '60' :: numeric(2, 0)
            ) as "Arrival to MD Eval Min",
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (
                                vw_ed."Admission Form Bed Request" - vw_ed."Earliest MD Eval"
                            )
                        )
                    )
                ) :: numeric(20, 2) / '60' :: numeric(2, 0)
            ) as "MD Eval to Bed Request Min",
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (
                                vw_ed."MD Report" - vw_ed."Admission Form Bed Request"
                            )
                        )
                    )
                ) :: numeric(20, 2) / '60' :: numeric(2, 0)
            ) as "Bed Request to MD Report Min",
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (
                                case
                                    when (vw_ed."EDECU Arrival Time" notnull) then vw_ed."EDECU Arrival Time"
                                    else vw_ed."Depart ED"
                                end - vw_ed."MD Report"
                            )
                        )
                    )
                ) :: numeric(20, 2) / '60' :: numeric(2, 0)
            ) as "MD Report to Pt Left Min",
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (
                                case
                                    when (vw_ed."EDECU Arrival Time" notnull) then vw_ed."EDECU Arrival Time"
                                    else vw_ed."Depart ED"
                                end - vw_ed."Admission Form Bed Request"
                            )
                        )
                    )
                ) :: numeric(20, 2) / '60' :: numeric(2, 0)
            ) as "Bed Request to Pt Left Min",
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (
                                case
                                    when (vw_ed."EDECU Arrival Time" notnull) then vw_ed."EDECU Arrival Time"
                                    else vw_ed."Depart ED"
                                end - vw_ed."Paged IP RN"
                            )
                        )
                    )
                ) :: numeric(20, 2) / '60' :: numeric(2, 0)
            ) as "IP RN Paged to Pt Left Min",
            (
                (
                    "NUMERIC"(
                        date_part(
                            'EPOCH' :: "VARCHAR",
                            (vw_ed."MD Report" - vw_ed."Paged IP MD")
                        )
                    )
                ) :: numeric(20, 2) / '60' :: numeric(2, 0)
            ) as "IP MD Paged to MD Report Min",
            case
                when (vai.edecu_arrvl_dt notnull) then 1
                else 0
            end as "EDECU Ind",
            case
                when (
                    vai.cuml_room_nm ~~ like_escape('%ED RES%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 1
                else 0
            end as "ED Resuscitation Rm Use Ind",
            case
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 'Indeterminate' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 'Admit' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 'EDECU - Discharge' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 'EDECU - Admit' :: "VARCHAR"
                when (
                    (
                        dict2.dict_nm = 'Transfered to Another Facility(Not from Triage)' :: "VARCHAR"
                    )
                    and (vai.edecu_arrvl_dt notnull)
                ) then 'EDECU - Transfered to Another Facility(Not from Triage)' :: "VARCHAR"
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (
                        (dep2.dept_abbr = 'EDEC' :: "VARCHAR")
                        or (dep2.dept_abbr = 'ED' :: "VARCHAR")
                    )
                ) then 'EDECU - Discharge' :: "VARCHAR"
                when (
                    (
                        (vai.edecu_arrvl_dt notnull)
                        and (dep2.dept_abbr <> 'EDEC' :: "VARCHAR")
                    )
                    and (hef."Admitting Department" isnull)
                ) then 'EDECU - Admit' :: "VARCHAR"
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (hef."Admitting Department" notnull)
                ) then 'EDECU - Admit' :: "VARCHAR"
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (hef."Admitting Department" isnull)
                ) then 'EDECU - Discharge' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (dep2.dept_abbr <> 'EDECU' :: "VARCHAR")
                ) then 'EDECU - Admit' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (dep2.dept_abbr = 'EDECU' :: "VARCHAR")
                ) then 'EDECU - Discharge' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (
                                    (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                    or (dict2.dict_nm = 'OR' :: "VARCHAR")
                                )
                                or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                            )
                            and (vai.edecu_arrvl_dt isnull)
                        )
                        and (hef."Admitting Department" isnull)
                    )
                    and (dep2.dept_abbr = 'PERIOP' :: "VARCHAR")
                ) then 'Admit' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 'Discharge' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 'Admit' :: "VARCHAR"
                when (
                    (dict2.dict_nm = 'HACU' :: "VARCHAR")
                    and (hef."Admitting Department" isnull)
                ) then 'Transfer from Triage to HACU' :: "VARCHAR"
                when (
                    (dict2.dict_nm = 'HACU' :: "VARCHAR")
                    and (hef."Admitting Department" notnull)
                ) then 'Admit' :: "VARCHAR"
                when (hef."Admitting Department" notnull) then 'Admit' :: "VARCHAR"
                else dict2.dict_nm
            end as "ED Disposition",
            case
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 'DISCHARGE' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 'ADMIT' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 'EDECU' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 'EDECU' :: "VARCHAR"
                when (
                    (
                        dict2.dict_nm = 'Transfered to Another Facility(Not from Triage)' :: "VARCHAR"
                    )
                    and (vai.edecu_arrvl_dt notnull)
                ) then 'EDECU' :: "VARCHAR"
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (
                        (dep2.dept_abbr = 'EDEC' :: "VARCHAR")
                        or (dep2.dept_abbr = 'ED' :: "VARCHAR")
                    )
                ) then 'EDECU' :: "VARCHAR"
                when (
                    (
                        (vai.edecu_arrvl_dt notnull)
                        and (dep2.dept_abbr <> 'EDEC' :: "VARCHAR")
                    )
                    and (hef."Admitting Department" isnull)
                ) then 'EDECU' :: "VARCHAR"
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (hef."Admitting Department" notnull)
                ) then 'EDECU' :: "VARCHAR"
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (hef."Admitting Department" isnull)
                ) then 'EDECU' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (dep2.dept_abbr <> 'EDECU' :: "VARCHAR")
                ) then 'EDECU' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (dep2.dept_abbr = 'EDECU' :: "VARCHAR")
                ) then 'EDECU' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (
                                    (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                    or (dict2.dict_nm = 'OR' :: "VARCHAR")
                                )
                                or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                            )
                            and (vai.edecu_arrvl_dt isnull)
                        )
                        and (hef."Admitting Department" isnull)
                    )
                    and (dep2.dept_abbr = 'PERIOP' :: "VARCHAR")
                ) then 'ADMIT' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 'DISCHARGE' :: "VARCHAR"
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 'ADMIT' :: "VARCHAR"
                when (
                    (dict2.dict_nm = 'HACU' :: "VARCHAR")
                    and (hef."Admitting Department" isnull)
                ) then 'TRANSFER FROM TRIAGE' :: "VARCHAR"
                when (
                    (dict2.dict_nm = 'HACU' :: "VARCHAR")
                    and (hef."Admitting Department" notnull)
                ) then 'ADMIT' :: "VARCHAR"
                when (
                    dict2.dict_nm ~~ like_escape('%Eloped%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 'DISCHARGE' :: "VARCHAR"
                when (
                    dict2.dict_nm ~~ like_escape('LWBS%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 'LWBS' :: "VARCHAR"
                when (
                    dict2.dict_nm ~~ like_escape('Transfered%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 'TRANSFER NOT FROM TRIAGE' :: "VARCHAR"
                when (
                    dict2.dict_nm ~~ like_escape('Transfer %' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 'TRANSFER FROM TRIAGE' :: "VARCHAR"
                when (
                    dict2.dict_nm ~~ like_escape('Dece%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 'DECEASED' :: "VARCHAR"
                when (hef."Admitting Department" notnull) then 'ADMIT' :: "VARCHAR"
                else upper(dict2.dict_nm)
            end as "ED General Disposition",
            case
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    (
                        dict2.dict_nm = 'Transfered to Another Facility(Not from Triage)' :: "VARCHAR"
                    )
                    and (vai.edecu_arrvl_dt notnull)
                ) then 1
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (
                        (dep2.dept_abbr = 'EDEC' :: "VARCHAR")
                        or (dep2.dept_abbr = 'ED' :: "VARCHAR")
                    )
                ) then 1
                when (
                    (
                        (vai.edecu_arrvl_dt notnull)
                        and (dep2.dept_abbr <> 'EDEC' :: "VARCHAR")
                    )
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (dep2.dept_abbr <> 'EDECU' :: "VARCHAR")
                ) then 1
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (dep2.dept_abbr = 'EDECU' :: "VARCHAR")
                ) then 1
                when (
                    (
                        (
                            (
                                (
                                    (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                    or (dict2.dict_nm = 'OR' :: "VARCHAR")
                                )
                                or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                            )
                            and (vai.edecu_arrvl_dt isnull)
                        )
                        and (hef."Admitting Department" isnull)
                    )
                    and (dep2.dept_abbr = 'PERIOP' :: "VARCHAR")
                ) then 1
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    (dict2.dict_nm = 'HACU' :: "VARCHAR")
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (dict2.dict_nm = 'HACU' :: "VARCHAR")
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    dict2.dict_nm ~~ like_escape('%Eloped%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 1
                when (
                    dict2.dict_nm ~~ like_escape('LWBS%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 1
                when (
                    dict2.dict_nm ~~ like_escape('Transfer%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 1
                when (
                    dict2.dict_nm ~~ like_escape('Dece%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 1
                else 1
            end as "ED Patients Presenting",
            case
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (
                                    (vai.dict_dspn_key = -1)
                                    or (vai.dict_dspn_key = 0)
                                )
                                or (vai.dict_dspn_key = -2)
                            )
                            or (dict2.dict_nm = 'error' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    (
                        dict2.dict_nm = 'Transfered to Another Facility(Not from Triage)' :: "VARCHAR"
                    )
                    and (vai.edecu_arrvl_dt notnull)
                ) then 1
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (
                        (dep2.dept_abbr = 'EDEC' :: "VARCHAR")
                        or (dep2.dept_abbr = 'ED' :: "VARCHAR")
                    )
                ) then 1
                when (
                    (
                        (vai.edecu_arrvl_dt notnull)
                        and (dep2.dept_abbr <> 'EDEC' :: "VARCHAR")
                    )
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    (vai.edecu_arrvl_dt notnull)
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (dep2.dept_abbr <> 'EDECU' :: "VARCHAR")
                ) then 1
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt notnull)
                    )
                    and (dep2.dept_abbr = 'EDECU' :: "VARCHAR")
                ) then 1
                when (
                    (
                        (
                            (
                                (
                                    (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                    or (dict2.dict_nm = 'OR' :: "VARCHAR")
                                )
                                or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                            )
                            and (vai.edecu_arrvl_dt isnull)
                        )
                        and (hef."Admitting Department" isnull)
                    )
                    and (dep2.dept_abbr = 'PERIOP' :: "VARCHAR")
                ) then 1
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (
                        (
                            (
                                (dict2.dict_nm = 'Admit' :: "VARCHAR")
                                or (dict2.dict_nm = 'OR' :: "VARCHAR")
                            )
                            or (dict2.dict_nm = 'EDECU' :: "VARCHAR")
                        )
                        and (vai.edecu_arrvl_dt isnull)
                    )
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    (dict2.dict_nm = 'HACU' :: "VARCHAR")
                    and (hef."Admitting Department" isnull)
                ) then 1
                when (
                    (dict2.dict_nm = 'HACU' :: "VARCHAR")
                    and (hef."Admitting Department" notnull)
                ) then 1
                when (
                    dict2.dict_nm ~~ like_escape('%Eloped%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 1
                when (
                    dict2.dict_nm ~~ like_escape('LWBS%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 0
                when (
                    dict2.dict_nm ~~ like_escape('Transfer%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 1
                when (
                    dict2.dict_nm ~~ like_escape('Dece%' :: "VARCHAR", '\' :: "VARCHAR")
                ) then 1
                else 1
            end as "ED Patients Seen"
        from
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        select
                                                            sub1."Encounter Key",
                                                            sub1."Arrive ED",
                                                            sub1."Depart ED",
                                                            sub1."Triage Start",
                                                            sub1."Triage End",
                                                            sub1."Assign RN",
                                                            sub1."Assign Resident NP",
                                                            sub1."Assign 1st Attending",
                                                            sub1."Registration Start",
                                                            sub1."Roomed ED",
                                                            sub1."Registration End",
                                                            sub1."ED Conference Review",
                                                            sub1."MD Evaluation",
                                                            sub1."Attending Evaluation",
                                                            sub1."After Visit Summary Printed",
                                                            sub1."MD Report",
                                                            sub1."Paged IP RN",
                                                            sub1."Paged IP MD",
                                                            sub1."IP Bed Assigned",
                                                            sub1."Admission Form Bed Request",
                                                            sub1."Triage RN Name",
                                                            sub1."Earliest MD Eval",
                                                            sub1."Earliest RN Report",
                                                            sub1."EDECU Arrival Time"
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
                                                                            when (et.event_id = '300103' :: int8) then ve.event_dt
                                                                            else null :: "TIMESTAMP"
                                                                        end
                                                                    ) over (
                                                                        partition by ve.visit_key rows between unbounded preceding
                                                                        and unbounded following
                                                                    ) as "Paged IP MD",
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
                                                                    ) as "Triage RN Name",
                                                                    min(
                                                                        case
                                                                            when (
                                                                                (
                                                                                    (et.event_id = '111' :: int8)
                                                                                    or (et.event_id = '300121' :: int8)
                                                                                )
                                                                                or (et.event_id = '300103' :: int8)
                                                                            ) then ve.event_dt
                                                                            else null :: "TIMESTAMP"
                                                                        end
                                                                    ) over (
                                                                        partition by ve.visit_key rows between unbounded preceding
                                                                        and unbounded following
                                                                    ) as "Earliest MD Eval",
                                                                    min(
                                                                        case
                                                                            when (
                                                                                (
                                                                                    (
                                                                                        (et.event_id = '300102' :: int8)
                                                                                        or (et.event_id = '300103' :: int8)
                                                                                    )
                                                                                    or (
                                                                                        (et.event_id = '300122' :: int8)
                                                                                        or (et.event_id = '300940' :: int8)
                                                                                    )
                                                                                )
                                                                                or (et.event_id = '300941' :: int8)
                                                                            ) then ve.event_dt
                                                                            else null :: "TIMESTAMP"
                                                                        end
                                                                    ) over (
                                                                        partition by ve.visit_key rows between unbounded preceding
                                                                        and unbounded following
                                                                    ) as "Earliest RN Report",
                                                                    min(vai.edecu_arrvl_dt) over (
                                                                        partition by ve.visit_key rows between unbounded preceding
                                                                        and unbounded following
                                                                    ) as "EDECU Arrival Time"
                                                                from
                                                                    (
                                                                        (
                                                                            (
                                                                                {{source('cdw', 'visit_ed_event')}} as ve
                                                                                join {{source('cdw', 'master_event_type')}} as et on ((ve.event_type_key = et.event_type_key))
                                                                            )
                                                                            left join {{source('cdw', 'employee')}} as emp on ((ve.event_init_emp_key = emp.emp_key))
                                                                        )
                                                                        left join {{source('cdw', 'visit_addl_info')}} as vai on ((ve.visit_key = vai.visit_key))
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
                                                                                '300112' :: int8,
                                                                                '300103' :: int8
                                                                            )
                                                                        )
                                                                        and (ve.visit_key <> -1)
                                                                    )
                                                            ) sub1
                                                        where
                                                            (sub1.col1 = 1)
                                                    ) vw_ed
                                                    left join {{source('cdw', 'visit_addl_info')}} as vai on ((vw_ed."Encounter Key" = vai.visit_key))
                                                )
                                                left join {{source('cdw', 'department')}} as dep on ((vai.last_dept_key = dep.dept_key))
                                            )
                                            left join {{source('cdw', 'location')}} as loc on ((dep.rev_loc_key = loc.loc_key))
                                        )
                                        left join {{source('cdw', 'cdw_dictionary')}} as dict2 on ((vai.dict_dspn_key = dict2.dict_key))
                                    )
                                    left join {{ref('vw_essa_hosp_encounter_fact')}} as hef on ((vai.visit_key = hef."Encounter Key"))
                                )
                                left join {{ref('visit')}} as visit on ((vai.visit_key = visit.visit_key))
                            )
                            left join {{source('cdw', 'hospital_account_visit')}} as hav on (
                                (
                                    (vai.visit_key = hav.visit_key)
                                    and (hav.pri_visit_ind = 1)
                                )
                            )
                        )
                        left join {{source('cdw', 'hospital_account')}} as ha on ((hav.hsp_acct_key = ha.hsp_acct_key))
                    )
                    left join {{source('cdw', 'department')}} as dep2 on ((ha.disch_dept_key = dep2.dept_key))
                )
                left join (
                    select
                        vw_essa_encounter_team."Encounter Key"
                    from
                        {{source('cdw', 'vw_essa_encounter_team')}} as vw_essa_encounter_team
                    where
                        (
                            vw_essa_encounter_team."Treatment Team Staff Type" = 'Sort RN' :: "VARCHAR"
                        )
                    group by
                        vw_essa_encounter_team."Encounter Key"
                ) sortrn on ((vw_ed."Encounter Key" = sortrn."Encounter Key"))
            )
        where
            (
                (
                    (
                        loc.loc_id in (('1001.000' :: numeric(14, 3)) :: numeric(14, 3))
                    )
                    and (vai.ed_cancelled_visit_ind <> 1)
                )
                and (
                    (
                        vw_ed."Arrive ED" >= "TIMESTAMP"(
                            to_date('07012011' :: "VARCHAR", 'MMDDYYYY' :: "VARCHAR")
                        )
                    )
                    and (
                        vw_ed."Arrive ED" <= "TIMESTAMP"(date('now(0)' :: "VARCHAR"))
                    )
                )
            )
    ) x
