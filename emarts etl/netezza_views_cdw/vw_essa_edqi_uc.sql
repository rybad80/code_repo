select
    vw_ed."Encounter Key",
    visit.enc_id as "Encounter ID",
    vw_ed."Arrive ED",
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (
                case
                    when (vw_ed."EDECU Arrival Time" notnull) then vw_ed."EDECU Arrival Time"
                    else vw_ed."Depart ED"
                end - vw_ed."Earliest MD Eval"
            )
        ) / 60
    ) as "MD Eval to Pt Left ED Min",
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (
                case
                    when (vw_ed."EDECU Arrival Time" notnull) then vw_ed."EDECU Arrival Time"
                    else vw_ed."Depart ED"
                end - vw_ed."Arrive ED"
            )
        ) / 60
    ) as "ED Length of Stay Min",
    case
        when (vw_ed."EDECU Arrival Time" notnull) then (
            date_part(
                'EPOCH' :: "VARCHAR",
                (vw_ed."Depart ED" - vw_ed."EDECU Arrival Time")
            ) / 60
        )
        else null :: int8
    end as "EDECU Length of Stay Min",
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (vw_ed."Triage Start" - vw_ed."Arrive ED")
        ) / 60
    ) as "Arrival to Triage Min",
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (vw_ed."Earliest MD Eval" - vw_ed."Arrive ED")
        ) / 60
    ) as "Arrival to MD Eval Min",
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (
                vw_ed."Admission Form Bed Request" - vw_ed."Earliest MD Eval"
            )
        ) / 60
    ) as "MD Eval to Bed Request Min",
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (
                vw_ed."MD Report" - vw_ed."Admission Form Bed Request"
            )
        ) / 60
    ) as "Bed Request to MD Report Min",
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (
                case
                    when (vw_ed."EDECU Arrival Time" notnull) then vw_ed."EDECU Arrival Time"
                    else vw_ed."Depart ED"
                end - vw_ed."MD Report"
            )
        ) / 60
    ) as "MD Report to Pt Left Min",
    (
        date_part(
            'EPOCH' :: "VARCHAR",
            (
                case
                    when (vw_ed."EDECU Arrival Time" notnull) then vw_ed."EDECU Arrival Time"
                    else vw_ed."Depart ED"
                end - vw_ed."Earliest RN Report"
            )
        ) / 60
    ) as "RN Report to Pt Left Min",
    case
        when (vai.edecu_arrvl_dt notnull) then 1
        else 0
    end as "EDECU Ind",
    case
        when (
            vai.cuml_room_nm ~~ like_escape(
                '%ED RES%' :: "VARCHAR",
                '\'::"VARCHAR")) then 1 else 0 
end as "ED Resuscitation Rmm Use Ind", 
case 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull))) then ' Indeterminate '::"VARCHAR" when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" notnull))) then ' Admit '::"VARCHAR" when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) 
and (hef."Admitting Department" isnull))) then ' EDECU - Discharge '::"VARCHAR" when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) 
AND (hef."Admitting Department" NOTNULL))) THEN ' edecu - admit '::"varchar" WHEN ((DICT2.DICT_NM = ' transfered to another facility(
                    not
                    from
                        triage
                ) '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
NOTNULL)) THEN ' edecu - transfered to another facility(
                    not
                    from
                        triage
                ) '::"VARCHAR" when ((vai.edecu_arrvl_dt 
notnull) 
and ((dep2.dept_abbr = ' EDEC '::"VARCHAR") 
or (dep2.dept_abbr = ' ED '::"VARCHAR"))) then ' EDECU - Discharge '::"VARCHAR" when (((vai.edecu_arrvl_dt 
notnull) 
and (dep2.dept_abbr <> ' EDEC '::"VARCHAR")) 
and (hef."Admitting Department" isnull)) then ' EDECU - Admit '::"VARCHAR" when ((vai.edecu_arrvl_dt 
notnull) 
and (hef."Admitting Department" notnull)) then ' EDECU - Admit '::"VARCHAR" when ((vai.edecu_arrvl_dt 
notnull) 
and (hef."Admitting Department" isnull)) then ' EDECU - Discharge '::"VARCHAR" when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt 
notnull)) 
and (dep2.dept_abbr <> ' EDECU '::"VARCHAR")) then ' EDECU - Admit '::"VARCHAR" when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt 
notnull)) 
and (dep2.dept_abbr = ' EDECU '::"VARCHAR")) then ' EDECU - Discharge '::"VARCHAR" when ((((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull)) 
and (dep2.dept_abbr = ' PERIOP '::"VARCHAR")) then ' Admit '::"VARCHAR" when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull)) then ' Discharge '::"VARCHAR" when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" notnull)) then ' Admit '::"VARCHAR" when ((dict2.dict_nm = ' HACU '::"VARCHAR") 
AND (hef."Admitting Department" ISNULL)) THEN ' transfer
                from
                    Triage to HACU '::"VARCHAR" when ((dict2.dict_nm = ' HACU '::"VARCHAR") 
and (hef."Admitting Department" notnull)) then ' Admit '::"VARCHAR" else dict2.dict_nm 
end as "ED Disposition", 
case 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull))) then ' INDETERMINATE '::"VARCHAR" when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" notnull))) then ' ADMIT '::"VARCHAR" when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) 
and (hef."Admitting Department" isnull))) then ' EDECU '::"VARCHAR" when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) 
AND (hef."Admitting Department" NOTNULL))) THEN ' edecu '::"varchar" WHEN ((DICT2.DICT_NM = ' transfered to another facility(
                        not
                        from
                            triage
                    ) '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) then ' EDECU '::"VARCHAR" when ((vai.edecu_arrvl_dt 
notnull) 
and ((dep2.dept_abbr = ' EDEC '::"VARCHAR") 
or (dep2.dept_abbr = ' ED '::"VARCHAR"))) then ' EDECU '::"VARCHAR" when (((vai.edecu_arrvl_dt 
notnull) 
and (dep2.dept_abbr <> ' EDEC '::"VARCHAR")) 
and (hef."Admitting Department" isnull)) then ' EDECU '::"VARCHAR" when ((vai.edecu_arrvl_dt 
notnull) 
and (hef."Admitting Department" notnull)) then ' EDECU '::"VARCHAR" when ((vai.edecu_arrvl_dt 
notnull) 
and (hef."Admitting Department" isnull)) then ' EDECU '::"VARCHAR" when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt 
notnull)) 
and (dep2.dept_abbr <> ' EDECU '::"VARCHAR")) then ' EDECU '::"VARCHAR" when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt 
notnull)) 
and (dep2.dept_abbr = ' EDECU '::"VARCHAR")) then ' EDECU '::"VARCHAR" when ((((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull)) 
and (dep2.dept_abbr = ' PERIOP '::"VARCHAR")) then ' ADMIT '::"VARCHAR" when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull)) then ' DISCHARGE '::"VARCHAR" when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" notnull)) then ' ADMIT '::"VARCHAR" when ((dict2.dict_nm = ' HACU '::"VARCHAR") 
AND (hef."Admitting Department" ISNULL)) THEN ' transfer
                from
                    TRIAGE '::"VARCHAR" when ((dict2.dict_nm = ' HACU '::"VARCHAR") 
AND (hef."Admitting Department" NOTNULL)) THEN ' admit '::"varchar" WHEN (DICT2.DICT_NM ~~ LIKE_ESCAPE(' % eloped % '::"varchar", ' \ '::"varchar")) THEN ' discharged '::"varchar" WHEN (DICT2.DICT_NM ~~ LIKE_ESCAPE(' lwbs % '::"varchar", ' \ '::"varchar")) THEN ' lwbs '::"varchar" WHEN (DICT2.DICT_NM ~~ LIKE_ESCAPE(' transfer % '::"varchar", ' \ '::"varchar")) THEN ' transfer
                from
                    TRIAGE '::"VARCHAR" when (dict2.dict_nm ~~ like_escape(' Dece % '::"VARCHAR", ' \ '::"VARCHAR")) then ' DECEASED '::"VARCHAR" else upper(dict2.dict_nm) 
end as "ED General Disposition", 
case 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull))) then 1 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" notnull))) then 1 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) 
and (hef."Admitting Department" isnull))) then 1 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) 
and (hef."Admitting Department" notnull))) then 1 
WHEN ((DICT2.DICT_NM = ' transfered to another facility(
                        not
                        from
                            triage
                    ) '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) then 1 
when ((vai.edecu_arrvl_dt 
notnull) 
and ((dep2.dept_abbr = ' EDEC '::"VARCHAR") 
or (dep2.dept_abbr = ' ED '::"VARCHAR"))) then 1 
when (((vai.edecu_arrvl_dt 
notnull) 
and (dep2.dept_abbr <> ' EDEC '::"VARCHAR")) 
and (hef."Admitting Department" isnull)) then 1 
when ((vai.edecu_arrvl_dt 
notnull) 
and (hef."Admitting Department" notnull)) then 1 
when ((vai.edecu_arrvl_dt 
notnull) 
and (hef."Admitting Department" isnull)) then 1 
when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt 
notnull)) 
and (dep2.dept_abbr <> ' EDECU '::"VARCHAR")) then 1 
when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt 
notnull)) 
and (dep2.dept_abbr = ' EDECU '::"VARCHAR")) then 1 
when ((((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull)) 
and (dep2.dept_abbr = ' PERIOP '::"VARCHAR")) then 1 
when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull)) then 1 
when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" notnull)) then 1 
when ((dict2.dict_nm = ' HACU '::"VARCHAR") 
and (hef."Admitting Department" isnull)) then 1 
when ((dict2.dict_nm = ' HACU '::"VARCHAR") 
and (hef."Admitting Department" notnull)) then 1 
when (dict2.dict_nm ~~ like_escape(' % Eloped % '::"VARCHAR", ' \ '::"VARCHAR")) then 1 
when (dict2.dict_nm ~~ like_escape(' LWBS % '::"VARCHAR", ' \ '::"VARCHAR")) then 1 
when (dict2.dict_nm ~~ like_escape(' Transfer % '::"VARCHAR", ' \ '::"VARCHAR")) then 1 
when (dict2.dict_nm ~~ like_escape(' Dece % '::"VARCHAR", ' \ '::"VARCHAR")) then 1 else 1 
end as "ED Patients Presenting", 
case 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull))) then 1 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" notnull))) then 1 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) 
and (hef."Admitting Department" isnull))) then 1 
when ((((vai.dict_dspn_key = -1) 
or (vai.dict_dspn_key = 0)) 
or (vai.dict_dspn_key = -2)) 
or (((dict2.dict_nm = ' '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) 
and (hef."Admitting Department" notnull))) then 1 
WHEN ((DICT2.DICT_NM = ' transfered to another facility(
                        not
                        from
                            triage
                    ) '::"VARCHAR") 
and (vai.edecu_arrvl_dt 
notnull)) then 1 
when ((vai.edecu_arrvl_dt 
notnull) 
and ((dep2.dept_abbr = ' EDEC '::"VARCHAR") 
or (dep2.dept_abbr = ' ED '::"VARCHAR"))) then 1 
when (((vai.edecu_arrvl_dt 
notnull) 
and (dep2.dept_abbr <> ' EDEC '::"VARCHAR")) 
and (hef."Admitting Department" isnull)) then 1 
when ((vai.edecu_arrvl_dt 
notnull) 
and (hef."Admitting Department" notnull)) then 1 
when ((vai.edecu_arrvl_dt 
notnull) 
and (hef."Admitting Department" isnull)) then 1 
when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt 
notnull)) 
and (dep2.dept_abbr <> ' EDECU '::"VARCHAR")) then 1 
when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt 
notnull)) 
and (dep2.dept_abbr = ' EDECU '::"VARCHAR")) then 1 
when ((((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull)) 
and (dep2.dept_abbr = ' PERIOP '::"VARCHAR")) then 1 
when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" isnull)) then 1 
when (((((dict2.dict_nm = ' Admit '::"VARCHAR") 
OR (DICT2.DICT_NM = '
                    OR '::"VARCHAR")) 
or (dict2.dict_nm = ' EDECU '::"VARCHAR")) 
and (vai.edecu_arrvl_dt isnull)) 
and (hef."Admitting Department" notnull)) then 1 
when ((dict2.dict_nm = ' HACU '::"VARCHAR") 
and (hef."Admitting Department" isnull)) then 1 
when ((dict2.dict_nm = ' HACU '::"VARCHAR") 
and (hef."Admitting Department" notnull)) then 1 
when (dict2.dict_nm ~~ like_escape(' % Eloped % '::"VARCHAR", ' \ '::"VARCHAR")) then 1 
when (dict2.dict_nm ~~ like_escape(' LWBS % '::"VARCHAR", ' \ '::"VARCHAR")) then 0 
when (dict2.dict_nm ~~ like_escape(' Transfer % '::"VARCHAR", ' \ '::"VARCHAR")) then 1 
when (dict2.dict_nm ~~ like_escape(' Dece % '::"VARCHAR", ' \ '::"VARCHAR")) then 1 else 1 
end as "ED Patients Seen" from ((((((((((
select missing_alias."Encounter Key", missing_alias."Arrive ED", missing_alias."Depart ED", missing_alias."Triage Start", missing_alias."Triage End", missing_alias."Assign RN", missing_alias."Assign Resident NP", missing_alias."Assign 1st Attending", missing_alias."Registration Start", missing_alias."Roomed ED", missing_alias."Registration End", missing_alias."ED Conference Review", missing_alias."MD Evaluation", missing_alias."Attending Evaluation", missing_alias."After Visit Summary Printed", missing_alias."MD Report", missing_alias."Paged IP RN", missing_alias."Paged IP MD", missing_alias."IP Bed Assigned", missing_alias."Admission Form Bed Request", missing_alias."Triage RN Name", missing_alias."Earliest MD Eval", missing_alias."Earliest RN Report", missing_alias."EDECU Arrival Time" from (
select ve.visit_key as "Encounter Key", row_number() 
over (
partition by ve.visit_key  
order by ve.pat_key ) as col1, min(
case 
when (et.event_id = ' 50 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Arrive ED", max(
case 
when (et.event_id = ' 95 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Depart ED", min(
case 
when (et.event_id = ' 205 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Triage Start", max(
case 
when (et.event_id = ' 210 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Triage End", min(
case 
when (et.event_id = ' 120 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Assign RN", min(
case 
when (et.event_id = ' 300121 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Assign Resident NP", min(
case 
when (et.event_id = ' 111 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Assign 1st Attending", min(
case 
when (et.event_id = ' 55 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Registration Start", min(
case 
when (et.event_id = ' 55 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Roomed ED", max(
case 
when (et.event_id = ' 220 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Registration End", max(
case 
when (et.event_id = ' 300711 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "ED Conference Review", min(
case 
when (et.event_id = ' 30020501 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "MD Evaluation", min(
case 
when (et.event_id = ' 30020502 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Attending Evaluation", min(
case 
when (et.event_id = ' 85 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "After Visit Summary Printed", min(
case 
when (et.event_id = ' 300100 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "MD Report", min(
case 
when (et.event_id = ' 300101 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Paged IP RN", min(
case 
when (et.event_id = ' 300103 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Paged IP MD", min(
case 
when (et.event_id = ' 300105 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "IP Bed Assigned", min(
case 
when (et.event_id = ' 231 '::int8) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Admission Form Bed Request", min(
case 
when (et.event_id = ' 205 '::int8) then emp.full_nm else 
null::"VARCHAR" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Triage RN Name", min(
case 
when (((et.event_id = ' 111 '::int8) 
or (et.event_id = ' 300121 '::int8)) 
or (et.event_id = ' 300103 '::int8)) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Earliest MD Eval", min(
case 
when ((((et.event_id = ' 300102 '::int8) 
or (et.event_id = ' 300103 '::int8)) 
or ((et.event_id = ' 300122 '::int8) 
or (et.event_id = ' 300940 '::int8))) 
or (et.event_id = ' 300941 '::int8)) then ve.event_dt else 
null::"TIMESTAMP" end) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "Earliest RN Report", min(vai.edecu_arrvl_dt) 
over (
partition by ve.visit_key 
rows between unbounded preceding and unbounded following) as "EDECU Arrival Time" from (((
{{source('cdw', 'visit_ed_event')}} as ve 
join {{source('cdw', 'master_event_type')}} as et on ((ve.event_type_key = et.event_type_key))) 
left join {{source('cdw', 'employee')}} as emp on ((ve.event_init_emp_key = emp.emp_key))) 
left join {{source('cdw', 'visit_addl_info')}} as vai on ((ve.visit_key = vai.visit_key))) 
where ((et.event_id 
in (' 50 '::int8, ' 55 '::int8, ' 95 '::int8, ' 205 '::int8, ' 210 '::int8, ' 300121 '::int8, ' 120 '::int8, ' 111 '::int8, ' 215 '::int8, ' 220 '::int8, ' 300711 '::int8, ' 30020501 '::int8, ' 30020502 '::int8, ' 85 '::int8, ' 300100 '::int8, ' 300101 '::int8, ' 300105 '::int8, ' 231 '::int8, ' 300112 '::int8, ' 300103 '::int8)) 
and (ve.visit_key <> -1))) missing_alias 
where (missing_alias.col1 = 1)) vw_ed 
left join {{source('cdw', 'visit_addl_info')}} as vai on ((vw_ed."Encounter Key" = vai.visit_key))) 
left join {{source('cdw', 'department')}} as dep on ((vai.last_dept_key = dep.dept_key))) 
left join {{source('cdw', 'location')}} as loc on ((dep.rev_loc_key = loc.loc_key))) 
left join {{source('cdw', 'cdw_dictionary')}} as dict2 on ((vai.dict_dspn_key = dict2.dict_key))) 
left join {{ref('vw_essa_hosp_encounter_fact')}} as hef on ((vai.visit_key = hef."Encounter Key"))) 
left join {{source('cdw', 'visit')}} as visit on ((vai.visit_key = visit.visit_key))) 
left join {{source('cdw', 'hospital_account_visit')}} as hav on (((vai.visit_key = hav.visit_key) 
and (hav.pri_visit_ind = 1)))) 
left join {{source('cdw', 'hospital_account')}} as ha on ((hav.hsp_acct_key = ha.hsp_acct_key))) 
left join {{source('cdw', 'department')}} as dep2 on ((ha.disch_dept_key = dep2.dept_key))) 
where ((loc.loc_id 
in ((' 1026.000 '::numeric(14,3))::numeric(14,3))) 
and ((vw_ed."Arrive ED" >= "TIMESTAMP"(to_date(' 07222013 '::"VARCHAR", ' MMDDYYYY '::"VARCHAR"))) 
and (vw_ed."Arrive ED" <= "TIMESTAMP"(date(' now(0) '::"VARCHAR")))))
