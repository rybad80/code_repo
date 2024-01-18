SELECT 
V.PAT_KEY AS "Patient Key",
 DA.DICT_NM AS "Accomodation Name",
 SA.SVC_AREA_ID AS "ADT Service Area ID",
 SA.SVC_AREA_NM AS "ADT Service Area Name",
 NULL::UNKNOWN AS "ADT Patient Status Name",
 MB.BED_ID AS "Bed ID",
 V.VISIT_KEY AS "Encounter Key",
 HV.HSP_ACCT_KEY AS "Hospital Account Key",
 V.ENC_ID AS "Encounter ID",
 TO_DATE("VARCHAR"(V.CONTACT_DT_KEY),
 'YYYYMMDD'::"VARCHAR") AS "Encounter Contact Date",
 V.APPT_CANCEL_DT AS "Encounter Canceled Date",
 V.APPT_CANCEL_24HR_IND AS "Encounter Canceled 24 Ind",
 V.APPT_CANCEL_48HR_IND AS "Encounter Canceled 48 Ind",
 CASE 
    WHEN (DATE_TRUNC('day'::"VARCHAR", VAI.HOSP_ADMIT_DT) NOTNULL) THEN DATE_TRUNC('day'::"VARCHAR", VAI.HOSP_ADMIT_DT) 
    WHEN (TO_DATE("VARCHAR"(V.CONTACT_DT_KEY),'YYYYMMDD'::"VARCHAR") NOTNULL) THEN "TIMESTAMP"(TO_DATE("VARCHAR"(V.CONTACT_DT_KEY),'YYYYMMDD'::"VARCHAR")) 
    ELSE NULL::"TIMESTAMP" 
 END AS "Encounter Start Date",
 CASE
    WHEN (DATE_TRUNC('day'::"VARCHAR", VAI.HOSP_DISCH_DT) NOTNULL) THEN DATE_TRUNC('day'::"VARCHAR", VAI.HOSP_DISCH_DT) 
    WHEN (TO_DATE("VARCHAR"(V.CONTACT_DT_KEY), 'YYYYMMDD'::"VARCHAR") NOTNULL) THEN "TIMESTAMP"(TO_DATE("VARCHAR"(V.CONTACT_DT_KEY), 'YYYYMMDD'::"VARCHAR")) 
    ELSE NULL::"TIMESTAMP" 
 END AS "Encounter End Date",
 V.AGE AS "Encounter Patient Age",
 V.AGE_DAYS AS "Encounter Patient Age Days",
 CASE 
    WHEN (V.AGE_DAYS < '30'::NUMERIC(2, 0)) THEN '1 Neonate(<30 days)'::"VARCHAR" 
    WHEN ((V.AGE_DAYS >= '30'::NUMERIC(2, 0)) AND (V.AGE < '1'::NUMERIC(1,0))) THEN '2 Infancy(>=30 days and <1 year)'::"VARCHAR" 
    WHEN ((V.AGE >= '1'::NUMERIC(1,0)) AND (V.AGE < '5'::NUMERIC(1,0))) THEN '3 Early Childhood(>=1 year and <5 years)'::"VARCHAR" 
    WHEN ((V.AGE >= '5'::NUMERIC(1,0)) AND (V.AGE < '13'::NUMERIC(2,0))) THEN '4 Late Childhood(>=5 years and <13 years)'::"VARCHAR" 
    WHEN ((V.AGE >= '13'::NUMERIC(2,0)) AND (V.AGE < '18'::NUMERIC(2,0))) THEN '5 Adolescence(>=13 years and <18 years)'::"VARCHAR" 
    WHEN ((V.AGE >= '18'::NUMERIC(2,0)) AND (V.AGE < '30'::NUMERIC(2,0))) THEN '6 Adult(>=18 years and <30)'::"VARCHAR" 
    WHEN (V.AGE >= '30'::NUMERIC(2,0)) THEN '7 Adult(>=30 years)'::"VARCHAR" 
    ELSE 'Invalid'::"VARCHAR" 
 END AS "Pediatric Patient Age Grouper",
 DICT1.DICT_NM AS "Encounter Type",
 P1.PAYOR_NM AS "Encounter Payor Name",
 P1.PAYOR_ID AS "Encounter Payor ID",
 P1.PAYOR_KEY AS "Encounter Payor Key",
 FC1.FC_NM AS "Encounter Financial Class Name",
 DEP1.DEPT_NM AS "Encounter Department Name",
 DEP1.DEPT_ABBR AS "Encounter Department Abbr",
 DEP1.SPECIALTY AS "Encounter Department Specialty",
 PROV1.PROV_ID AS "Encounter Provider ID",
 PROV1.FULL_NM AS "Encounter Provider Name",
 PROV1.PROV_TYPE AS "Encounter Provider Type",
 PROV_SPEC1.SPEC_NM AS "Encounter Provider Specialty",
 PROV3.PROV_KEY AS "Referring Provider Key",
 PROV2.PROV_KEY AS "Admission Provider Key",
 PROV3.PROV_ID AS "Referral Provider ID",
 PROV3.FULL_NM AS "Referral Provider Name",
 REF_PROV_ADDR.ADDR_LINE1 AS "Referral Provider Address1",
 REF_PROV_ADDR.ADDR_LINE2 AS "Referral Provider Address2",
 REF_PROV_ADDR.CITY AS "Referral Provider City",
 REF_PROV_ADDR."STATE" AS "Referral Provider State Name",
 REF_PROV_ADDR.ZIP AS "Referral Provider ZIP",
 REF_PROV_ADDR.PHONE AS "Referral Provider Phone",
 PROV_SPEC3.SPEC_NM AS "Referral Provider Specialty",
 PROV2.PROV_ID AS "Admission Provider ID",
 PROV2.FULL_NM AS "Admission Provider Name",
 PROV_SPEC2.SPEC_NM AS "Admission Provider Specialty",
 PROV4.PROV_ID AS "Discharge Provider ID",
 PROV4.PROV_KEY AS "Discharge Provider Key",
 PROV4.FULL_NM AS "Discharge Provider Name",
 PROV_SPEC4.SPEC_NM AS "Discharge Provider Specialty",
 DATE_TRUNC('day'::"VARCHAR",
 VAI.HOSP_ADMIT_DT) AS "Hospital Admission Date",
 DATE_TRUNC('day'::"VARCHAR",
 VAI.HOSP_DISCH_DT) AS "Hospital Discharge Date",
 VAI.HOSP_ADMIT_DT AS "Hospital Admission DateTime",
 VAI.HOSP_DISCH_DT AS "Hospital Discharge DateTime",
 TO_CHAR(VAI.HOSP_ADMIT_DT,
 'HH24'::"VARCHAR") AS "Hospital Admission Time Hour",
 TO_CHAR(VAI.HOSP_DISCH_DT,
 'HH24'::"VARCHAR") AS "Hospital Discharge Time Hour",
 DICT2.DICT_NM AS "ED Patient Disposition",
 DICT3.DICT_NM AS "Hospital Patient Class",
 CASE WHEN (DICT3.DICT_NM NOTNULL) THEN DICT3.DICT_NM WHEN ('Outpatient' NOTNULL) THEN 'Outpatient'::"VARCHAR" ELSE NULL::"VARCHAR" END AS "Encounter Patient Class",
 DICT4.DICT_NM AS "Hospital Admission Type",
 CASE WHEN (UPPER(DICT4.DICT_NM) = 'ELECTIVE'::"VARCHAR") THEN 'YES'::"VARCHAR" ELSE 'NO'::"VARCHAR" END AS "ADT Elective",
 DICT5.DICT_NM AS "Hospital Service",
 DICT6.DICT_NM AS "Admission Source",
 DICT8.DICT_NM AS "Hospital Admission Status",
 DICT9.DICT_NM AS "Hospital Discharge Disposition",
 DICT_PAT_STAT.DICT_NM AS "Hospital Discharge Destination",
 PRIM_DX.DX_NM AS "Encounter Primary Diagnosis",
 PRIM_DX.DX_KEY AS "Diagnosis Key",
 PRIM_DX.ICD9_CD AS "Encounter Primary ICD9 Code",
 PRIM_DX.ICD10_CD AS "Encounter Primary ICD10 Code",
 V.LESS_72HR_HOSP_ADMIT_IND AS "72HR Readmission Indicator",
 V.LESS_72HR_VISIT_KEY AS "72HR Readmission Encounter Key",
 VAI.ED_IND AS "ED Indicator",
 V.LOS_HOURS AS "Length of Stay Hours",
 CASE WHEN (V.LOS_HOURS > '0'::NUMERIC) THEN (ADMIN.DAYS_BETWEEN(DATE_TRUNC('day'::"VARCHAR",VAI.HOSP_ADMIT_DT),DATE_TRUNC('day'::"VARCHAR",CASE WHEN (VAI.HOSP_DISCH_DT NOTNULL) THEN VAI.HOSP_DISCH_DT WHEN (NOW() NOTNULL) THEN NOW() ELSE NULL::"TIMESTAMP" END)) + 1) ELSE NULL::INT4 END AS "Length of Stay Days",
 CASE WHEN (((((DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR")) OR (DICT1.DICT_NM = 'CONFIDENTIAL VISIT'::"VARCHAR")) AND ((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR"))) AND (HV.PRI_VISIT_IND ISNULL)) THEN 'YES'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR")) OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR"))) AND (((DICT3.DICT_NM = 'Inpatient'::"VARCHAR") OR (DICT3.DICT_NM = 'Admit After Surgery'::"VARCHAR")) OR ((DICT3.DICT_NM = 'Admit After Surgery-IP'::"VARCHAR") OR (DICT3.DICT_NM = 'IP Deceased Organ Donor'::"VARCHAR")))) THEN 'YES'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR")) OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR"))) AND ((DICT3.DICT_NM = 'Outpatient'::"VARCHAR") OR (DICT3.DICT_NM = 'Day Surgery'::"VARCHAR"))) THEN 'YES'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND ((HV.PRI_VISIT_IND = 0) OR (HV.PRI_VISIT_IND = 1))) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR")) OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR"))) AND (DICT3.DICT_NM = 'Recurring Outpatient'::"VARCHAR")) THEN 'YES'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR")) OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR"))) AND (DICT3.DICT_NM = 'Emergency'::"VARCHAR")) THEN 'YES'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR")) OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR"))) AND ((DICT3.DICT_NM = 'Observation'::"VARCHAR") OR (DICT3.DICT_NM = 'Admit After Surgery-OBS'::"VARCHAR"))) THEN 'YES'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR")) OR (DICT1.DICT_NM = 'CONFIDENTIAL VISIT'::"VARCHAR")) AND ((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (DICT3.DICT_NM ISNULL)) THEN 'YES'::"VARCHAR" WHEN ((((DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR") AND (DICT7.DICT_NM = 'COMPLETED'::"VARCHAR")) AND (HV.PRI_VISIT_IND = 0)) AND (DICT3.DICT_NM ISNULL)) THEN 'YES'::"VARCHAR" ELSE 'NO'::"VARCHAR" END AS "Last Encounter Stay Indicator",
 CASE WHEN (((((DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR")) OR (DICT1.DICT_NM = 'CONFIDENTIAL VISIT'::"VARCHAR")) AND ((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR"))) AND (HV.PRI_VISIT_IND ISNULL)) THEN 'Non-Hospital OP Encounter'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR")) OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR"))) AND (((DICT3.DICT_NM = 'Inpatient'::"VARCHAR") OR (DICT3.DICT_NM = 'Admit After Surgery'::"VARCHAR")) OR ((DICT3.DICT_NM = 'Admit After Surgery-IP'::"VARCHAR") OR (DICT3.DICT_NM = 'IP Deceased Organ Donor'::"VARCHAR")))) THEN 'Hospital IP Encounter'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR")) OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR"))) AND ((DICT3.DICT_NM = 'Outpatient'::"VARCHAR") OR (DICT3.DICT_NM = 'Day Surgery'::"VARCHAR"))) THEN 'Hospital OP Encounter'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND ((HV.PRI_VISIT_IND = 0) OR (HV.PRI_VISIT_IND = 1))) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR")) OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR"))) AND (DICT3.DICT_NM = 'Recurring Outpatient'::"VARCHAR")) THEN 'Hospital OP Encounter'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR")) OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR"))) AND (DICT3.DICT_NM = 'Emergency'::"VARCHAR")) THEN 'Hospital ED Encounter'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'HOSPITAL ENCOUNTER'::"VARCHAR") OR (DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR")) OR ((DICT1.DICT_NM = 'PROCEDURE ONLY'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR")) OR (DICT7.DICT_NM = 'NOT APPLICABLE'::"VARCHAR"))) AND ((DICT3.DICT_NM = 'Observation'::"VARCHAR") OR (DICT3.DICT_NM = 'Admit After Surgery-OBS'::"VARCHAR"))) THEN 'Hospital OBS Encounter'::"VARCHAR" WHEN ((((((DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR") OR (DICT1.DICT_NM = 'APPOINTMENT'::"VARCHAR")) OR (DICT1.DICT_NM = 'CONFIDENTIAL VISIT'::"VARCHAR")) AND ((DICT7.DICT_NM = 'COMPLETED'::"VARCHAR") OR (DICT7.DICT_NM = 'ARRIVED'::"VARCHAR"))) AND (HV.PRI_VISIT_IND = 1)) AND (DICT3.DICT_NM ISNULL)) THEN 'Non-Hospital OP Encounter'::"VARCHAR" WHEN ((((DICT1.DICT_NM = 'OFFICE VISIT'::"VARCHAR") AND (DICT7.DICT_NM = 'COMPLETED'::"VARCHAR")) AND (HV.PRI_VISIT_IND = 0)) AND (DICT3.DICT_NM ISNULL)) THEN 'Non-Hospital OP Encounter'::"VARCHAR" ELSE NULL::"VARCHAR" END AS "Last Encounter Stay Class",
 DICT10.DICT_NM AS "Encounter Patient Acuity",
 DICT7.DICT_NM AS "Encounter Appointment Status",
 V.APPT_DT AS "Encounter Appointment DateTime",
 MVT.VISIT_TYPE_NM AS "Encounter Appointment Type",
 CASE 
    WHEN (DEP1.RPT_GRP_7 <> 'CHOP MAIN'::"VARCHAR") THEN 'Non-Hospital'::"VARCHAR" 
    WHEN (DEP1.DEPT_NM ~~ LIKE_ESCAPE('VIRT %'::"VARCHAR",'\'::"VARCHAR")) THEN 'Non-Hospital'::"VARCHAR" 
    WHEN (HV.PRI_VISIT_IND ISNULL) THEN 'Non-Hospital'::"VARCHAR" 
    ELSE 'Hospital'::"VARCHAR" 
END AS "Encounter Class",
 LOC1.LOC_NM AS "Encounter Revenue Location",
 LOC1.LOC_ID AS "Encounter Revenue Location ID",
 LOC1.RPT_GRP_6 AS "Encounter Revenue Location Grp",
 V.APPT_MADE_DT AS "Encounter Appt Made Date",
 V.CONTACT_DT_KEY,
 MVT.VISIT_TYPE_ID AS "Encounter Appnt Procedure ID",
 V.APPT_MADE_DT AS "Enc Appnt Made Date",
 V.APPT_VISIT_TYPE_KEY AS "Encounter Procedure Key",
 V.APPT_SN AS "Encounter Serial Number",
 LD.DEPT_ID AS "Last Department ID",
 LD.DEPT_NM AS "Last Department Name",
 LD.DEPT_KEY AS "Last Department Key",
 RFL.AUTH_NUM AS "Authorization Number",
 PRG.PRGM_NM AS "Program Name",
 V.APPT_CHECKIN_DT AS "Encounter Checkin Date",
 V.APPT_CHECKOUT_DT AS "Encounter Checkout Date",
 DARV.DICT_NM AS "Means of Arrival Name",
 DDPT.DICT_NM AS "Means of Depature Name",
 MRM.ROOM_ID AS "Last Room ID",
 V.ACCT_KEY AS "Account ID",
 NULL::UNKNOWN AS "Cancel Reason Name",
 V.COPAY_DUE AS "Copay Due",
 V.COPAY_COLL AS "Copay Collected",
 CVG.CVG_ID AS "Coverage ID",
 V.CHRG_SLIP_NUM AS "Charge Slip Number",
 DEP1.DEPT_ID AS "Encounter Department ID",
 V.ENC_CLOSED_IND AS "Encounter Closed Indicator",
 EFDEP.DEPT_ID AS "Effective Department ID",
 EFDEP.DEPT_KEY AS "Effective Department Key",
 FC1.FC_ID AS "Financial Class ID",
 HA.HSP_ACCT_ID AS "Hospital Account ID",
 PROC.PROC_NM AS "LOS Primary Procedure",
 LOC.LOC_ID AS "Primary Location ID",
 LOC.LOC_NM AS "Primary Location Name",
 RFL.PRE_CERT_NUM AS "Pre-certification Number",
 CVG.SUBSCR_NM AS "Coverage Subscriber Name",
 CVG.SUBSCR_NUM AS "Coverage Subscriber Number",
 BP.BP_ID AS "Encounter Benefit Plan ID",
 BP.BP_NM AS "Encounter Benefit Plan Name",
 PCP.PROV_ID AS "Encounter PC Provider ID",
 PCP.FULL_NM AS "Encounter PC Provider Name",
 PCP_ADDR.ADDR_LINE1 AS "Encounter PC Provider Address1",
 PCP_ADDR.ADDR_LINE2 AS "Encounter PC Provider Address2",
 PCP_ADDR.CITY AS "Encounter PC Provider City",
 PCP_ADDR."STATE" AS "Encounter PC Provider State",
 PCP_ADDR.ZIP AS "Encounter PC Provider ZIP",
 V.EFF_DT AS "Effective Date",
 CVG.CVG_KEY AS "Coverage Key",
 DICT11.DICT_NM AS "Patient Transfer From",
 RFL2.DICT_NM AS "Referral Required Ind",
 DIM_PH_STAT.PHONE_REMIND_STAT_NM AS "Phone Reminder Status Name" 
 FROM (((((((((((((((((((((((((((((((((((((((((((((((( 
{{source('cdw', 'visit') }} V 
LEFT JOIN {{ source('cdw', 'dim_phone_reminder_status') }} DIM_PH_STAT ON ((DIM_PH_STAT.DIM_PHONE_REMINDER_STAT_KEY = V.DIM_PHONE_REMINDER_STAT_KEY))) 
LEFT JOIN {{ source('cdw', 'visit_addl_info') }} VAI ON ((V.VISIT_KEY = VAI.VISIT_KEY))) 
LEFT JOIN {{ source('cdw', 'hospital_account_visit') }} HV ON ((V.VISIT_KEY = HV.VISIT_KEY))) 
LEFT JOIN {{ source('cdw', 'payor') }} P1 ON ((V.PAYOR_KEY = P1.PAYOR_KEY))) 
LEFT JOIN {{ source('cdw', 'financial_class') }} FC1 ON ((P1.FC_KEY = FC1.FC_KEY))) 
LEFT JOIN {{ source('cdw', 'department') }} DEP1 ON ((DEP1.DEPT_KEY = V.DEPT_KEY))) 
LEFT JOIN {{ source('cdw', 'location') }} LOC1 ON ((LOC1.LOC_KEY = DEP1.REV_LOC_KEY))) 
LEFT JOIN {{ source('cdw', 'referral') }} RFL ON ((V.RFL_KEY = RFL.RFL_KEY))) 
LEFT JOIN {{ source('cdw', 'provider') }} PROV1 ON ((PROV1.PROV_KEY = V.VISIT_PROV_KEY))) 
LEFT JOIN {{ source('cdw', 'provider_specialty') }} PROV_SPEC1 ON (((PROV_SPEC1.PROV_KEY = V.VISIT_PROV_KEY) AND (PROV_SPEC1.LINE = 1)))) 
LEFT JOIN {{ source('cdw', 'provider') }} PROV2 ON ((PROV2.PROV_KEY = VAI.ADMIT_PROV_KEY))) 
LEFT JOIN {{ source('cdw', 'provider_specialty') }} PROV_SPEC2 ON (((PROV_SPEC2.PROV_KEY = VAI.ADMIT_PROV_KEY) AND (PROV_SPEC2.LINE = 1)))) 
LEFT JOIN {{ source('cdw', 'provider') }} PROV3 ON ((PROV3.PROV_KEY = RFL.REFERRING_PROV_KEY))) 
LEFT JOIN {{ source('cdw', 'provider_addr') }} REF_PROV_ADDR ON (((PROV3.PROV_KEY = REF_PROV_ADDR.PROV_KEY) AND (REF_PROV_ADDR.LINE = 1)))) 
LEFT JOIN {{ source('cdw', 'provider_specialty') }} PROV_SPEC3 ON (((PROV_SPEC3.PROV_KEY = RFL.REFERRING_PROV_KEY) AND (PROV_SPEC3.LINE = 1)))) 
LEFT JOIN {{ source('cdw', 'provider') }} PROV4 ON ((PROV4.PROV_KEY = VAI.DISCHRG_PROV_KEY))) 
LEFT JOIN {{ source('cdw', 'provider_specialty') }} PROV_SPEC4 ON (((PROV_SPEC4.PROV_KEY = VAI.DISCHRG_PROV_KEY) AND (PROV_SPEC4.LINE = 1)))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT1 ON ((V.DICT_ENC_TYPE_KEY = DICT1.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT2 ON ((VAI.DICT_DSPN_KEY = DICT2.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT3 ON ((VAI.DICT_PAT_CLASS_KEY = DICT3.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT4 ON ((VAI.DICT_HSP_ADMSN_TYPE_KEY = DICT4.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT5 ON ((VAI.DICT_HOSP_SERV_KEY = DICT5.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT6 ON ((VAI.DICT_ADMIT_SRC_KEY = DICT6.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT7 ON ((V.DICT_APPT_STAT_KEY = DICT7.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT8 ON ((VAI.DICT_DISCHRG_DSPN_KEY = DICT8.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT9 ON ((VAI.DICT_DISCH_DEST_KEY = DICT9.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT10 ON ((VAI.DICT_ACUITY_KEY = DICT10.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'master_visit_type') }} MVT ON ((V.APPT_VISIT_TYPE_KEY = MVT.VISIT_TYPE_KEY))) 
LEFT JOIN (
    SELECT 
        PRIM_DX_LIST.VISIT_KEY,
        CASE WHEN (DX10.DX_KEY NOTNULL) THEN DX10.DX_KEY WHEN (DX9.DX_KEY NOTNULL) THEN DX9.DX_KEY ELSE NULL::INT8 END AS DX_KEY, 
        CASE WHEN (DX10.DX_GRP NOTNULL) THEN DX10.DX_GRP WHEN (DX9.DX_GRP NOTNULL) THEN DX9.DX_GRP ELSE NULL::"VARCHAR" END AS DX_GRP, 
        CASE WHEN (DX10.DX_NM NOTNULL) THEN DX10.DX_NM WHEN (DX9.DX_NM NOTNULL) THEN DX9.DX_NM ELSE NULL::"VARCHAR" END AS DX_NM, 
        DX9.ICD9_CD,
        DX10.ICD10_CD 
        FROM (
            ((SELECT VD.VISIT_KEY, MAX(CASE WHEN (VD_DICT2.DICT_NM /=/ 'VISIT - PRIMARY'::"VARCHAR") THEN VD.DX_KEY ELSE INT8(NULL::INT4) END) AS DX_VISIT_DX_KEY,
            MAX(CASE WHEN (VD_DICT2.DICT_NM /=/ 'HSP ACCT ADMIT - PRIMARY'::"VARCHAR") THEN VD.DX_KEY ELSE INT8(NULL::INT4) END) AS DX_ADMIT_DX_KEY,
            MAX(CASE WHEN (VD_DICT2.DICT_NM /=/ 'HSP ACCT FINAL - PRIMARY'::"VARCHAR") THEN VD.DX_KEY ELSE INT8(NULL::INT4) END) AS DX_FINAL_DX_KEY 
            FROM (
                 {{ source('cdw', 'visit_diagnosis') }} VD 
            JOIN {{ source('cdw', 'cdw_dictionary') }} VD_DICT2 ON ((VD_DICT2.DICT_KEY = VD.DICT_DX_STS_KEY))) 
            WHERE (VD_DICT2.DICT_NM IN (('VISIT - PRIMARY'::"VARCHAR")::VARCHAR(500), ('HSP ACCT ADMIT - PRIMARY'::"VARCHAR")::VARCHAR(500), ('HSP ACCT FINAL - PRIMARY'::"VARCHAR")::VARCHAR(500))) 
            GROUP BY VD.VISIT_KEY
            ) 
 PRIM_DX_LIST 
LEFT JOIN {{ source('cdw', 'diagnosis') }} DX9 ON ((((DX9.SEQ_NUM = 1) AND (DX9.ICD9_CD NOTNULL)) AND (DX9.DX_KEY = CASE WHEN (PRIM_DX_LIST.DX_FINAL_DX_KEY NOTNULL) THEN PRIM_DX_LIST.DX_FINAL_DX_KEY WHEN (PRIM_DX_LIST.DX_VISIT_DX_KEY NOTNULL) THEN PRIM_DX_LIST.DX_VISIT_DX_KEY WHEN (PRIM_DX_LIST.DX_ADMIT_DX_KEY NOTNULL) THEN PRIM_DX_LIST.DX_ADMIT_DX_KEY ELSE INT8(NULL::INT4) END)))) 
LEFT JOIN {{ source('cdw', 'diagnosis') }} DX10 ON ((((DX10.SEQ_NUM = 1) AND (DX10.ICD10_CD NOTNULL)) AND (DX10.DX_KEY = CASE WHEN (PRIM_DX_LIST.DX_FINAL_DX_KEY NOTNULL) THEN PRIM_DX_LIST.DX_FINAL_DX_KEY WHEN (PRIM_DX_LIST.DX_VISIT_DX_KEY NOTNULL) THEN PRIM_DX_LIST.DX_VISIT_DX_KEY WHEN (PRIM_DX_LIST.DX_ADMIT_DX_KEY NOTNULL) THEN PRIM_DX_LIST.DX_ADMIT_DX_KEY ELSE INT8(NULL::INT4) END))))) PRIM_DX ON ((V.VISIT_KEY = PRIM_DX.VISIT_KEY))) 
LEFT JOIN {{ source('cdw', 'service_area') }} SA ON ((V.SVC_AREA_KEY = SA.SVC_AREA_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DA ON ((VAI.DICT_ACCOM_KEY = DA.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'master_bed') }} MB ON ((VAI.LAST_BED_KEY = MB.BED_KEY))) 
LEFT JOIN {{ source('cdw', 'department') }} LD ON ((VAI.LAST_DEPT_KEY = LD.DEPT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DARV ON ((VAI.DICT_ARRVL_MODE_KEY = DARV.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DDPT ON ((VAI.DICT_DPART_MODE_KEY = DDPT.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'master_room') }} MRM ON ((VAI.LAST_ROOM_KEY = MRM.ROOM_KEY))) 
LEFT JOIN {{ source('cdw', 'program') }} PRG ON ((V.PRGM_KEY = PRG.PRGM_KEY))) 
LEFT JOIN {{ source('cdw', 'coverage') }} CVG ON ((V.CVG_KEY = CVG.CVG_KEY))) 
LEFT JOIN {{ source('cdw', 'department') }} EFDEP ON ((V.EFF_DEPT_KEY = EFDEP.DEPT_KEY))) 
LEFT JOIN {{ source('cdw', 'hospital_account') }} HA ON ((HV.HSP_ACCT_KEY = HA.HSP_ACCT_KEY))) 
LEFT JOIN {{ source('cdw', 'procedure') }} PROC ON ((V.PROC_KEY = PROC.PROC_KEY))) 
LEFT JOIN {{ source('cdw', 'location') }} LOC ON ((V.LOC_KEY = LOC.LOC_KEY))) 
LEFT JOIN {{ source('cdw', 'benefit_plan') }} BP ON ((V.BP_KEY = BP.BP_KEY))) 
LEFT JOIN {{ source('cdw', 'provider') }} PCP ON ((V.PC_PROV_KEY = PCP.PROV_KEY))) 
LEFT JOIN {{ source('cdw', 'provider_addr') }} PCP_ADDR ON (((PCP_ADDR.PROV_KEY = PCP.PROV_KEY) AND (PCP_ADDR.LINE = 1)))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT11 ON ((VAI.DICT_XFER_SRC_KEY = DICT11.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'vw_yes__no_ind') }} RFL2 ON ((V.RFL_REQ_IND = RFL2.DICT_KEY))) 
LEFT JOIN {{ source('cdw', 'cdw_dictionary') }} DICT_PAT_STAT ON ((VAI.DICT_PAT_STAT_KEY = DICT_PAT_STAT.DICT_KEY)))