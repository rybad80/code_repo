 WITH PHYS AS (
 SELECT 
      REFNO
      ,DOPER
     , ROW_NUMBER() OVER(PARTITION BY REFNO ORDER BY SEQNO) AS SEQ
FROM CDW_ODS_UAT..SENSIS_PHYS
) 

SELECT  DISTINCT
    --CASEINFO       
    '21' AS HOSPITALID,
    STUDY.ACCESSNO AS SENSISREFNUM,
    '' AS STSDIAGCODE,
    --STUDY.REFNO 
    ''  AS ISUNPLANNEDADMISSION,
        --PT.PATID MRN,
    --CASECLINICALINFO      
    '' AS OTH_AECODE,
    '' AS OTH_AESERIOUSNESS,
    '' AS AENOTES,
    '' AS REQUIREDRESOURCES,
    TO_CHAR(CT.PATIM, 'MM-DD-YYYY') AS CATHDATE,
    CASE WHEN PHYS.DOPER = 1 THEN '1081'
        WHEN PHYS.DOPER = 2 THEN '1082'
        WHEN PHYS.DOPER = 3 THEN '1083'
        WHEN PHYS.DOPER = 4 THEN '1084'
        WHEN PHYS.DOPER = 121 THEN '1760'
        WHEN PHYS.DOPER = 147 THEN '1780'
        ELSE ''
    END AS OPERATOR,
        --CAST(PD.BSA AS NUMERIC(4,2)) AS PATIENTBSA,
    --DAYS
    CASE
        WHEN
            DATE(
                CT.PATIM
            ) - DATE(PT.PATBIRTH) < 30 THEN DATE(NOW()) - DATE(PT.PATBIRTH)
        --MONTHS
        WHEN
            DATE(
                CT.PATIM
            ) - DATE(
                PT.PATBIRTH
            ) BETWEEN 30 AND 365 THEN (DATE(NOW()) - DATE(PT.PATBIRTH)) / 30
        --YEARS
        WHEN
            DATE(
                CT.PATIM
            ) - DATE(
                PT.PATBIRTH
            ) > 365 THEN (DATE(NOW()) - DATE(PT.PATBIRTH)) / 365
    END AS PATIENTAGE, --NEED FROM CATH GROUP
    CASE WHEN DATE(CT.PATIM) - DATE(PT.PATBIRTH) < 30 THEN 0 --DAYS
        --MONTHS
        WHEN DATE(CT.PATIM) - DATE(PT.PATBIRTH) BETWEEN 30 AND 365 THEN 1
        WHEN DATE(CT.PATIM) - DATE(PT.PATBIRTH) > 365 THEN 2 --YEARS
    END AS PATIENTAGETYPE,
    CASE WHEN PATSEX = 1 THEN '2'
        WHEN PATSEX = 2 THEN '1' END AS PATIENTSEX,
    COALESCE(CAST(PD.WEIGHT AS NUMERIC(5, 1)), 0) AS PATIENTWEIGHT,
    COALESCE(CAST(PD.HEIGHT AS NUMERIC(5, 1)), 0) AS PATIENTHEIGHT,
    CAST(((PD.WEIGHT * PD.HEIGHT) / 3600)^.5 AS NUMERIC(4, 2)) AS PATIENTBSA, --NEED THIS MAPPING
    --CASEPROCEDUREINFO     
    CASE
        WHEN DAYS_BETWEEN(CPOMRCP.CDATE, STUDY.STUDATE) <= 90 THEN '1' ELSE '0'
    END AS PREVCATHLAST90DIND,
    CASE
        WHEN DAYS_BETWEEN(CPOMRSP.SDATE, STUDY.STUDATE) <= 90 THEN '1' ELSE '0'
    END AS PREVSURGLAST90DIND,
    CASE WHEN GENCOND.IP3100 = 1 THEN '1'
        WHEN GENCOND.IP3105 = 1 THEN '1'
        WHEN GENCOND.IP3110 = 1 THEN '1'
        WHEN GENCOND.IP3115 = 1 THEN '1'
        WHEN GENCOND.IP3120 = 1 THEN '1'
        WHEN GENCOND.IP3125 = 1 THEN '1'
        WHEN GENCOND.IP3130 = 1 THEN '1'
        WHEN GENCOND.IP3135 = 1 THEN '1'
        WHEN GENCOND.IP3140 = 1 THEN '1'
        WHEN GENCOND.IP3145 = 1 THEN '1'
        WHEN GENCOND.IP3150 = 1 THEN '1'
        WHEN GENCOND.IP3155 = 1 THEN '1'
        ELSE '0' END AS GENSYNDROMEIND,
    CASE WHEN CPONCP IS NOT NULL THEN '1' ELSE '0' END AS NONCARDIACPROBIND,
    COALESCE(CPONCP, '') AS NONCARDIACPROBVALUES,
    --CASEEOCADMDISPOSITION     
    COALESCE(XRAYBSUM.FLTIME, '') AS FLUROTIME,
    COALESCE(XRAYBSUM.DOSE, '') AS TOTALDAP,
    COALESCE(
        TO_CHAR(COALESCE(ASR.SHTIME, CT.PATIM), 'HH24:MI'), ''
    ) AS SHEATHCATHINDATETIME,
    COALESCE(TO_CHAR(POCT.PREND, 'HH24:MI'), '') AS SHEATHCATHOUTDATETIME,
    CASE WHEN IPPOEVNT.IP8130 = 1 THEN '1' ELSE '0' END AS BLOODTRANSFUSION,
    CASE WHEN APTVER2.DPRSTAT = 1  THEN '1'
              WHEN APTVER2.DPRSTAT IN (2, 3) THEN '0'
        ELSE '' END AS ADMISSIONSOURCE,
    --      CASE WHEN INPT_ENC.DISCHARGE_DISPOSITION = 'Expired' THEN 1 ELSE 0 END AS ISALIVEATDISCHARGE,  
    CASE WHEN APTVER2.PTSTAT IN (4, 7) THEN 'PCL01'
                                 WHEN APTVER2.PTSTAT IN (3, 6) THEN 'PCL02'
        WHEN APTVER2.PTSTAT IN (2, 5) THEN 'PCL04'
        ELSE '' END AS POSTCATHLOCATION,
    CASE
        WHEN
            (
                EXTRACT(
                    EPOCH FROM INPT_ENC.HOSPITAL_ADMIT_DATE - COALESCE(
                        ASR.SHTIME, CT.PATIM
                    )
                )
            ) / 3600 < -48 THEN '1'
        ELSE '0'
    END AS ADMITGREATERTHAN48HRSPRIORTOCATH,
    --HEMODYNAMICS           
    CASE
        WHEN
            (
                EXTRACT(
                    EPOCH FROM INPT_ENC.HOSPITAL_DISCHARGE_DATE - POCT.PREND
                )
            ) / 3600 > 48 THEN '1'
        ELSE '0'
    END AS DISCHARGEGREATERTHAN48HRSPOSTCATH,
    COALESCE(ASR.SHTIME, CT.PATIM) AS CATHTIME,
    CASE
        WHEN
            (
                EXTRACT(EPOCH FROM PATIENT.DEATH_DT - POCT.PREND)
            ) / 3600 < 72 THEN '1'
        ELSE '0'
    END AS DEATHLESSTHAN72HRSPOSTCATH,
    CASE WHEN AE2.COMPL = 213 THEN '0'
              WHEN AE2.COMPL < 213 THEN '1'
        ELSE '' END AS DIDADVERSEEVENTOCCUR,
    CASE WHEN PREPCON.IP3245 = 1 THEN '1'
              WHEN PREPCON.IP3245 = 2 THEN '0'
        ELSE '' END AS SINGLEVENTRICLEPHYSIOLOGY,
    CASE WHEN HEMODYN.LVD IS NULL THEN ''
              WHEN HEMODYN.LVD >= 18 THEN '1'
        ELSE '0' END AS SVEDPGREATERTHANOREQUALTO18MMHG,
    CASE WHEN HEMODYN.MVSAT IS NULL THEN ''
              WHEN PREPCON.IP3245 = 2 AND HEMODYN.MVSAT < 60 THEN '1'
        ELSE '0' END AS MVSATLESSTHAN60PERCENT,
    CASE WHEN HEMODYN.MVSAT IS NULL THEN ''
              WHEN HEMODYN.MVSAT < 50 AND PREPCON.IP3245 = 1 THEN '1'
        ELSE '0' END AS MVSATLESSTHAN50PERCENT,
    CASE WHEN HEMODYN.QPQS IS NULL THEN ''
              WHEN HEMODYN.QPQS > 1.5 THEN '1' ELSE '0' END AS QPQSGREATERTHAN1POINT5,
    CASE WHEN PREPCON.IP3245 = 2 AND HEMODYN.SASAT IS NULL THEN ''
              WHEN PREPCON.IP3245 = 2 AND HEMODYN.SASAT < 95 THEN '1'
        WHEN PREPCON.IP3245 = 2 AND HEMODYN.SASAT >= 95 THEN '0'
        ELSE '' END AS SYSSATLESSTHAN95PERCENT,

    --MAJORADVERSEEVENT
    CASE WHEN PREPCON.IP3245 = 1 AND HEMODYN.SASAT IS NULL THEN ''
              WHEN PREPCON.IP3245 = 1 AND HEMODYN.SASAT < 78 THEN '1'
        WHEN PREPCON.IP3245 = 1 AND HEMODYN.SASAT >= 78 THEN '0'
        ELSE '' END AS SYSSATLESSTHAN78PERCENT,
    CASE WHEN PREPCON.IP3245 = 2 AND HEMODYN.MPAS IS NULL THEN ''
              WHEN PREPCON.IP3245 = 2 AND HEMODYN.MPAS >= 45 THEN '1'
        WHEN PREPCON.IP3245 = 2 AND HEMODYN.MPAS < 45 THEN '0'
        ELSE '' END AS PASYSGREATERTHANOREQUALTO45MMHG,
    CASE WHEN HEMODYN.PVR IS NULL THEN ''
        WHEN HEMODYN.PVR > 3 THEN '1' ELSE '0' END AS  PVRGREATERTHAN3WU,
    CASE WHEN HEMODYN.MPAM IS NULL AND PREPCON.IP3245 = 1 THEN ''
        WHEN HEMODYN.MPAM >= 17 AND PREPCON.IP3245 = 1 THEN '1'
        WHEN HEMODYN.MPAM < 17 AND PREPCON.IP3245 = 1 THEN '0'
        ELSE '' END AS PAMEANLESSTHANOREQUALTO17MMHG,
    CASE WHEN IPPOEVNT.IP8000 = 1 THEN '9001'
        WHEN IPPOEVNT.IP8010 = 1 THEN '9002'
        WHEN IPPOEVNT.IP8015 = 1 THEN '9003'
        WHEN IPPOEVNT.IP8020 = 1 THEN '9004'
        WHEN IPPOEVNT.IP8025 = 1 THEN '9005'
        WHEN IPPOEVNT.IP8030 = 1 THEN '9006'
        WHEN IPPOEVNT.IP8035 = 1 THEN '9007'
        WHEN IPPOEVNT.IP8040 = 1 THEN '9008'
        WHEN IPPOEVNT.IP8045 = 1 THEN '9009'
        WHEN IPPOEVNT.IP8050 = 1 THEN '9010'
        WHEN IPPOEVNT.IP8055 = 1 THEN '9011'
        WHEN IPPOEVNT.IP8070 = 1 THEN '9012'
        WHEN IPPOEVNT.IP8075 = 1 THEN '9013'
        WHEN IPPOEVNT.IP8080 = 1 THEN '9014'
        WHEN IPPOEVNT.IP8085 = 1 THEN '9015'
        WHEN IPPOEVNT.IP8090 = 1 THEN '9016'
        WHEN IPPOEVNT.IP8140 = 1 THEN '9017'
        WHEN
            IPPEVEN2.IP8160 = 1 OR IPPEVEN2.IP8165 = 1 OR IPPEVEN2.IP8170 = 1 THEN '9018'
        WHEN IPPEVEN2.IP8175 = 1 THEN '9019'
        WHEN DISCH.DSTATUS = 2 THEN '9020'
        ELSE '' END AS MAJ_AECODE,
    CASE WHEN AE2.AESEV = 1 THEN 'AES01'
        WHEN AE2.AESEV = 2 THEN 'AES02'
        WHEN AE2.AESEV = 3 THEN 'AES03'
        WHEN AE2.AESEV = 4 THEN 'AES04'
        WHEN AE2.AESEV = 5 THEN 'AES05'
        ELSE '' END AS MAJ_AESERIOUSNESS --SELECT *
        FROM CDW_ODS..SENSIS_STUDY STUDY
                                LEFT JOIN PHYS ON STUDY.REFNO = PHYS.REFNO AND PHYS.SEQ = 1
                                LEFT JOIN CDW_ODS..SENSIS_PATIENT PT ON PT.PATNO = STUDY.PATNO
                                LEFT JOIN CDW_ODS..SENSIS_DISCH DISCH ON DISCH.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_PD PD ON PD.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_AE2 AE2 ON AE2.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_CT CT ON CT.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_GENCOND GENCOND ON GENCOND.REFNO = STUDY.REFNO
                                --LEFT JOIN CDW_ODS..SENSIS_HISTB HISTB ON HISTB.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_XRAYBSUM XRAYBSUM ON XRAYBSUM.REFNO = STUDY.REFNO
                                LEFT JOIN (SELECT REFNO, SUM(DOSE) DAP FROM CDW_ODS..SENSIS_XRAYSUM GROUP BY REFNO) XRAYSUM ON XRAYSUM.REFNO = STUDY.REFNO
                                LEFT JOIN (SELECT REFNO, MIN(SHTIME) SHTIME FROM CDW_ODS..SENSIS_ASR GROUP BY REFNO) ASR ON ASR.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_POCT POCT ON POCT.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_HEMODYN HEMODYN ON STUDY.REFNO = HEMODYN.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_PREPCON PREPCON ON STUDY.REFNO = PREPCON.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_APTVER2 APTVER2 ON APTVER2.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_IPPOEVNT IPPOEVNT ON IPPOEVNT.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_IPPEVEN2 IPPEVEN2 ON IPPEVEN2.REFNO = STUDY.REFNO
                                LEFT JOIN (SELECT REFNO, GROUP_CONCAT(NCP, '|') CPONCP FROM
                                              (SELECT DISTINCT REFNO,
                                                      CASE WHEN IP3205 = 1 THEN 'NCP01' END AS NCP
                                                 FROM CDW_ODS..SENSIS_IP2HRSK
                                                  UNION ALL
                                               SELECT DISTINCT REFNO,
                                                      CASE WHEN IP3200 = 1 THEN 'NCP02' END AS NCP
                                                 FROM CDW_ODS..SENSIS_IP2HRSK
                                                  UNION ALL
                                               SELECT DISTINCT REFNO,
                                                      CASE WHEN IP3230 = 1 THEN 'NCP03' END AS NCP
                                                 FROM CDW_ODS..SENSIS_IP2HRSK
                                                  UNION ALL
                                               SELECT DISTINCT REFNO,
                                                      CASE WHEN (IP3220 = 1 or IP3225 = 1 or IP3235 = 1 or IP3240 = 1 or IP3250 = 1)
                                                           THEN 'NCP04' END AS NCP
                                                 FROM CDW_ODS..SENSIS_IP2HRSK
                                                ) CPO
                                                WHERE NCP IS NOT NULL
                                                GROUP BY REFNO
                                            ) CPONCP ON CPONCP.REFNO = STUDY.REFNO
                                LEFT JOIN CDW_ODS..SENSIS_CPOMRCP CPOMRCP ON STUDY.REFNO = CPOMRCP.REFNO AND CPOMRCP.SEQNO = 1
                                LEFT JOIN CDW_ODS..SENSIS_CPOMRSP CPOMRSP ON STUDY.REFNO = CPOMRSP.REFNO AND CPOMRSP.SEQNO = 1
                                LEFT JOIN chop_analytics.CLINICAL.ENCOUNTER_INPATIENT INPT_ENC ON INPT_ENC.CSN = STUDY.ADMISSID
                                LEFT JOIN CDWPRD..PATIENT ON INPT_ENC.PAT_KEY = PATIENT.PAT_KEY
WHERE 1 = 1
    AND DATE(CT.PATIM) BETWEEN '2020-03-01' AND '2020-04-05' --backload
    --    and INPT_ENC.HOSPITAL_DISCHARGE_DATE >= '2019-01-01'
    AND PHYS.DOPER IN (1, 2, 3, 4, 121, 147)
     --and SENSISREFNUM in ('7055402','7223957','7170101','7213707')