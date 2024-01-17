WITH TMP AS 
    (SELECT  
          OR_LOG.LOG_KEY,  
         RECORDED_DATE,--FLOW_MEA.FS_KEY, FLOW_MEA.MEAS_VAL
         CAST(((MEAS_VAL_NUM)-32)*(5/9.0) AS NUMERIC(4,1)) TMP,
         ROW_NUMBER() OVER (PARTITION BY OR_LOG.LOG_KEY ORDER BY TMP) TMP_ORDER
--select *
    FROM OR_LOG 
        JOIN ANESTHESIA_ENCOUNTER_LINK AEL ON OR_LOG.LOG_KEY = AEL.OR_LOG_KEY 
        JOIN VISIT_STAY_INFO VSI ON AEL.ANES_VISIT_KEY = VSI.VISIT_KEY 
        JOIN CHOP_ANALYTICS..CARDIAC_PERFUSION_FLOWSHEET FLOWSHEET_ALL ON FLOWSHEET_ALL.VSI_KEY = VSI.VSI_KEY
        JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_SURGERY PERF ON PERF.LOG_KEY = AEL.OR_LOG_KEY 
        JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_BYPASS BYPASS ON PERF.ANES_VISIT_KEY = BYPASS.VISIT_KEY 
                    AND (RECORDED_DATE BETWEEN BYPASS_START_DATE_1 AND BYPASS_STOP_DATE_1 or
                         RECORDED_DATE BETWEEN BYPASS_START_DATE_2 AND BYPASS_STOP_DATE_2 or
                         RECORDED_DATE BETWEEN BYPASS_START_DATE_3 AND BYPASS_STOP_DATE_3 or
                         RECORDED_DATE BETWEEN BYPASS_START_DATE_4 AND BYPASS_STOP_DATE_4 or
                         RECORDED_DATE BETWEEN BYPASS_START_DATE_5 AND BYPASS_STOP_DATE_5 or
                         RECORDED_DATE BETWEEN BYPASS_START_DATE_6 AND BYPASS_STOP_DATE_6 
                         )
    WHERE 
    FLOWSHEET_ID = 7727
    AND MEAS_VAL_NUM > 60.0 
    ) 
,EVENT_TIMES AS
(SELECT LOG_KEY,
        COALESCE(EXTRACT(EPOCH FROM COOL_STOP-COOL_START)/60,0) COOLTIMEPRIOR,
        CPERF_COUNT,
        ULTRAFIL_IND,
        MUF_IND,
        COALESCE(EXTRACT(EPOCH FROM ULTRAFIL_STOP-ULTRAFIL_START)/60,0) ULTRAFILTIME,
        COALESCE(EXTRACT(EPOCH FROM MUF_STOP-MUF_START)/60,0) MUFTIME,
        COALESCE(EXTRACT(EPOCH FROM INDFIB_STOP-INDFIB_START)/60,0) INDFIBTIME
FROM        
    (
    SELECT OR_CASE.LOG_KEY,
           MIN(CASE WHEN EVTTYPE.EVENT_ID = 112700005 THEN EVENT_DT END) AS COOL_START,
           MAX(CASE WHEN EVTTYPE.EVENT_ID = 112700006 THEN EVENT_DT END) AS COOL_STOP,
           SUM(CASE WHEN EVTTYPE.EVENT_ID = 112700054 THEN 1 ELSE NULL END) AS CPERF_COUNT,
           MIN(CASE WHEN EVTTYPE.EVENT_ID = 112700017 THEN EVENT_DT END) AS ULTRAFIL_START,
           MAX(CASE WHEN EVTTYPE.EVENT_ID = 112700017 THEN 1 ELSE NULL END) AS ULTRAFIL_IND,
           MAX(CASE WHEN EVTTYPE.EVENT_ID = 112700018 THEN EVENT_DT END) AS ULTRAFIL_STOP,
           MIN(CASE WHEN EVTTYPE.EVENT_ID = 112700011 THEN EVENT_DT END) AS MUF_START,
           MAX(CASE WHEN EVTTYPE.EVENT_ID = 112700011 THEN 1 ELSE NULL END) AS MUF_IND,
           MAX(CASE WHEN EVTTYPE.EVENT_ID = 112700012 THEN EVENT_DT END) AS MUF_STOP,
           MIN(CASE WHEN EVTTYPE.EVENT_ID = 112700009 THEN EVENT_DT END) AS INDFIB_START,
           MAX(CASE WHEN EVTTYPE.EVENT_ID = 112700010 THEN EVENT_DT END) AS INDFIB_STOP
    FROM   OR_CASE JOIN OR_LOG ON OR_CASE.LOG_KEY = OR_LOG.LOG_KEY
                              JOIN ANESTHESIA_ENCOUNTER_LINK ANESLINK ON ANESLINK.OR_CASE_KEY = OR_CASE.OR_CASE_KEY       
                              JOIN VISIT_ED_EVENT EVT ON EVT.VISIT_KEY = ANESLINK.ANES_VISIT_KEY 
                              JOIN MASTER_EVENT_TYPE EVTTYPE ON EVT.EVENT_TYPE_KEY = EVTTYPE.EVENT_TYPE_KEY
    WHERE EVTTYPE.EVENT_ID IN (112700005,112700006,112700013,112700014,112700054,112700055,112700017,112700018,112700011,112700012,112700009,112700010 )
    GROUP BY OR_CASE.LOG_KEY
    ) A
)
, ART_TEMP AS
(
SELECT  
        OR_LOG.LOG_KEY,  
        CAST(AVG(((MEAS_VAL_NUM)-32)*(5/9.0)) AS INTEGER) AVG_ART_TEMP
    FROM OR_LOG 
        JOIN ANESTHESIA_ENCOUNTER_LINK AEL ON OR_LOG.LOG_KEY = AEL.OR_LOG_KEY 
        JOIN VISIT_STAY_INFO VSI ON AEL.ANES_VISIT_KEY = VSI.VISIT_KEY 
        JOIN CHOP_ANALYTICS..CARDIAC_PERFUSION_FLOWSHEET FLOWSHEET_ALL ON FLOWSHEET_ALL.VSI_KEY = VSI.VSI_KEY
        JOIN EVENT_TIMES ON EVENT_TIMES.LOG_KEY = OR_LOG.LOG_KEY
        JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_SURGERY PERF ON PERF.LOG_KEY = AEL.OR_LOG_KEY 
        JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_CEREBRAL_PERFUSION CPERF ON PERF.ANES_VISIT_KEY = CPERF.VISIT_KEY 
                    AND (RECORDED_DATE BETWEEN cerebral_perfusion_start_date_1 AND cerebral_perfusion_stop_date_1 or
                         RECORDED_DATE BETWEEN cerebral_perfusion_start_date_2 AND cerebral_perfusion_stop_date_2 or
                         RECORDED_DATE BETWEEN cerebral_perfusion_start_date_3 AND cerebral_perfusion_stop_date_3
                         )
    WHERE 
        FLOWSHEET_ID = 112700021
        AND MEAS_VAL_NUM > 60.0 
    GROUP BY OR_LOG.LOG_KEY
)
,CPLEGIA AS 
(
 SELECT PERF.LOG_KEY,
         COUNT(MEDADMIN.DOSE) CPLEGIADOSE
   FROM CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_SURGERY PERF
                   JOIN MEDICATION_ADMINISTRATION MEDADMIN ON  MEDADMIN.VISIT_KEY = PERF.ANES_VISIT_KEY   
                   JOIN MEDICATION_ORDER MEDORD ON MEDORD.MED_ORD_KEY = MEDADMIN.MED_ORD_KEY  
                   LEFT JOIN CDW.CDW_DICTIONARY DICT_RSLT_KEY ON DICT_RSLT_KEY.DICT_KEY = MEDADMIN.DICT_RSLT_KEY 
   WHERE 1=1
          AND MED_ORD_NM = 'cardioplegia soln'
          and DICT_RSLT_KEY.SRC_ID IN (105, 102, 122.0020, 6, 103, 1, 106, 112, 117) --GIVEN MEDS        
 GROUP BY PERF.LOG_KEY
)
, PROTAMINE AS
(
 SELECT 
        PERF.LOG_KEY,
        MIN(MEDADMIN.ACTION_DT) PROTAMINE_TM
   FROM 
        CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_SURGERY PERF
                   JOIN MEDICATION_ADMINISTRATION MEDADMIN ON  MEDADMIN.VISIT_KEY = PERF.ANES_VISIT_KEY   
                   JOIN MEDICATION_ORDER MEDORD ON MEDORD.MED_ORD_KEY = MEDADMIN.MED_ORD_KEY  
                   LEFT JOIN CDW.CDW_DICTIONARY DICT_RSLT_KEY ON DICT_RSLT_KEY.DICT_KEY = MEDADMIN.DICT_RSLT_KEY 
   WHERE 
        upper(MED_ORD_NM) LIKE '%PROTAMINE%'
         and DICT_RSLT_KEY.SRC_ID IN (105, 102, 122.0020, 6, 103, 1, 106, 112, 117) --GIVEN MEDS)
 GROUP BY 
        PERF.LOG_KEY  
)          
,HCT AS
( 
    SELECT PERF.LOG_KEY,
           SPECIMEN_TAKEN_DT RSLT_DT,
           RSLT_NUM_VAL HCT_VALUE,
           FIRST_BYPASS_START_DATE ON_BYPASS_TM,
           FIRST_CIRC_ARREST_START_DATE,
           FIRST_CEREBRAL_PERFUSION_START_DATE CPERF_START,
           LAST_BYPASS_STOP_DATE OFF_BYPASS_TM,
           PROTAMINE_TM,
           PROCORD.PROC_ORD_ID SORT
      FROM  PROCEDURE_ORDER PROCORD 
               JOIN PROCEDURE_ORDER_RESULT RSLT ON PROCORD.PROC_ORD_KEY = RSLT.PROC_ORD_KEY
               JOIN RESULT_COMPONENT COMP ON RSLT.RSLT_COMP_KEY = COMP.RSLT_COMP_KEY
               JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_SURGERY PERF ON PERF.VISIT_KEY = RSLT.VISIT_KEY
               LEFT JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_CIRC_ARREST CIRCARREST ON PERF.ANES_VISIT_KEY = CIRCARREST.VISIT_KEY
               LEFT JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_BYPASS BYPASS ON BYPASS.VISIT_KEY = PERF.ANES_VISIT_KEY
               LEFT JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_CEREBRAL_PERFUSION ACP ON ACP.VISIT_KEY = PERF.ANES_VISIT_KEY
               LEFT JOIN PROTAMINE ON PROTAMINE.LOG_KEY = PERF.LOG_KEY AND PROTAMINE.PROTAMINE_TM > LAST_BYPASS_STOP_DATE
      WHERE RSLT_COMP_ID in (502952,123130039)
        AND RSLT_NUM_VAL <> 9999999    
)
,BLOOD AS
(SELECT
        PRIME.OR_LOG_KEY ,
        MAX(1) AS PRIME,
        SUM(VOL.PRBC_VAL) PRBC,
        SUM(VOL.FFP_VAL) FFP,
        SUM(VOL.WB_VAL) WHOLEBLOOD
   FROM
    (SELECT c.OR_LOG_KEY,
            CASE WHEN FLOWSHEET_ID = 1120090107 THEN RECORDED_DATE END AS PRBC_PRIME_ENTRY_DT,
            CASE WHEN FLOWSHEET_ID = 112700031 THEN RECORDED_DATE END AS FFP_PRIME_ENTRY_DT,
            CASE WHEN FLOWSHEET_ID = 1120090108 THEN RECORDED_DATE END AS WB_PRIME_ENTRY_DT
            FROM     ANESTHESIA_ENCOUNTER_LINK c
            join CDW.VISIT_STAY_INFO vsi on c.ANES_VISIT_KEY = vsi.VISIT_KEY
            JOIN CHOP_ANALYTICS..CARDIAC_PERFUSION_FLOWSHEET FLOWSHEET_ALL ON FLOWSHEET_ALL.VSI_KEY = VSI.VSI_KEY
      WHERE FLOWSHEET_ID IN (1120090107,112700031,1120090108)
        AND MEAS_VAL = 'In Prime'
        --and FLOW_MEA.ENTRY_DT BETWEEN PERF_REC_BEGIN_TM AND PERF_REC_END_TM
    )    PRIME
JOIN 
    (SELECT c.OR_LOG_KEY,
            CASE WHEN FLOWSHEET_ID = 40001452 THEN RECORDED_DATE END AS PRBC_VAL_ENTRY_DT,
            CASE WHEN FLOWSHEET_ID = 40001440 THEN RECORDED_DATE END AS FFP_VAL_ENTRY_DT,
            CASE WHEN FLOWSHEET_ID = 40001456 THEN RECORDED_DATE END AS WB_VAL_ENTRY_DT,
            CASE WHEN FLOWSHEET_ID = 40001452 THEN MEAS_VAL_NUM END AS PRBC_VAL,
            CASE WHEN FLOWSHEET_ID = 40001440 THEN MEAS_VAL_NUM END AS FFP_VAL,
            CASE WHEN FLOWSHEET_ID = 40001456 THEN MEAS_VAL_NUM END AS WB_VAL
            FROM     ANESTHESIA_ENCOUNTER_LINK c
            join CDW.VISIT_STAY_INFO vsi on c.ANES_VISIT_KEY = vsi.VISIT_KEY
            JOIN CHOP_ANALYTICS..CARDIAC_PERFUSION_FLOWSHEET FLOWSHEET_ALL ON FLOWSHEET_ALL.VSI_KEY = VSI.VSI_KEY
      WHERE FLOWSHEET_ID IN (40001452,40001440,40001456)
    ) VOL    
    ON PRIME.OR_LOG_KEY = VOL.OR_LOG_KEY 
       AND (PRBC_PRIME_ENTRY_DT = PRBC_VAL_ENTRY_DT 
            OR FFP_PRIME_ENTRY_DT = FFP_VAL_ENTRY_DT
            OR WB_PRIME_ENTRY_DT = WB_VAL_ENTRY_DT)
    GROUP BY PRIME.OR_LOG_KEY
),
HCTPRIOR as 
(SELECT LOG_KEY,
     HCT_VALUE HCTPRIOR,
     ROW_NUMBER() OVER(PARTITION BY LOG_KEY ORDER BY (COALESCE(FIRST_CIRC_ARREST_START_DATE,CPERF_START)-RSLT_DT), SORT DESC) HCT_PRIOR_ROW
    FROM (SELECT DISTINCT LOG_KEY,
             HCT_VALUE, 
             ON_BYPASS_TM,
             FIRST_CIRC_ARREST_START_DATE,
             CPERF_START,
             RSLT_DT,
             SORT
        FROM HCT ) HCT 
    WHERE RSLT_DT BETWEEN ON_BYPASS_TM AND COALESCE(FIRST_CIRC_ARREST_START_DATE, CPERF_START)
 ), 
HCTFIRST as
(SELECT LOG_KEY,
         HCT_VALUE HCTFIRST,
         ROW_NUMBER() OVER(PARTITION BY LOG_KEY ORDER BY (RSLT_DT-ON_BYPASS_TM), SORT) HCT_FIRST_ROW
    FROM (SELECT DISTINCT LOG_KEY,
                 HCT_VALUE, 
                 ON_BYPASS_TM,
                 OFF_BYPASS_TM,
                 FIRST_CIRC_ARREST_START_DATE,
                 CPERF_START,
                 RSLT_DT,
                 SORT
            FROM HCT ) HCT 
   WHERE RSLT_DT BETWEEN ON_BYPASS_TM AND OFF_BYPASS_TM
), 
HCTLAST as 
(SELECT LOG_KEY,
         HCT_VALUE HCTLAST,
         ROW_NUMBER() OVER(PARTITION BY LOG_KEY ORDER BY (RSLT_DT-OFF_BYPASS_TM) DESC, SORT) HCT_LAST_ROW
    FROM (SELECT DISTINCT LOG_KEY,
                 HCT_VALUE, 
                 ON_BYPASS_TM,
                 OFF_BYPASS_TM,
                 FIRST_CIRC_ARREST_START_DATE,
                 CPERF_START,
                 RSLT_DT,
                 SORT
            FROM HCT ) HCT 
   WHERE RSLT_DT BETWEEN ON_BYPASS_TM AND OFF_BYPASS_TM
), 
HCTPOSTPROT as 
(SELECT LOG_KEY,
         HCT_VALUE HCTPOSTPROT,
         ROW_NUMBER() OVER(PARTITION BY LOG_KEY ORDER BY (RSLT_DT-PROTAMINE_TM), SORT) HCT_POST_PROT_ROW
    FROM HCT 
    WHERE RSLT_DT > OFF_BYPASS_TM AND RSLT_DT > PROTAMINE_TM
) ,


blood_admin as (
select 
        or_log.log_key, 
        surgery.anes_visit_key,
        recorded_date,
        first_bypass_start_date,
        description,
        case when procedure_id = 129642 then 1 else 0 end as prime_ind,
        replace(order_proc.display_name,'Transfusion Order: ','') blood_product_type,
        case when lower(order_proc.display_name) like '%packed%red%cells%' then 'PRBC'
             when lower(order_proc.display_name) like '%platelets%' then 'PLATELETS'
             when lower(order_proc.display_name) like '%fresh%frozen%plasma%' then 'FFP'
             when lower(order_proc.display_name) like '%cryoprecipitate%' then 'CRYO'
             when lower(order_proc.display_name) like '%whole%blood%' then 'WB'
             else BLOOD_PRODUCT_TYPE end as blood_product_category,
             order_proc.display_name,
        blood_start_instant,
        blood_end_instant,
		flowsheet_all.meas_val_num as blood_vol,
        blood_product_code --select distinct proc_id, description
 from 
    chop_analytics..cardiac_perfusion_surgery as surgery
    inner join chop_analytics..cardiac_perfusion_bypass as bypass on bypass.visit_key = surgery.anes_visit_key
    inner join cdwprd..or_log on surgery.log_key = or_log.log_key
    inner join cdwprd..anesthesia_encounter_link on anesthesia_encounter_link.or_log_key = or_log.log_key
    inner join cdwprd..visit as anes_enc on anes_enc.visit_key =  anesthesia_encounter_link.anes_visit_key
    inner join chop_analytics..procedure_order_clinical on procedure_order_clinical.visit_key = anesthesia_encounter_link.visit_key
    inner join cdwprd..visit_addl_info vai on vai.visit_key = anesthesia_encounter_link.visit_key
    inner join cdwprd..visit_stay_info vsi on vsi.vsi_key = vai.vsi_key
    inner join cdwprd..visit_stay_info_rows vsi_rows on vsi_rows.vsi_key = vsi.vsi_key
    inner join cdwprd..visit_stay_info_rows_order vsi_order on vsi_order.vsi_key = vsi_rows.vsi_key
                    					and vsi_rows.seq_num = vsi_order.seq_num
                    					and vsi_order.ord_key = procedure_order_clinical.proc_ord_key
    inner join cdw_ods..ord_blood_admin on procedure_order_clinical.procedure_order_id = ord_blood_admin.order_id   
    inner join cdw_ods..order_proc on ord_blood_admin.order_id = order_proc.order_proc_id
    inner join chop_analytics..flowsheet_all as flowsheet_all on flowsheet_all.vsi_key = vsi_rows.vsi_key 
                                        and vsi_rows.seq_num = flowsheet_all.occurance                                      
where
    proc_id in (129642,500200703,500200704,500200705,500200707,81295)
    and procedure_order_type = 'Child Order'
    and flowsheet_id = 500025331     
) ,

prime_cpb_blood as (

select 
      blood_admin.log_key,
      sum(case when blood_product_category = 'PRBC' then (blood_vol) else 0 end) as prbc,
      sum(case when blood_product_category = 'FFP' then (blood_vol) else 0 end) as ffp,
      sum(case when blood_product_category = 'WB' then (blood_vol) else 0 end) as wholeblood 
from
     blood_admin
where 
     prime_ind = 1
group by 
      blood_admin.log_key
)

SELECT  DISTINCT
       OR_LOG.LOG_ID,
    --   HEIGHT_CM,
    --   WEIGHT_KG,
    --   PERFUSIONIST_NAME,
    --   PERFUSIONIST_EMAIL,
      total_bypass_MINUTES CPBTM,
      coalesce(total_cross_clamp_MINUTES,0)  XCLAMPTM,
      coalesce(total_circ_arrest_MINUTES,0) DHCATM,
      2 TEMPSITEBLA,    
      NULL LOWCTMPBLA,    
      2 TEMPSITEESO,    
      NULL LOWCTMPESO,    
      1 TEMPSITENAS,    
      CAST(TMP.TMP AS NUMERIC(8,2)) LOWCTMPNAS,    
      2 TEMPSITEREC,    
      NULL LOWCTMPREC,    
      2 TEMPSITETYM,
      NULL LOWCTMPTYM,    
      2 TEMPSITEOTH,    
      NULL LOWCTMPOTH,
      coalesce(TOTAL_REWARM_MINUTES,0) REWARMTIME,
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN 1 ELSE 2 END CPERFUTIL,
      total_cerebral_perfusion_MINUTES CPERFTIME,
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN 1 ELSE NULL END AS CPERFCANINN,
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN 2 ELSE NULL END AS CPERFCANRSUB,
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN 2 ELSE NULL END AS CPERFCANRAX,    
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN 2 ELSE NULL END AS CPERFCANRCAR,
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN 2 ELSE NULL END AS CPERFCANLCAR,    
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN 2 ELSE NULL END AS CPERFCANSVC    ,
      CPERF_COUNT CPERFPER,
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN 50 ELSE NULL END AS CPERFFLOW,
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN AVG_ART_TEMP ELSE NULL END AS CPERFTEMP,
      CASE WHEN COOLTIMEPRIOR > 0 THEN 448 ELSE 446 END AS ABLDGASMGT,
      CAST(HCTPRIOR.HCTPRIOR AS NUMERIC(8,2)) HCTPRICIRCA,
      CPLEGIADOSE,
      CASE WHEN CPLEGIADOSE is not null THEN 2845 ELSE NULL END AS CPLEGSOL,
      null INFLWOCCLTM,
      CASE WHEN total_cerebral_perfusion_MINUTES > 0 THEN 1370 ELSE NULL END AS CEREBRALFLOWTYPE,
      COALESCE(BLOOD.PRIME,2) CPBPRIMED,
      CASE WHEN CPLEGIADOSE is not null THEN 2842 ELSE 2841 END AS  CPLEGIADELIV,
      CASE WHEN CPLEGIADOSE is not null THEN 2855 ELSE NULL END AS  CPLEGIATYPE,
      CAST(HCTFIRST.HCTFIRST AS NUMERIC(4,1)) HCTFIRST,
      CAST(HCTLAST.HCTLAST AS NUMERIC(4,1)) HCTLAST,
      CAST(HCTPOSTPROT.HCTPOSTPROT AS NUMERIC(4,1)) HCTPOST,
      COALESCE(blood.PRBC,prime_cpb_blood.prbc,0) PRBC,
      COALESCE(blood.FFP,prime_cpb_blood.FFP,0) FFP,
      COALESCE(blood.WHOLEBLOOD,prime_cpb_blood.WHOLEBLOOD,0) WHOLEBLOOD,      
      CASE WHEN INDFIBTIME > 0 THEN 1 ELSE 2 END AS INDUCEDFIB,
      CASE WHEN INDFIBTIME > 0 THEN INDFIBTIME ELSE NULL END AS INDUCEDFIBTMMIN,
      CASE WHEN INDFIBTIME > 0 THEN 0 ELSE NULL END AS INDUCEDFIBTMSEC,
      COOLTIMEPRIOR,
      CASE WHEN COALESCE(ULTRAFIL_IND,0) > 0 or COALESCE(MUF_IND,0) > 0 THEN 1 ELSE 2 END AS ULTRAFILPERFORM,
      CASE WHEN COALESCE(ULTRAFIL_IND,0) > 0 AND COALESCE(MUF_IND,0) > 0 THEN 5283
           WHEN COALESCE(ULTRAFIL_IND,0) = 0 AND COALESCE(MUF_IND,0) > 0 THEN 5282
           WHEN COALESCE(ULTRAFIL_IND,0) > 0 AND COALESCE(MUF_IND,0) = 0 THEN 5281
           ELSE NULL END AS ULTRAFILPERFWHEN ,
      25 ANTICOAGUSED,
      1 ANTICOAGUNFHEP,
      2 ANTICOAGARG,
      2 ANTICOAGBIVAL,
      2 ANTICOAGOTH,
      cast(PERF.HEIGHT_CM as numeric(5,1)) HEIGHT_CM,
      cast(PERF.WEIGHT_KG as numeric(5,1)) WEIGHT_KG
--SELECT *
FROM CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_SURGERY PERF LEFT JOIN OR_LOG ON PERF.LOG_KEY = OR_LOG.LOG_KEY
                                           LEFT JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_CIRC_ARREST CIRCARREST ON PERF.ANES_VISIT_KEY = CIRCARREST.VISIT_KEY
                                           LEFT JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_CEREBRAL_PERFUSION ACP ON PERF.ANES_VISIT_KEY = ACP.VISIT_KEY
                                           LEFT JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_REWARM REWARM ON PERF.ANES_VISIT_KEY = REWARM.VISIT_KEY
                                           LEFT JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_BYPASS BYPASS ON PERF.ANES_VISIT_KEY = BYPASS.VISIT_KEY
                                           LEFT JOIN CHOP_ANALYTICS.STACKS_CLINICAL.CARDIAC_PERFUSION_CROSS_CLAMP XCLAMP ON PERF.ANES_VISIT_KEY = XCLAMP.VISIT_KEY
                                            LEFT JOIN TMP ON TMP.LOG_KEY = PERF.LOG_KEY AND TMP_ORDER = 1
                                           LEFT JOIN EVENT_TIMES EVTTM ON EVTTM.LOG_KEY = PERF.LOG_KEY
                                           LEFT JOIN BLOOD ON BLOOD.OR_LOG_KEY = PERF.LOG_KEY
                                           LEFT JOIN ART_TEMP ON PERF.LOG_KEY = ART_TEMP.LOG_KEY
                                           LEFT JOIN CPLEGIA ON PERF.LOG_KEY = CPLEGIA.LOG_KEY
                                           LEFT JOIN HCTPRIOR ON HCTPRIOR.LOG_KEY = PERF.LOG_KEY AND HCT_PRIOR_ROW = 1 
                                           LEFT JOIN HCTFIRST ON HCTFIRST.LOG_KEY = PERF.LOG_KEY AND HCT_FIRST_ROW = 1 
                                           LEFT JOIN HCTLAST ON HCTLAST.LOG_KEY = PERF.LOG_KEY AND HCT_LAST_ROW = 1 
                                           LEFT JOIN HCTPOSTPROT ON HCTPOSTPROT.LOG_KEY = PERF.LOG_KEY AND HCT_POST_PROT_ROW = 1 
                                           left join prime_cpb_blood on prime_cpb_blood.log_key = perf.log_key
WHERE FIRST_BYPASS_START_DATE IS NOT NULL
and OR_LOG.LOG_ID > 0