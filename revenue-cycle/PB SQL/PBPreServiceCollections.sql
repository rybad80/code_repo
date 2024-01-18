WITH CHARGES AS (SELECT TDL.TX_ID 
                        ,TDL.PAT_ENC_CSN_ID 
                        ,TDL.PAT_ID 
                        ,TDL.INT_PAT_ID 
                        ,TDL.LOC_ID 
                        ,TDL.DEPT_ID 
                        ,TDL.ORIG_SERVICE_DATE
                        ,SUM(TDL.AMOUNT) AS CHARGE_AMOUNT 
                 FROM CLARITY_TDL_TRAN TDL
                 WHERE TDL.DETAIL_TYPE IN (1,10)
                  AND TDL.LOC_ID IN (1016,1017,1018,1020,1026,1029,1030,1033,1038)
                  --AND TX_ID = 20393504
                 GROUP BY TDL.TX_ID 
                          ,TDL.PAT_ENC_CSN_ID 
                          ,TDL.PAT_ID 
                          ,TDL.INT_PAT_ID 
                          ,TDL.LOC_ID 
                          ,TDL.DEPT_ID 
                          ,TDL.ORIG_SERVICE_DATE
                  ),
PAYMENTS AS (SELECT TDL.TX_ID 
                    , TDL.PAT_ID 
                    , TDL.ACCOUNT_ID
                    , TDL.INT_PAT_ID 
                    , TDL.PAT_ENC_CSN_ID
                    , TDL.ORIG_SERVICE_DATE 
                    , TDL.ORIG_POST_DATE
                    , TDL.POST_DATE 
                    , TDL.LOC_ID
                    , TDL.DEPT_ID 
                    , CASE WHEN EAP.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP.PROC_ID
                           WHEN EAP2.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP2.PROC_ID 
                           ELSE NULL
                      END FINAL_PROC_ID
                    , CASE WHEN EAP.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP.PROC_CODE
                           WHEN EAP2.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP2.PROC_CODE
                           ELSE NULL
                      END FINAL_PROC_CODE
                    , CASE WHEN EAP.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP.PROC_NAME
                           WHEN EAP2.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP2.PROC_NAME 
                           ELSE 'CHECK'
                      END AS FINAL_PROC_NAME   
                 ,SUM(TDL.AMOUNT) AS AMOUNT 
                 ,SUM(TDL.INSURANCE_AMOUNT) AS INSURANCE_PAYMENT
                 ,SUM(TDL.PATIENT_AMOUNT) AS PATIENT_PAYMENT
             FROM CLARITY_TDL_TRAN TDL
             LEFT JOIN CLARITY_EAP EAP ON TDL.PROC_ID = EAP.PROC_ID 
             LEFT JOIN CLARITY_EAP EAP2 ON TDL.MATCH_PROC_ID = EAP2.PROC_ID
             WHERE tdl.detail_type IN( 2, 5, 11, 20, 22, 32, 33  )
             --AND TDL.TX_ID = 20393504
             AND TDL.LOC_ID IN (1016,1017,1018,1020,1026,1029,1030,1033,1038)
             AND (EAP.proc_code IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') 
                                 OR EAP2.PROC_code IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162','1174',
                                    '1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373')
                 )
             AND TDL.POST_DATE BETWEEN add_months(trunc(current_date,'month'),-15) and add_months(last_day(current_date),-1)

             GROUP BY TDL.TX_ID 
                 ,TDL.PAT_ID 
                 ,TDL.ACCOUNT_ID
                 ,TDL.INT_PAT_ID 
                 ,TDL.PAT_ENC_CSN_ID
                 ,TDL.ORIG_SERVICE_DATE 
                 ,TDL.ORIG_POST_DATE
                 ,TDL.POST_DATE
                 ,TDL.LOC_ID
                 ,TDL.DEPT_ID 
                 ,CASE WHEN EAP.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP.PROC_NAME
                       WHEN EAP2.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP2.PROC_NAME 
                                        ELSE 'CHECK'
                                     END
                 ,CASE WHEN EAP.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP.PROC_ID
                       WHEN EAP2.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP2.PROC_ID 
                      ELSE NULL
                 END 
                ,CASE WHEN EAP.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP.PROC_CODE
                      WHEN EAP2.PROC_CODE  IN ('1014','1018','1023','1048','1050','1068','1076','1080','1081','1100','1158','1162',
                                        '1174','1178','1193','1194','1210','1211','1212','1215','1223','1230','1250','1300','1301','1302','1330','1362','1373') THEN EAP2.PROC_CODE
                      ELSE NULL
                  END                     
            )
SELECT DISTINCT PMT.TX_ID
                , PMT.PAT_ID
                , PMT.ACCOUNT_ID
                , PMT.INT_PAT_ID
                , PMT.ORIG_SERVICE_DATE
                , PMT.ORIG_POST_DATE
                , last_day(PMT.POST_DATE) as POST_MONTH
                , PMT.FINAL_PROC_NAME
                , CHG.LOC_ID
                , CHG.DEPT_ID
                , CHG.ORIG_SERVICE_DATE AS DATE_OF_SERVICE 
				, dep.GL_PREFIX as COST_CNTR_ID
                , CASE WHEN PMT.POST_DATE <= CHG.ORIG_SERVICE_DATE THEN 'PrePayment_On_Before_Service_Dt' 
                       WHEN PMT.POST_DATE BETWEEN CHG.ORIG_SERVICE_DATE  AND CHG.ORIG_SERVICE_DATE+10  THEN 'PostPayment_On_After_Service_Dt'
                       ELSE 'Payment_11+_Days_After_Service_Dt'
                  END AS PAYMENT_CATEGORY  --MIGHT NEED THIS COLUMN HELPS DETERMINE WHEN THE PAYMENT WAS RECEIVED BASED ON PAYMENT POST DATE COMPARED TO ENCOUNTER SERVICE DATE
                --, SUM(CASE WHEN PMT.POST_DATE <= CHG.ORIG_SERVICE_DATE THEN PMT.PATIENT_PAYMENT ELSE 0 END) AS NUMERATOR
                , SUM(CASE WHEN PMT.POST_DATE <= CHG.ORIG_SERVICE_DATE THEN PMT.PATIENT_PAYMENT 
           		  WHEN PMT.POST_DATE BETWEEN CHG.ORIG_SERVICE_DATE  AND CHG.ORIG_SERVICE_DATE+10  THEN  PMT.PATIENT_PAYMENT
      			  ELSE 0 END) AS NUMERATOR
                , SUM(PMT.AMOUNT) AS MONTHLY_COLLECTION--DENOMINATOR  
FROM PAYMENTS PMT
LEFT JOIN CHARGES CHG ON PMT.TX_ID = CHG.TX_ID
LEFT JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = CHG.dept_id

GROUP BY PMT.TX_ID
                , PMT.PAT_ID
                , PMT.ACCOUNT_ID
                , PMT.INT_PAT_ID
                , PMT.ORIG_SERVICE_DATE
                , PMT.ORIG_POST_DATE
                , PMT.POST_DATE
                , PMT.FINAL_PROC_NAME
                , CHG.LOC_ID
                , CHG.DEPT_ID
                , CHG.ORIG_SERVICE_DATE 
                , dep.GL_PREFIX
                , CASE WHEN PMT.POST_DATE <= CHG.ORIG_SERVICE_DATE THEN 'PrePayment_On_Before_Service_Dt' 
                       WHEN PMT.POST_DATE BETWEEN CHG.ORIG_SERVICE_DATE  AND CHG.ORIG_SERVICE_DATE+10  THEN 'PostPayment_On_After_Service_Dt'
                       ELSE 'Payment_11+_Days_After_Service_Dt'
                  END
;