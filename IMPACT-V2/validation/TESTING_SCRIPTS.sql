---------------------------------
--CHECK FOR CASE IN PROD THAT WE'LL BE USING TO VALIDATE AGAINST
SELECT P_SRC.CathID, pcath.cathid, cath.cathid, c.cathid, pcath.PROCSTARTDATE, PDEMO.PATID, pdemo.MEDRECN, PDEMO.PatFName, pdemo.PATLNAME, pcath.aux5,  pcath.EMREventID 
FROM PSQLA012.Centripetus.[dbo].CathPulmonaryValvuloplasty P_SRC JOIN PSQLA012.Centripetus.[dbo].CathData Pcath ON p_SRC.CathID = PCATH.CathID	
														   LEFT JOIN PSQLA012.Centripetus.[dbo].Demographics Pdemo ON Pdemo.patid = PCATH.patid		
														   left join Demographics demo ON demo.medrecn = pdemo.medrecn													   
														   LEFT join Cathdata CATH on demo.patid = cath.patid and CATH.ProcStartDate = pcath.ProcStartDate
														   left join CathPulmonaryValvuloplasty c on CATH.cathid = c.cathid
where pCATH.AUX5 = '19-0501'
order by pcath.cathid


--GET THE MAIN INFO FROM THE PROD TABLES
SELECT CATHID, PROCSTARTDATE, HOSPID, ADMITDT, PDEMO.PATID, MEDRECN, PDEMO.PatFName, PATLNAME, aux5,  EMREventID 
FROM PSQLA012.Centripetus.[dbo].cathdata PCATH LEFT JOIN PSQLA012.Centripetus.[dbo].Hospitalization PHOSP ON PCATH.HospID = PHOSP.HospitalizationID
                                               LEFT JOIN PSQLA012.Centripetus.[dbo].DEMOGRAPHICS PDEMO ON PDEMO.PATID = PCATH.PatID 
where aux5 = '19-0501' 

--GET THE MAIN INFO FROM THE TEST TABLES
SELECT CATHID, HOSPID, ADMITDT, DEMO.PATID, MEDRECN, aux5,  EMREventID 
FROM cathdata CATH LEFT JOIN Hospitalization HOSP ON CATH.HospID = HOSP.HospitalizationID
                                               LEFT JOIN DEMOGRAPHICS DEMO ON DEMO.PATID = CATH.PatID 
where  CATH.AUX5 = '19-0124'
========================
--parent tables

DECLARE @CATHCASEID VARCHAR(255)
SET @CATHCASEID = '19-0501'

SELECT EMREVENTID, CATHID FROM CATHDATA WHERE AUX5 = @CATHCASEID
SELECT SURG_ENC_ID FROM CHOP_IMPACT_CATHDATA WHERE CATH_CASE_ID = @CATHCASEID

SELECT * FROM CHOP_IMPACT_CathPulmonaryValvuloplasty WHERE SURG_ENC_ID = 2060705494


SELECT 'PROD',B.*
FROM PSQLA012.Centripetus.[dbo].[CathData] prd_CATHDATA JOIN PSQLA012.Centripetus.[dbo].CathPulmonaryValvuloplasty b on prd_CATHDATA.cathid = b.cathid                                                        
WHERE AUX5 = @CATHCASEID

SELECT 'TEST',B.*
FROM [CathData] tst_CATHDATA JOIN CathPulmonaryValvuloplasty b on tst_CATHDATA.cathid = b.cathid    
WHERE AUX5 = @CATHCASEID

select * from CathDeviceMaster_LU

--child tables
---------------------
---------------------

DECLARE @CATHCASEID VARCHAR(255)
SET @CATHCASEID = '19-0501'


SELECT DISTINCT C.EMREventID, C.CATHID FROM CATHDATA C  left JOIN CATHDEVICESUSED A ON A.CATHID = C.CathID
                                  LEFT JOIN CATHDEVICEASSN B ON A.DeviceID = B.DeviceID 
WHERE AUX5 = @CATHCASEID

SELECT AUX5, SURG_ENC_ID FROM CHOP_IMPACT_CATHDATA WHERE CATH_CASE_ID = @CATHCASEID

SELECT * FROM CHOP_IMPACT_CATHPASTENT WHERE SURG_ENC_ID = 2061379399
SELECT * FROM CHOP_IMPACT_CATHPASTENTDEFECT WHERE SURG_ENC_ID = 2061379399

SELECT 'PROD',A.*,B.*
FROM PSQLA012.Centripetus.[dbo].[CathData] prd_CATHDATA LEFT JOIN PSQLA012.Centripetus.[dbo].CATHPASTENT A on prd_CATHDATA.cathid = A.cathid  
                                                        LEFT JOIN PSQLA012.Centripetus.[dbo].CATHPASTENTdefect b on B.pastentid = A.pastentid                                                
WHERE AUX5 = @CATHCASEID

SELECT 'TEST',A.*,B.*
FROM CATHDATA LEFT JOIN CATHPASTENT A on CATHDATA.cathid = A.cathid  
              LEFT JOIN CATHPASTENTdefect b on B.pastentid = A.pastentid                                                
WHERE AUX5 = @CATHCASEID

------------------
-----------------

SELECT MEDRECN, PROCSTARTDATE, ADMITDT, EMREVENTID
FROM [CathData] CATH JOIN DEMOGRAPHICS D ON CATH.PATID = D.PATID
                     JOIN HOSPITALIZATION H ON H.HOSPITALIZATIONID = CATH.HOSPID
WHERE  cath.Aux5= '19-0678'

select * From CATHPASTENt
select * From CATHPASTENTdefect
select * from chop_impact_CATHPASTENTdefect WHERE surg_enc_id = '2058689001'