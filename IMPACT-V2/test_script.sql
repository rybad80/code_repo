--find records not yet loaded 
--2058689001	11973
--2058691690	11969

SELECT CATH_SRC.SURG_ENC_ID,
       C.CATHID,
	   CATH_TGT.CathID,
		 procstartdate,
		  CATH_SRC.PendingImport
FROM dbo.CHOP_IMPACT_CATHDIAGNOSIS CATH_SRC left JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
													LEFT join CATHDIAGNOSIS  CATH_TGT ON CATH_TGT.CATHID = C.CATHID
													--where CATH_SRC.SURG_ENC_ID = '2057083414'
order by 1 

--find cathid/emreventid/surgencid mapping
select cathid 
from cathdata join Demographics on cathdata.cathid = demographics.cathid
where medrecn = '00968511'
  and cast(ProcStartDate as date) = '2019-02-07 00:00:00'

select * From chop_impact_CATHDIAGNOSIS where surg_enc_id IN (2058559507)
select * from CATHDIAGNOSIS where CATHID = (select CATHID from cathdata where emreventid = 2058559507) SELECT EMREVENTID FROM CATHDATA WHERE CATHID = 11941
SELECT * FROM CATHDATA WHERE CATHID = 11970

--delete exising record to test new insert
delete from CATHDIAGNOSIS  where ARRID IN (3392,
3393,
3394,
3395,
3396,
3397,
3398) = 11973


--update cathdata emreventid
update chop_impact_CATHDIAGNOSIS set PENDINGIMPORT = 1 where surg_enc_id IN (2058572754,
2058631674,
2058634225,
2058689001)

--lookup in Staging table

select * from CHOP_IMPACT_CATHDIAGNOSIS WHERE SURG_ENC_ID = (SELECT EMREVENTID FROM CATHDATA WHERE CATHID  = 11813) 

--test update to existing record 

--run sproc

select * from CATHDIAGNOSIS where  CATHID  = 11813 --check updated target

--reset import flag
update CHOP_IMPACT_CATHDIAGNOSIS Set PENDINGIMPORT = 1 where SURG_ENC_ID = 2058572754
update CHOP_IMPACT_CATHDIAGNOSIS Set ARRHYTHMIAHX = 4010 where SURG_ENC_ID = 2058689001

--alter staging record
update CHOP_IMPACT_CATHDIAGNOSIS Set COARCPROCIND = 1510 where SURG_ENC_ID = 2058489896

-- delete record

delete from chop_impact_CATHDIAGNOSIS where arrid = 3391
--test insert of new record

--if a record doesn't exist in staging, load a new one by duplicating an existing one and giving it the SURG_ENC_ID of a record in CathData's EMREVENTID
select cathid, emreventid from cathdata where EMREventID is not null

INSERT INTO CHOP_IMPACT_CATHDIAGNOSIS
SELECT  '2058691690',
				'92',
				'0',
				'66',
				'0',
				'124',
				'0',
				'24',
				'0',
				'120',
				'0',
				'60',
				'0',
				'84',
				'0',
				'69',
				'0',
				'46',
				'0',
				'60',
				'0',
				'3',
				'0',
				'3',
				'0',
				'1',
				'0',
				'2019-06-03 10:47:10.000',
				'6952D40100BC7354973297A164F3229B',
				'1'

-- check record was inserted to staging
	SELECT CathID, CATH_SRC.SURG_ENC_ID
	FROM CHOP_IMPACT_CATHDIAGNOSISCATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                           INNER JOIN dbo.Hospitalization H on C.CATHID = H.CATHID
	where emreventid = '2058691690'

--run sproc

--check record was inserted to CA table
select * from CATHDIAGNOSIS c LEFT join CATHDIAGNOSIS  CATH_TGT ON CATH_TGT.CathID = C.CathID where cathid = 11969

update CHOP_IMPACT_CATHDIAGNOSIS Set PENDINGIMPORT = 1 where SURG_ENC_ID = 2058634225

select * from CHOP_IMPACT_CATHDIAGNOSIS where SURG_ENC_ID = '2058691690'