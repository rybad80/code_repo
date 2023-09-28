/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.CASELINKNUM)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/
ALTER PROCEDURE "dbo"."sp_CHOP_LoadMissingData" as


DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @CaseLinkNum int
DECLARE @AnestCaseList TABLE (CaseNumber int, CaseLinkNum int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @AnestCaseList(CaseNumber, CaseLinkNum, DBVrsn, RowID)
	SELECT ANEST_SRC.CaseNumber, ANEST_SRC.LOG_ID, C.CDataVrsn, ROW_NUMBER() OVER (ORDER BY ANEST_SRC.CaseNumber) 
	FROM [dbo].[CHOP_STS_CASES] ANEST_SRC INNER JOIN dbo.Cases C ON ANEST_SRC.CaseNumber = C.CaseNumber 
	--where orentryt is null or sistartt is null or sistopt is null or orexitt is null

	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CaseNumber, @CaseLinkNum = CaseLinkNum, @DBVrsn = DBVrsn FROM @AnestCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT APM.CaseNumber FROM Cases APM WHERE APM.CaseNumber = @EventID)  
				BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE ANEST_TRGT
							SET  ANEST_TRGT.CaseLinkNum = ANEST_SRC.LOG_ID 
							     ,ANEST_TRGT.ORENTRYT = ANEST_SRC.IN_ROOM
								 ,ANEST_TRGT.SISTARTT = ANEST_SRC.PROC_START_INCISION
								 ,ANEST_TRGT.SISTOPT = ANEST_SRC.PROC_CLOSE_INCISION
								 ,ANEST_TRGT.OREXITT = ANEST_SRC.OUT_ROOM -- select *
								 ,ANEST_TRGT.MultiDay = ANEST_SRC.MultiDay
								 ,ANEST_TRGT.PERFUSIONIST = ANEST_SRC.PERFUSIONIST
							FROM Cases ANEST_TRGT INNER JOIN @AnestCaseList AC ON ANEST_TRGT.CaseNumber = AC.CaseNumber  
							INNER JOIN [dbo].[CHOP_STS_CASES] ANEST_SRC ON AC.CaseNumber = ANEST_SRC.CaseNumber  
							WHERE AC.RowID = @i
							
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
				END
	      SET @i = @i + 1
        END

