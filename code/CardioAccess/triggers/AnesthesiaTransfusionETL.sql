/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.CASELINKNUM)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/
CREATE TRIGGER tr_CHOP_CCAS_ANESTHESIA_TRANSFUSION ON CHOP_CCAS_ANESTHESIA_TRANSFUSION
AFTER UPDATE, INSERT  as

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @AnesthesiaFK int
DECLARE @AnestCaseList TABLE (CaseNumber int, CaseLinkNum int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @AnestCaseList(CaseNumber, CaselinkNum, DBVrsn, RowID)
	SELECT CaseNumber, ANEST_SRC.CASELINKNUM, C.CDataVrsn, ROW_NUMBER() OVER (ORDER BY CaseNumber) 
	FROM [dbo].[CHOP_CCAS_ANESTHESIA_TRANSFUSION] ANEST_SRC INNER JOIN dbo.Cases C ON ANEST_SRC.CaseLinkNum = C.CaseLinkNum 
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	ORDER BY CaseNumber
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CaseNumber, @DBVrsn = DBVrsn, @AnesthesiaFK = CaseLinkNum FROM @AnestCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT APM.CaseNumber FROM Transfusion APM WHERE APM.CaseNumber = @EventID)  
				BEGIN
					BEGIN TRY
					  BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE ANEST_TRGT
							SET ANEST_TRGT.AutologousTrans=ANEST_SRC.AutologousTrans,
							    ANEST_TRGT.CELLSAVSAL=ANEST_SRC.CELLSAVSAL,
								ANEST_TRGT.TRANSFUSION=ANEST_SRC.TRANSFUSION,
								ANEST_TRGT.TRANSFUSBLDPRODANY=ANEST_SRC.TRANSFUSBLDPRODANY,
								ANEST_TRGT.BLDPRODPRBCDUR=ANEST_SRC.BLDPRODPRBCDUR,
								ANEST_TRGT.BLDPRODFFPDUR=ANEST_SRC.BLDPRODFFPDUR,
								ANEST_TRGT.BLDPRODFRESHPDUR=ANEST_SRC.BLDPRODFRESHPDUR,
                                ANEST_TRGT.BLDPRODSNGLPLATDUR=ANEST_SRC.BLDPRODSNGLPLATDUR,
								ANEST_TRGT.BLDPRODINDPLATDUR=ANEST_SRC.BLDPRODINDPLATDUR,
								ANEST_TRGT.BLDPRODCRYODUR=ANEST_SRC.BLDPRODCRYODUR,
								ANEST_TRGT.BLDPRODFRESHWBDUR=ANEST_SRC.BLDPRODFRESHWBDUR,
                                ANEST_TRGT.BLDPRODWBDUR=ANEST_SRC.BLDPRODWBDUR, 
								LastUpdate = GetDate(), 
								UpdatedBy = 'CHOP_AUTOMATION'
							FROM Transfusion ANEST_TRGT INNER JOIN @AnestCaseList AC ON ANEST_TRGT.CaseNumber = AC.CaseNumber  
							INNER JOIN [dbo].[CHOP_CCAS_ANESTHESIA_TRANSFUSION] ANEST_SRC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 0
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_TRANSFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'Transfusion',@EventID,1,@DBVrsn;
					  END
					END	TRY
					--IF ERROR IN TRANSACTION, PROVIDE ROLLBACK PROCEDURE
					BEGIN CATCH
						--PRINT 'BEGIN CATCH'
						IF @@TRANCOUNT > 0
							BEGIN
								--PRINT 'BEGIN ROLLBACK'
								
								ROLLBACK TRAN
								
								--PRINT 'BEGIN ERROR UPDATE'
								
								UPDATE ANEST_SRC 
								SET ANEST_SRC.PendingImport = 3
								FROM [dbo].[CHOP_CCAS_ANESTHESIA_TRANSFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.CaseNumber FROM Cases C INNER JOIN @AnestCaseList AC ON C.CaseNumber = AC.CaseNumber WHERE AC.RowID = @i)
				
				BEGIN TRY
				   BEGIN
					BEGIN TRAN
						INSERT INTO dbo.Transfusion (Casenumber,AutologousTrans,TRANSFUSBLDPRODANY,CELLSAVSAL,TRANSFUSION,BLDPRODPRBCDUR,BLDPRODFFPDUR,BLDPRODFRESHPDUR,BLDPRODSNGLPLATDUR,BLDPRODINDPLATDUR,BLDPRODCRYODUR,
						BLDPRODFRESHWBDUR,BLDPRODWBDUR,CreateDate,LastUpdate,UpdatedBy)
						SELECT AC.CaseNumber,AutologousTrans,TRANSFUSBLDPRODANY,CELLSAVSAL,TRANSFUSION,BLDPRODPRBCDUR,BLDPRODFFPDUR,BLDPRODFRESHPDUR,BLDPRODSNGLPLATDUR,BLDPRODINDPLATDUR,BLDPRODCRYODUR,BLDPRODFRESHWBDUR,
						BLDPRODWBDUR, GetDate(), GetDate(), 'CHOP_AUTOMATION' 
						FROM [dbo].[CHOP_CCAS_ANESTHESIA_TRANSFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM 
						WHERE AC.RowID = @i

						UPDATE ANEST_SRC 
						SET ANEST_SRC.PendingImport = 0
						FROM [dbo].[CHOP_CCAS_ANESTHESIA_TRANSFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'Transfusion',@EventID,1,@DBVrsn;
                  END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 4
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_TRANSFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE ANEST_SRC 
					SET ANEST_SRC.PendingImport = 2
					FROM [dbo].[CHOP_CCAS_ANESTHESIA_TRANSFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END