/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.CASELINKNUM)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE TRIGGER tr_CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS ON CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS
AFTER UPDATE, INSERT  as

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @AnesthesiaFK int
DECLARE @AnestCaseList TABLE (CaseNumber int, CaseLinkNum int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @AnestCaseList(CaseNumber, CaselinkNum, DBVrsn, RowID)
	SELECT CaseNumber, ANEST_SRC.CASELINKNUM, C.CDataVrsn, ROW_NUMBER() OVER (ORDER BY CaseNumber) 
	FROM [dbo].[CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS] ANEST_SRC INNER JOIN dbo.Cases C ON ANEST_SRC.CaseLinkNum = C.CaseLinkNum  
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	ORDER BY CaseNumber
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CaseNumber, @DBVrsn = DBVrsn, @AnesthesiaFK = CaseLinkNum FROM @AnestCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT APM.CaseNumber FROM Procoagulants APM WHERE APM.CaseNumber = @EventID)  --CHANGE TABLE NAME
				BEGIN
					BEGIN TRY
					  BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE ANEST_TRGT
							SET ANEST_TRGT.AntifibEpUse=ANEST_SRC.AntifibEpUse,
							    ANEST_TRGT.AntifibEpLoad=ANEST_SRC.AntifibEpLoad,
								ANEST_TRGT.AntifibEpPrime=ANEST_SRC.AntifibEpPrime,
								ANEST_TRGT.AntifibEpInfRate=ANEST_SRC.AntifibEpInfRate,
							    ANEST_TRGT.AntifibTranexUse=ANEST_SRC.AntifibTranexUse,
								ANEST_TRGT.AntifibTranexLoad=ANEST_SRC.AntifibTranexLoad,
								ANEST_TRGT.AntifibTranexPrime=ANEST_SRC.AntifibTranexPrime,
								ANEST_TRGT.AntifibTranexInfRate=ANEST_SRC.AntifibTranexInfRate,
							    ANEST_TRGT.AntifibUsage=ANEST_SRC.AntifibUsage,
								ANEST_TRGT.ANTIFIBTRANEXPRIMEDOSE=ANEST_SRC.ANTIFIBTRANEXPRIMEDOSE,
								ANEST_TRGT.POCCOAGTSTUTIL=ANEST_SRC.POCCOAGTSTUTIL,
								LastUpdate = GetDate(), 
								UpdatedBy = 'CHOP_AUTOMATION'
							FROM Procoagulants ANEST_TRGT INNER JOIN @AnestCaseList AC ON ANEST_TRGT.CaseNumber = AC.CaseNumber  --CHANGE TABLE and COLUMNS NAMES
							INNER JOIN [dbo].[CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS] ANEST_SRC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 0
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum 
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'Procoagulants',@EventID,1,@DBVrsn; --CHANGE TABLE NAME
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
								FROM [dbo].[CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
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
						INSERT INTO dbo.Procoagulants (Casenumber,AntifibUsage,AntifibEpUse,AntifibEpLoad,AntifibEpPrime,AntifibEpInfRate,AntifibTranexUse,AntifibTranexLoad,AntifibTranexPrime,AntifibTranexInfRate,Antifibtranexprimedose,Poccoagtstutil,CreateDate,LastUpdate,UpdatedBy)
						SELECT AC.CaseNumber, ANTIFIBUSAGE,ANTIFIBEPUSE,ANTIFIBEPLOAD,ANTIFIBEPPRIME,ANTIFIBEPINFRATE,ANTIFIBTRANEXUSE,ANTIFIBTRANEXLOAD,ANTIFIBTRANEXPRIME,ANTIFIBTRANEXINFRATE, ANTIFIBTRANEXPRIMEDOSE,POCCOAGTSTUTIL, GetDate(), GetDate(), 'CHOP_AUTOMATION' 
						FROM [dbo].[CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS] ANEST_SRC INNER JOIN @AnestCaseList AC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM 
						WHERE AC.RowID = @i

						UPDATE ANEST_SRC 
						SET ANEST_SRC.PendingImport = 0
						FROM [dbo].[CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'Procoagulants',@EventID,1,@DBVrsn; --CHANGE TABLE NAME
                  END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 4
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE ANEST_SRC 
					SET ANEST_SRC.PendingImport = 2
					FROM [dbo].[CHOP_CCAS_ANESTHESIA_ANTIFIBRINOLYTICS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
