/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE PROCEDURE sp_CHOP_IMPACT_CATHARRHYTHMIAHISTORY 

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @HospitalizationID int, @EventID int, @DBVrsn varchar(5), @EMREventID int
DECLARE @CathCaseList TABLE (HospitalizationID int, EMREventID int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(HospitalizationID, EMREventID, DBVrsn, RowID) 
	SELECT HospitalizationID, CATH_SRC.SURG_ENC_ID, h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY HospitalizationID) 
	FROM [dbo].CHOP_IMPACT_CATHARRHYTHMIAHISTORY CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                 INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	--WHERE CATH_SRC.SURG_ENC_ID= '2059350625'
	ORDER BY HospitalizationID
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = HospitalizationID, @DBVrsn = DBVrsn, @EMREventID = EMREventID FROM @CathCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT ASD.HospitalizationID FROM CathArrhythmiaHistory ASD WHERE ASD.HospitalizationID = @EventID)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET CATH_TRGT.ARRHYTHMIAHX=CATH_SRC.ARRHYTHMIAHX,
							    CATH_TRGT.ARRHYTHMIAHXTERM=CATH_SRC.ARRHYTHMIAHXTERM,
								CATH_TRGT.LastUpdate= GetDate(),
								CATH_TRGT.UpdatedBy= 'CHOP_AUTOMATION' 				
			
							FROM CathArrhythmiaHistory CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.HospitalizationID = AC.HospitalizationID  
							INNER JOIN [dbo].CHOP_IMPACT_CATHARRHYTHMIAHISTORY CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].CHOP_IMPACT_CATHARRHYTHMIAHISTORY CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CathArrhythmiaHistory',@EventID,4,@DBVrsn;
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
								
								UPDATE CATH_SRC 
								SET CATH_SRC.PendingImport = 3
								FROM [dbo].CHOP_IMPACT_CATHARRHYTHMIAHISTORY CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.HospitalizationID FROM Hospitalization C INNER JOIN @CathCaseList AC ON C.HospitalizationID = AC.HospitalizationID WHERE AC.RowID = @i)
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CathArrhythmiaHistory (HospitalizationID, ARRHYTHMIAHX, ARRHYTHMIAHXTERM, CreateDate, LastUpdate, UpdatedBy) 
						SELECT AC.HospitalizationID, ARRHYTHMIAHX, ARRHYTHMIAHXTERM, GetDate(), GetDate(), 'CHOP_AUTOMATION' 
						FROM [dbo].CHOP_IMPACT_CATHARRHYTHMIAHISTORY CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
                                                                              INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID 
																			  INNER JOIN @CathCaseList AC ON AC.HospitalizationID = H.HospitalizationID 
						WHERE AC.RowID = @i

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].CHOP_IMPACT_CATHARRHYTHMIAHISTORY CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
                                                                              INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID 
																			  INNER JOIN @CathCaseList AC ON AC.HospitalizationID = H.HospitalizationID  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CathArrhythmiaHistory',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].CHOP_IMPACT_CATHARRHYTHMIAHISTORY CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].CHOP_IMPACT_CATHARRHYTHMIAHISTORY CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
