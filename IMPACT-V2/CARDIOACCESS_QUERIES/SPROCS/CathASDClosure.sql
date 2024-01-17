/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE PROCEDURE sp_CHOP_IMPACT_CATHASDCLOSURE 

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int
DECLARE @CathCaseList TABLE (CathID int, EMREventID int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(CathID, EMREventID, DBVrsn, RowID)
	SELECT CathID, CATH_SRC.SURG_ENC_ID, h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY CathID) 
	FROM [dbo].[CHOP_IMPACT_CATHASDCLOSURE] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                 INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	--and SURG_ENC_ID = '2059350625'
	ORDER BY CathID
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CathID, @DBVrsn = DBVrsn, @EMREventID = EMREventID FROM @CathCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT ASD.CathID FROM CathASDClosure ASD WHERE ASD.CathID = @EventID)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET CATH_TRGT.CathID=AC.CathID,
							    CATH_TRGT.ASDProcInd=CATH_SRC.ASDProcInd,
							    CATH_TRGT.ASDSeptLength=CATH_SRC.ASDSeptLength,
								CATH_TRGT.ASDAneurysm=CATH_SRC.ASDAneurysm, 
								CATH_TRGT.ASDMultiFen=CATH_SRC.ASDMultiFen, 
								CATH_TRGT.ASDSeptLengthNA=CATH_SRC.ASDSeptLengthNA
			
							FROM CathASDClosure CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.CathID = AC.CathID  
							INNER JOIN [dbo].[CHOP_IMPACT_CATHASDCLOSURE] CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHASDCLOSURE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CathASDClosure',@EventID,4,@DBVrsn;
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
								FROM [dbo].[CHOP_IMPACT_CATHASDCLOSURE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.CathID FROM CathData C INNER JOIN @CathCaseList AC ON C.CathID = AC.CathID WHERE AC.RowID = @i)
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CathASDClosure (CathID, ASDProcInd, ASDSeptLength, ASDAneurysm, ASDMultiFen, ASDSeptLengthNA) 
						SELECT AC.CathID, ASDProcInd, ASDSeptLength, ASDAneurysm, ASDMultiFen, ASDSeptLengthNA 
						FROM [dbo].[CHOP_IMPACT_CATHASDCLOSURE] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID 
						WHERE AC.RowID = @i

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHASDCLOSURE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CathASDClosure',@EventID,4,@DBVrsn;
					EXEC Validation_Call_ByTableEventID 'CathASDDefect' ,@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHASDCLOSURE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHASDCLOSURE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
