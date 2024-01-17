

create procedure [dbo].[sp_CHOP_IMPACT_CATHDIAGNOSIS]

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int, @Sort int, @DiagID int
DECLARE @CathCaseList TABLE (CathID int, DiagID int, EMREventID int, Sort int , DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(CathID, EMREventID, DiagID, Sort, DBVrsn, RowID)
	SELECT C.CathID, CATH_SRC.SURG_ENC_ID, CATH_SRC.PREPROCCARDDIAGID, CATH_SRC.Sort, h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY C.CathID) 
	FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
													LEFT JOIN CATHDIAGNOSIS CATH_TGT ON C.CathID = CATH_TGT.CathID AND CATH_SRC.PREPROCCARDDIAGID = CATH_TGT.PREPROCCARDDIAGID
    WHERE PendingImport IN (1,2)   --This is to test an individual record
	AND CATH_TGT.PREPROCCARDDIAGID IS NULL
	ORDER BY C.CathID 

		
	SELECT @numrows = @@RowCount, @i = 1 
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CathID, @DBVrsn = DBVrsn, @DiagID = DiagID,@Sort = Sort, @EMREventID = EMREventID FROM @CathCaseList AC WHERE AC.RowID = @i 
			
			IF EXISTS(SELECT ASD.CathID FROM CATHDIAGNOSIS ASD WHERE ASD.CathID = @EventID and ASD.PreProcCardDiagID = @DiagID)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET 
								CATH_TRGT.PREPROCCARDDIAGID = CATH_SRC.PREPROCCARDDIAGID,
								CATH_TRGT.DIAGNOSISNAME = DX_LU.DIAGNOSISNAME,
								CATH_TRGT.SORT = CATH_SRC.SORT
							FROM CATHDIAGNOSIS CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.CathID = AC.CathID  AND AC.Sort = CATH_TRGT.sort
							INNER JOIN [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID AND AC.Sort = CATH_SRC.sort
							INNER JOIN [dbo].[CathDiagnosisMaster_LU] DX_LU ON DX_LU.DIAGNOSISID = CATH_SRC.PREPROCCARDDIAGID  
							WHERE AC.RowID = @i and AC.Sort = CATH_SRC.sort

							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
							WHERE AC.RowID = @i and AC.Sort = CATH_SRC.sort
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHDIAGNOSIS',@EventID,4,@DBVrsn;
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
								FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
								WHERE AC.RowID = @i and AC.Sort = CATH_SRC.sort
								
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
						INSERT INTO dbo.CATHDIAGNOSIS (CathID,PreProcCardDiagID,DiagnosisName, Sort) 
						SELECT AC.CathID,CATH_SRC.PreProcCardDiagID, DX_LU.DiagnosisName, CATH_SRC.SORT
						FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID 
						                                                INNER JOIN [dbo].[CathDiagnosisMaster_LU] DX_LU ON DX_LU.DIAGNOSISID = CATH_SRC.PreProcCardDiagID  
						WHERE AC.RowID = @i AND AC.DiagID = CATH_SRC.PREPROCCARDDIAGID

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHDIAGNOSIS',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID 
							WHERE AC.RowID = @i and AC.Sort = CATH_SRC.Sort
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
					WHERE AC.RowID = @i and AC.Sort = CATH_SRC.Sort
					 
				END

			SET @i = @i + 1
		END
