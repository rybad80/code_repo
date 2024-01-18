/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE TRIGGER TR_CHOP_IMPACT_CATHASDDEFECT ON CHOP_IMPACT_CATHASDDEFECT
AFTER UPDATE, INSERT

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int, @Sort int
DECLARE @CathCaseList TABLE (ASDClosureID int, EMREventID int, Sort int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(ASDClosureID, EMREventID, Sort, DBVrsn, RowID)
	SELECT tgt1.ASDClosureID, CATH_SRC.SURG_ENC_ID, CATH_SRC.Sort, h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY c.CathID) 
	FROM [dbo].[CHOP_IMPACT_CATHASDDEFECT] CATH_SRC   INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                      INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
														  left join CATHASDCLOSURE tgt1 on c.cathid = tgt1.cathid
														  left join CATHASDDEFECT tgt2 on tgt1.ASDClosureID = tgt2.ASDClosureID and cath_src.sort = tgt2.sort
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	--WHERE SURG_ENC_ID = '2058849208'
	ORDER BY c.CathID

		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
					SELECT @EventID = ASDClosureID, @DBVrsn = DBVrsn, @EMREventID = EMREventID, @Sort = Sort FROM @CathCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT ASD.ASDClosureID FROM CATHASDDEFECT ASD WHERE ASD.ASDClosureID = @EventID and asd.Sort = @Sort)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET 
							CATH_TRGT.ASDSIZE=CATH_SRC.ASDSIZE,
							CATH_TRGT.ASDBALLSIZPERF=CATH_SRC.ASDBALLSIZPERF,
							CATH_TRGT.ASDSTRETCHDIAMETER=CATH_SRC.ASDSTRETCHDIAMETER,
							CATH_TRGT.ASDSTRETCHDIAMETERSIZE=CATH_SRC.ASDSTRETCHDIAMETERSIZE,
							CATH_TRGT.ASDSTOPFLOWTECH=CATH_SRC.ASDSTOPFLOWTECH,
							CATH_TRGT.ASDSTOPFLOWTECHSIZE=CATH_SRC.ASDSTOPFLOWTECHSIZE,
							CATH_TRGT.ASDRIMMEAS=CATH_SRC.ASDRIMMEAS,
							CATH_TRGT.ASDIVCRIMLENGTH=CATH_SRC.ASDIVCRIMLENGTH,
							CATH_TRGT.ASDAORTRIMLENGTH=CATH_SRC.ASDAORTRIMLENGTH,
							CATH_TRGT.ASDRESSHUNT=CATH_SRC.ASDRESSHUNT,
							CATH_TRGT.SORT=CATH_SRC.SORT,
							CATH_TRGT.ASDPOSTRIMLENGTH=CATH_SRC.ASDPOSTRIMLENGTH,
							CATH_TRGT.ASDMULTIFENESTRATED=CATH_SRC.ASDMULTIFENESTRATED
							FROM CATHASDDEFECT CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.ASDClosureID = AC.ASDClosureID AND CATH_TRGT.SORT = AC.SORT  
							INNER JOIN [dbo].[CHOP_IMPACT_CATHASDDEFECT] CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID  and CATH_SRC.SORT = AC.SORT  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHASDDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHASDDEFECT',@EventID,4,@DBVrsn;
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
								FROM [dbo].[CHOP_IMPACT_CATHASDDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID   and CATH_SRC.SORT = AC.SORT  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.ASDClosureID FROM CATHASDCLOSURE C INNER JOIN @CathCaseList AC ON C.ASDClosureID = AC.ASDClosureID WHERE AC.RowID = @i )
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CATHASDDEFECT (ASDSize,ASDBallSizPerf,ASDStretchDiameter,ASDStretchDiameterSize,ASDStopFlowTech,ASDStopFlowTechSize,ASDRimMeas,ASDIVCRimLength,ASDAortRimLength,ASDResShunt,Sort,ASDClosureID,ASDPostRimLength,ASDMultiFenestrated) 
						SELECT ASDSize,ASDBallSizPerf,ASDStretchDiameter,ASDStretchDiameterSize,ASDStopFlowTech,ASDStopFlowTechSize,ASDRimMeas,ASDIVCRimLength,ASDAortRimLength,ASDResShunt,AC.Sort,AC.ASDClosureID,ASDPostRimLength,ASDMultiFenestrated
					FROM [dbo].[CHOP_IMPACT_CATHASDDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID and CATH_SRC.SORT = AC.SORT 
						WHERE AC.RowID = @i

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHASDDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT 
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHASDDEFECT',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHASDDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID and CATH_SRC.SORT = AC.SORT 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHASDDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT 
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END