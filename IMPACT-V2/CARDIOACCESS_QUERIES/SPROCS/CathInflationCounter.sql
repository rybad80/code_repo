/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE PROCEDURE sp_CHOP_IMPACT_CathInflationCounter 

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int, @Sort int
DECLARE @CathCaseList TABLE (CathID int, EMREventID int, Sort int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(CathID, EMREventID, Sort, DBVrsn, RowID)
	SELECT c.CathID, CATH_SRC.SURG_ENC_ID, CATH_SRC.Sort, h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY c.CathID) 
	FROM [dbo].[CHOP_IMPACT_CathInflationCounter] CATH_SRC   INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                      INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
														  left join CathInflationCounter tgt on tgt.CathID = c.CathID and cath_src.sort = tgt.sort
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	--WHERE c.AUX5 = '19-0700'
	ORDER BY c.CathID

		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
					SELECT @EventID = CathID, @DBVrsn = DBVrsn, @EMREventID = EMREventID, @Sort = Sort FROM @CathCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT ASD.CathID FROM CathInflationCounter ASD WHERE ASD.CathID = @EventID and asd.Sort = @Sort)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET 
							    CATH_TRGT.CathprocID = 2006,
								CATH_TRGT.BALLTECH=CATH_SRC.BALLTECH,
								CATH_TRGT.SINGDEVID=CATH_SRC.SINGDEVID,
								CATH_TRGT.SINGBALLSTAB=CATH_SRC.SINGBALLSTAB,
								CATH_TRGT.SINGBALLPRESSURE=CATH_SRC.SINGBALLPRESSURE,
								CATH_TRGT.SINGBALLOUTCOME=CATH_SRC.SINGBALLOUTCOME,
								CATH_TRGT.SINGBALLPOSTPKSYSGRAD=CATH_SRC.SINGBALLPOSTPKSYSGRAD,
								CATH_TRGT.SINGBALLPOSTINSUFF=CATH_SRC.SINGBALLPOSTINSUFF,
								CATH_TRGT.DOUBDEVID1=CATH_SRC.DOUBDEVID1,
								CATH_TRGT.DOUBBALLSTAB1=CATH_SRC.DOUBBALLSTAB1,
								CATH_TRGT.DOUBBALLPRESSURE1=CATH_SRC.DOUBBALLPRESSURE1,
								CATH_TRGT.DOUBBALLOUTCOME1=CATH_SRC.DOUBBALLOUTCOME1,
								CATH_TRGT.DOUBDEVID2=CATH_SRC.DOUBDEVID2,
								CATH_TRGT.DOUBBALLSTAB2=CATH_SRC.DOUBBALLSTAB2,
								CATH_TRGT.DOUBBALLPRESSURE2=CATH_SRC.DOUBBALLPRESSURE2,
								CATH_TRGT.DOUBBALLOUTCOME2=CATH_SRC.DOUBBALLOUTCOME2,
								CATH_TRGT.DOUBBALLPOSTPKSYSGRAD=CATH_SRC.DOUBBALLPOSTPKSYSGRAD,
								CATH_TRGT.DOUBBALLPOSTINSUFF=CATH_SRC.DOUBBALLPOSTINSUFF,
								CATH_TRGT.SORT=CATH_SRC.SORT
							FROM CathInflationCounter CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.CathID = AC.CathID AND CATH_TRGT.SORT = AC.SORT  
							INNER JOIN [dbo].[CHOP_IMPACT_CathInflationCounter] CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID  and CATH_SRC.SORT = AC.SORT  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CathInflationCounter] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CathInflationCounter',@EventID,4,@DBVrsn;
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
								FROM [dbo].[CHOP_IMPACT_CathInflationCounter] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID   and CATH_SRC.SORT = AC.SORT  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.CathID FROM CathAorticValvuloplasty C INNER JOIN @CathCaseList AC ON C.CathID = AC.CathID WHERE AC.RowID = @i )
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CathInflationCounter (CathID,CathProcID,BallTech,SingDevID,SingBallStab,SingBallPressure,SingBallOutcome,SingBallPostPkSysGrad,SingBallPostInsuff,DoubDevID1,DoubBallStab1,DoubBallPressure1,DoubBallOutcome1,DoubDevID2,DoubBallStab2,DoubBallPressure2,DoubBallOutcome2,DoubBallPostPkSysGrad,DoubBallPostInsuff,Sort,PostDilSysGrad,PostDilRegurg) 
						SELECT ac.CathID,2006,BallTech,SingDevID,SingBallStab,SingBallPressure,SingBallOutcome,SingBallPostPkSysGrad,SingBallPostInsuff,DoubDevID1,DoubBallStab1,DoubBallPressure1,DoubBallOutcome1,DoubDevID2,DoubBallStab2,DoubBallPressure2,DoubBallOutcome2,DoubBallPostPkSysGrad,DoubBallPostInsuff,ac.Sort,PostDilSysGrad,PostDilRegurg
					FROM [dbo].[CHOP_IMPACT_CathInflationCounter] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID and CATH_SRC.SORT = AC.SORT 
						WHERE AC.RowID = @i

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CathInflationCounter] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT 
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CathInflationCounter',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CathInflationCounter] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID and CATH_SRC.SORT = AC.SORT 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CathInflationCounter] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT 
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
