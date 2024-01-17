/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE TRIGGER TR_CHOP_IMPACT_CATHCOARCPROCDEVICE ON CHOP_IMPACT_CATHCOARCPROCDEVICE
AFTER UPDATE, INSERT

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int, @Sort int
DECLARE @CathCaseList TABLE (CoarcProcID int, EMREventID int, Sort int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(CoarcProcID, EMREventID, Sort, DBVrsn, RowID)
	SELECT tgt1.CoarcProcID, CATH_SRC.SURG_ENC_ID, CATH_SRC.Sort, h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY c.CathID) 
	FROM [dbo].[CHOP_IMPACT_CATHCOARCPROCDEVICE] CATH_SRC 
	                                                      INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                      INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
														  left join cathcoarcproc tgt1 on c.cathid = tgt1.cathid
														  left join cathcoarcprocdevice tgt2 on tgt1.coarcprocid = tgt2.coarcprocid and cath_src.sort = tgt2.sort
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	--WHERE SURG_ENC_ID = '2058689001'
	ORDER BY c.CathID

		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
					SELECT @EventID = CoarcProcID, @DBVrsn = DBVrsn, @EMREventID = EMREventID, @Sort = Sort FROM @CathCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT ASD.CoarcProcID FROM CATHCOARCPROCDEVICE ASD WHERE ASD.CoarcProcID = @EventID and asd.Sort = @Sort)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET 
								CATH_TRGT.COARCDEVID=CATH_SRC.COARCDEVID,
								CATH_TRGT.COARCDEVTYPE=CATH_SRC.COARCDEVTYPE,
								CATH_TRGT.COARCBALLPRESSURE=CATH_SRC.COARCBALLPRESSURE,
								CATH_TRGT.COARCBALLOUTCOME=CATH_SRC.COARCBALLOUTCOME,
								CATH_TRGT.COARCSTENTOUTCOME=CATH_SRC.COARCSTENTOUTCOME,
								CATH_TRGT.COARCPOSTINSTENTDIAMETER=CATH_SRC.COARCPOSTINSTENTDIAMETER,
								CATH_TRGT.SORT=CATH_SRC.SORT,
								CATH_TRGT.COARCPOSTINSTENTDIAMASSESSED=CATH_SRC.COARCPOSTINSTENTDIAMASSESSED	
							FROM CATHCOARCPROCDEVICE CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.CoarcProcID = AC.CoarcProcID AND CATH_TRGT.SORT = AC.SORT  
							INNER JOIN [dbo].[CHOP_IMPACT_CATHCOARCPROCDEVICE] CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID  and CATH_SRC.SORT = AC.SORT  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHCOARCPROCDEVICE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHCOARCPROCDEVICE',@EventID,4,@DBVrsn;
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
								FROM [dbo].[CHOP_IMPACT_CATHCOARCPROCDEVICE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID   and CATH_SRC.SORT = AC.SORT  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.CoarcProcID FROM CathCoarcProc C INNER JOIN @CathCaseList AC ON C.CoarcProcID = AC.CoarcProcID WHERE AC.RowID = @i  )
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CATHCOARCPROCDEVICE (CoarcProcID,CoarcDevID,CoarcDevType,CoarcBallPurp,CoarcBallPressure,CoarcBallOutcome,CoarcStentOutcome,CoarcPostInStentDiameter,Sort,CoarcPostInStentDiamAssessed) 
						SELECT AC.CoarcProcID,CoarcDevID,CoarcDevType,CoarcBallPurp,CoarcBallPressure,CoarcBallOutcome,CoarcStentOutcome,CoarcPostInStentDiameter,CATH_SRC.Sort,CoarcPostInStentDiamAssessed
						FROM [dbo].[CHOP_IMPACT_CATHCOARCPROCDEVICE] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID and CATH_SRC.SORT = AC.SORT 
						WHERE AC.RowID = @i

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHCOARCPROCDEVICE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT 
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHCOARCPROCDEVICE',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHCOARCPROCDEVICE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID and CATH_SRC.SORT = AC.SORT 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHCOARCPROCDEVICE] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT 
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
