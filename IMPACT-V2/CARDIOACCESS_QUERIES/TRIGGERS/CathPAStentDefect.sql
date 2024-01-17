/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE TRIGGER TR_CHOP_IMPACT_CATHPASTENTDEFECT ON CHOP_IMPACT_CATHPASTENTDEFECT
AFTER UPDATE, INSERT

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int, @Sort int
DECLARE @CathCaseList TABLE (PAStentID int, EMREventID int, Sort int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(PAStentID, EMREventID, Sort, DBVrsn, RowID)
	SELECT tgt1.PAStentID, CATH_SRC.SURG_ENC_ID, CATH_SRC.Sort, h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY c.CathID) 
	FROM [dbo].[CHOP_IMPACT_CATHPASTENTDEFECT] CATH_SRC   INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                      INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
														  left join CATHPASTENT tgt1 on c.cathid = tgt1.cathid
														  left join CATHPASTENTDEFECT tgt2 on tgt1.PAStentID = tgt2.PAStentID and cath_src.sort = tgt2.sort
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	--WHERE SURG_ENC_ID = 2061379399
	ORDER BY c.CathID

		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
					SELECT @EventID = PAStentID, @DBVrsn = DBVrsn, @EMREventID = EMREventID, @Sort = Sort FROM @CathCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT ASD.PAStentID FROM CATHPASTENTDEFECT ASD WHERE ASD.PAStentID = @EventID and asd.Sort = @Sort)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET 
								CATH_TRGT.PASDEFECTLOC=CATH_SRC.PASDEFECTLOC,
								CATH_TRGT.PASOSTIALSTENOSIS=CATH_SRC.PASOSTIALSTENOSIS,
								CATH_TRGT.PASDISOBSTRUCTION=CATH_SRC.PASDISOBSTRUCTION,
								CATH_TRGT.PASSIDEJAIL=CATH_SRC.PASSIDEJAIL,
								CATH_TRGT.PASSIDEJAILINTENDED=CATH_SRC.PASSIDEJAILINTENDED,
								CATH_TRGT.PASSIDEJAILARTERY=CATH_SRC.PASSIDEJAILARTERY,
								CATH_TRGT.PASDSIDEJAILDECFLOW=CATH_SRC.PASDSIDEJAILDECFLOW,
								CATH_TRGT.PASPREPROXSYSPRESS=CATH_SRC.PASPREPROXSYSPRESS,
								CATH_TRGT.PASPREDISTSYSPRESS=CATH_SRC.PASPREDISTSYSPRESS,
								CATH_TRGT.PASPREPROXMEANPRESS=CATH_SRC.PASPREPROXMEANPRESS,
								CATH_TRGT.PASPREDISTMEANPRESS=CATH_SRC.PASPREDISTMEANPRESS,
								CATH_TRGT.PASPREPROXDIAMETER=CATH_SRC.PASPREPROXDIAMETER,
								CATH_TRGT.PASPREDISTDIAMETER=CATH_SRC.PASPREDISTDIAMETER,
								CATH_TRGT.PASPREMINDIAMETER=CATH_SRC.PASPREMINDIAMETER,
								CATH_TRGT.PASDEFECTTREATED=CATH_SRC.PASDEFECTTREATED,
								CATH_TRGT.PASPOSTPROXSYSPRESS=CATH_SRC.PASPOSTPROXSYSPRESS,
								CATH_TRGT.PASPOSTDISTSYSPRESS=CATH_SRC.PASPOSTDISTSYSPRESS,
								CATH_TRGT.PASPOSTPROXMEANPRESS=CATH_SRC.PASPOSTPROXMEANPRESS,
								CATH_TRGT.PASPOSTDISTMEANPRESS=CATH_SRC.PASPOSTDISTMEANPRESS,
								CATH_TRGT.PASPOSTPROXDIAMETER=CATH_SRC.PASPOSTPROXDIAMETER,
								CATH_TRGT.PASPOSTDISTDIAMETER=CATH_SRC.PASPOSTDISTDIAMETER,
								CATH_TRGT.PASPOSTMINDIAMETER=CATH_SRC.PASPOSTMINDIAMETER,
								CATH_TRGT.SORT=CATH_SRC.SORT
							FROM CATHPASTENTDEFECT CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.PAStentID = AC.PAStentID AND CATH_TRGT.SORT = AC.SORT  
							INNER JOIN [dbo].[CHOP_IMPACT_CATHPASTENTDEFECT] CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID  and CATH_SRC.SORT = AC.SORT  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHPASTENTDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHPASTENTDEFECT',@EventID,4,@DBVrsn;
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
								FROM [dbo].[CHOP_IMPACT_CATHPASTENTDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID   and CATH_SRC.SORT = AC.SORT  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.PAStentID FROM CathPAStent C INNER JOIN @CathCaseList AC ON C.PAStentID = AC.PAStentID WHERE AC.RowID = @i  )
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CATHPASTENTDEFECT (PAStentID,PASDefectLoc,PASOstialStenosis,PASDisObstruction,PASSideJail,PASSideJailIntended,PASSideJailArtery,PASDSideJailDecFlow,PASPreProxSysPress,PASPreDistSysPress,PASPreProxMeanPress,PASPreDistMeanPress,PASPreProxDiameter,PASPreDistDiameter,PASPreMinDiameter,PASDefectTreated,PASPostProxSysPress,PASPostDistSysPress,PASPostProxMeanPress,PASPostDistMeanPress,PASPostProxDiameter,PASPostDistDiameter,PASPostMinDiameter,Sort) 
						SELECT AC.PAStentID,PASDefectLoc,PASOstialStenosis,PASDisObstruction,PASSideJail,PASSideJailIntended,PASSideJailArtery,PASDSideJailDecFlow,PASPreProxSysPress,PASPreDistSysPress,PASPreProxMeanPress,PASPreDistMeanPress,PASPreProxDiameter,PASPreDistDiameter,PASPreMinDiameter,PASDefectTreated,PASPostProxSysPress,PASPostDistSysPress,PASPostProxMeanPress,PASPostDistMeanPress,PASPostProxDiameter,PASPostDistDiameter,PASPostMinDiameter,AC.Sort
						FROM [dbo].[CHOP_IMPACT_CATHPASTENTDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID and CATH_SRC.SORT = AC.SORT 
						WHERE AC.RowID = @i

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHPASTENTDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT 
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHPASTENTDEFECT',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHPASTENTDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID and CATH_SRC.SORT = AC.SORT 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHPASTENTDEFECT] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  and CATH_SRC.SORT = AC.SORT 
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
