/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/ 

CREATE PROCEDURE sp_CHOP_IMPACT_CATHDEVICEASSN

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @CathID int, @DeviceID int, @DefectID int, @DBVrsn varchar(5), @Sort int, @None int
DECLARE @CathCaseList TABLE (CathID int, DeviceID int, DefectID int, DBVrsn varchar(5), Sort int, RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(CathID, DeviceID, DefectID, DBVrsn, Sort, RowID)
	SELECT devuse.CathID, DeviceID, DefectID, IMPACTDataVrsn, defs.Sort, ROW_NUMBER() OVER (ORDER BY devuse.CathID) 
	FROM CathDevicesUsed devuse JOIN (SELECT CathID, PASD.DefectID, PASD.Sort, 2009 ProcID FROM CathPAStent PAS INNER JOIN CathPAStentDefect PASD ON PAS.PAStentID = PASD.PAStentID 
									   UNION ALL
									  SELECT CathID, ASDD.DefectID, ASDD.Sort, 2004 ProcID FROM CathASDClosure ASDC INNER JOIN CathASDDefect ASDD ON ASDC.ASDClosureID = ASDD.ASDClosureID ) defs 
										on defs.CATHId = devuse.CathID and defs.procid = devuse.cathprocid and defs.sort = devuse.sort
								INNER JOIN dbo.CathData C ON devuse.cathid = C.cathid 
	                            INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
								INNER JOIN DBO.CHOP_IMPACT_CATHDATA CD ON CD.SURG_ENC_ID = C.EMREVENTID
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	--WHERE devuse.CATHID = 9015
	ORDER BY CathID
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID =  CathID, @CathID = CathID, @DBVrsn = DBVrsn, @DeviceID = DeviceID, @DefectID = DefectID, @Sort=Sort FROM @CathCaseList AC WHERE AC.RowID = @i 

			IF EXISTS(SELECT ASD.DeviceID FROM CATHDEVICEASSN ASD WHERE ASD.DeviceID = @DeviceID and ASD.Sort = @Sort)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET 
								CATH_TRGT.DeviceID=AC.DeviceID,
								CATH_TRGT.DefectCounterAssn=AC.DefectID,
								CATH_TRGT.SORT=AC.SORT
									
							FROM CATHDEVICEASSN CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.DefectCounterAssn = AC.DefectID   
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							/*UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].CHOP_IMPACT_CATHDEVICESUSED CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
							WHERE AC.RowID = @i
						    */ 
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHDEVICEASSN',@EventID,4,@DBVrsn;
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
								
							/*	UPDATE CATH_SRC 
								SET CATH_SRC.PendingImport = 3
								FROM [dbo].CHOP_IMPACT_CATHDEVICESUSED CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								*/
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.CathID FROM CathDevicesUsed C INNER JOIN @CathCaseList AC ON C.CathID = AC.CathID WHERE AC.RowID = @i and C.Sort = @Sort)
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CATHDEVICEASSN (DeviceID,DefectCounterAssn,Sort,CathProcID)
						SELECT devuse.DeviceID, defs.DefectID, defs.Sort, ProcID
								FROM CathDevicesUsed devuse JOIN (SELECT CathID, PASD.DefectID, PASD.Sort, 2009 ProcID FROM CathPAStent PAS INNER JOIN CathPAStentDefect PASD ON PAS.PAStentID = PASD.PAStentID 
																	UNION ALL
																  SELECT CathID, ASDD.DefectID, ASDD.Sort, 2004 ProcID FROM CathASDClosure ASDC INNER JOIN CathASDDefect ASDD ON ASDC.ASDClosureID = ASDD.ASDClosureID ) defs on defs.CATHId = devuse.CathID and defs.procid = devuse.cathprocid and defs.sort = devuse.sort
                                INNER JOIN @CathCaseList AC ON AC.CathID = devuse.CathID 
						WHERE AC.RowID = @i and devuse.Sort = @Sort

						/*UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].CHOP_IMPACT_CATHDEVICESUSED CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
						WHERE AC.RowID = @i
						*/
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHDEVICEASSN',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							/*UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].CHOP_IMPACT_CATHDEVICESUSED CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID 
							WHERE AC.RowID = @i
							*/
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)
							
							
						END						
				END CATCH

			SET @i = @i + 1
		END
