/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE PROCEDURE sp_CHOP_IMPACT_CATHHISTORY 

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @HospitalizationID int, @EventID int, @PatID int, @DBVrsn varchar(5), @EMREventID int
DECLARE @CathCaseList TABLE (HospitalizationID int, EventID int, PatID int, EMREventID int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList (HospitalizationID, EventID, PatId, EMREventID, DBVrsn, RowID)
	SELECT H.HospitalizationID, C.CATHID, C.PatID, cath_src.surg_enc_id,  h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY c.CathID) 
	FROM [dbo].[CHOP_IMPACT_CATHHISTORY] CATH_SRC LEFT JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID
	                                              INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
	                                              LEFT join CATHHISTORY tgt on H.HospitalizationID = tgt.HospitalizationID 
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	--and surg_enc_id in (2059350625)
	ORDER BY CathID
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = HospitalizationID, @DBVrsn = DBVrsn, @EMREventID = EMREventID FROM @CathCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT ASD.HospitalizationID FROM CATHHISTORY ASD WHERE ASD.HospitalizationID = @EventID)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET 
							CATH_TRGT.CHRONICLUNGDISEASE=CATH_SRC.CHRONICLUNGDISEASE,
							CATH_TRGT.COAGDISORDER=CATH_SRC.COAGDISORDER,
							CATH_TRGT.HYPERCOAG=CATH_SRC.HYPERCOAG,
							CATH_TRGT.HYPOCOAG=CATH_SRC.HYPOCOAG,
							CATH_TRGT.DIABETES=CATH_SRC.DIABETES,
							CATH_TRGT.HEPATICDISEASE=CATH_SRC.HEPATICDISEASE,
							CATH_TRGT.RENALINSUFF=CATH_SRC.RENALINSUFF,
							CATH_TRGT.SEIZURES=CATH_SRC.SEIZURES,
							CATH_TRGT.SICKLECELL=CATH_SRC.SICKLECELL,
							CATH_TRGT.PRIORSTROKE=CATH_SRC.PRIORSTROKE,
							CATH_TRGT.LASTUPDATE=GETDATE(),
							CATH_TRGT.UPDATEDBY='CHOP_AUTOMATION',
							CATH_TRGT.ARRHYTHMIA=CATH_SRC.ARRHYTHMIA,
							CATH_TRGT.PRIORCM=CATH_SRC.PRIORCM,
							CATH_TRGT.PRIORCMHX=CATH_SRC.PRIORCMHX,
							CATH_TRGT.ENDOCARDITIS=CATH_SRC.ENDOCARDITIS,
							CATH_TRGT.HF=CATH_SRC.HF,
							CATH_TRGT.NYHA=CATH_SRC.NYHA,
							CATH_TRGT.HEARTTRANSPLANT=CATH_SRC.HEARTTRANSPLANT,
							CATH_TRGT.ISCHEMICHD=CATH_SRC.ISCHEMICHD,
							CATH_TRGT.KAWASAKIDISEASE=CATH_SRC.KAWASAKIDISEASE,
							CATH_TRGT.RHEUMATICHD=CATH_SRC.RHEUMATICHD
			
							FROM CathHistory CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.HospitalizationID = AC.HospitalizationID  
							INNER JOIN [dbo].[CHOP_IMPACT_CATHHISTORY] CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHHISTORY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHHISTORY',@EventID,4,@DBVrsn;
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
								FROM [dbo].[CHOP_IMPACT_CATHHISTORY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.CathID FROM CathData C INNER JOIN @CathCaseList AC ON C.CathID = AC.EventID WHERE AC.RowID = @i)
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CATHHISTORY (HospitalizationID, HospitalID,PatID,ChronicLungDisease,CoagDisorder,HyperCoag,HypoCoag,Diabetes,HepaticDisease,RenalInsuff,Seizures,SickleCell,PriorStroke,CreateDate, LastUpdate,UpdatedBy,Arrhythmia,PriorCM,PriorCMHx,Endocarditis,HF,NYHA,HeartTransplant,IschemicHD,KawasakiDisease,RheumaticHD) 
						SELECT  AC.HospitalizationID,4,PatID,ChronicLungDisease,CoagDisorder,HyperCoag,HypoCoag,Diabetes,HepaticDisease,RenalInsuff,Seizures,SickleCell,PriorStroke,getdate(), getdate(),'CHOP_AUTOMATION',Arrhythmia,PriorCM,PriorCMHx,Endocarditis,HF,NYHA,HeartTransplant,IschemicHD,KawasakiDisease,RheumaticHD 
						FROM [dbo].[CHOP_IMPACT_CATHHISTORY] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID 
		
						WHERE AC.RowID = @i

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHHISTORY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHHISTORY',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHHISTORY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHHISTORY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
