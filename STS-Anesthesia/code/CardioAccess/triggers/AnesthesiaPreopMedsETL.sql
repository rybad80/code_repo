/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.CASELINKNUM)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE TRIGGER tr_CHOP_CCAS_ANESTHESIA_PREOPMEDS ON CHOP_CCAS_ANESTHESIA_PREOPMEDS
AFTER UPDATE, INSERT  as


DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @AnesthesiaFK int
DECLARE @AnestCaseList TABLE (CaseNumber int, CaseLinkNum int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @AnestCaseList(CaseNumber, CaselinkNum, DBVrsn, RowID)
	SELECT CaseNumber, ANEST_SRC.CASELINKNUM, C.CDataVrsn, ROW_NUMBER() OVER (ORDER BY CaseNumber) 
	FROM [dbo].[CHOP_CCAS_ANESTHESIA_PREOPMEDS] ANEST_SRC INNER JOIN dbo.Cases C ON ANEST_SRC.CaseLinkNum = C.CaseLinkNum  --CHANGE TABLE NAME
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	ORDER BY CaseNumber
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CaseNumber, @DBVrsn = DBVrsn, @AnesthesiaFK = CaseLinkNum FROM @AnestCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT APM.CaseNumber FROM AnesthesiaPreopMeds APM WHERE APM.CaseNumber = @EventID)  --CHANGE TABLE NAME
				BEGIN
					BEGIN TRY
						BEGIN
							--BEGIN THE TRANSACTION
							BEGIN TRAN
								--UPDATE THE TARGET RECORD
								UPDATE ANEST_TRGT
								SET ANEST_TRGT.PreopO2Sat = ANEST_SRC.PreopO2Sat
								, ANEST_TRGT.PreopOxygen = ANEST_SRC.PreopOxygen
								, ANEST_TRGT.PLocTransDT = ANEST_SRC.PLocTransDT
								, ANEST_TRGT.PreopSed = ANEST_SRC.PreopSed
								, ANEST_TRGT.PreopSedRte =  ANEST_SRC.PreopSedRte 
								, ANEST_TRGT.PreopSedDrugAtro =  ANEST_SRC.PreopSedDrugAtro 
								, ANEST_TRGT.PreopSedDrugDem =  ANEST_SRC.PreopSedDrugDem 
								, ANEST_TRGT.PreopSedDrugDex =  ANEST_SRC.PreopSedDrugDex 
								, ANEST_TRGT.PreopSedDrugDiaz =  ANEST_SRC.PreopSedDrugDiaz 
								, ANEST_TRGT.PreopSedDrugFent =  ANEST_SRC.PreopSedDrugFent 
								, ANEST_TRGT.PreopSedDrugGlyco =  ANEST_SRC.PreopSedDrugGlyco 
								, ANEST_TRGT.PreopSedDrugKet =  ANEST_SRC.PreopSedDrugKet 
								, ANEST_TRGT.PreopSedDrugLoraz =  ANEST_SRC.PreopSedDrugLoraz 
								, ANEST_TRGT.PreopSedDrugMidaz =  ANEST_SRC.PreopSedDrugMidaz 
								, ANEST_TRGT.PreopSedDrugMorph =  ANEST_SRC.PreopSedDrugMorph 
								, ANEST_TRGT.PreopSedDrugPent =  ANEST_SRC.PreopSedDrugPent 
								, LastUpdate = GetDate()
								, UpdateBy = 'CHOP_AUTOMATION'
								FROM AnesthesiaPreopMeds ANEST_TRGT INNER JOIN @AnestCaseList AC ON ANEST_TRGT.CaseNumber = AC.CaseNumber  --CHANGE TABLE and COLUMNS NAMES
								INNER JOIN [dbo].[CHOP_CCAS_ANESTHESIA_PREOPMEDS] ANEST_SRC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM  --CHANGE TABLE NAME
								WHERE AC.RowID = @i
							
								--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
								UPDATE ANEST_SRC 
								SET ANEST_SRC.PendingImport = 0
								FROM [dbo].[CHOP_CCAS_ANESTHESIA_PREOPMEDS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  --CHANGE TABLE NAME
								WHERE AC.RowID = @i
						
							COMMIT TRAN
							--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
							-- VALIDATE THE RECORD FOR THE END USER
							EXEC Validation_Call_ByTableEventID 'AnesthesiaPreopMeds',@EventID,1,@DBVrsn; --CHANGE TABLE NAME
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
								FROM [dbo].[CHOP_CCAS_ANESTHESIA_PREOPMEDS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  --CHANGE TABLE NAME
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
							INSERT INTO dbo.AnesthesiaPreopMeds (HospitalID, CaseNumber
							, PreopO2Sat
							, PreopOxygen
							, PLocTransDT
							, PreopSed
							, PreopSedRte
							, PreopSedDrugAtro
							, PreopSedDrugDem
							, PreopSedDrugDex
							, PreopSedDrugDiaz
							, PreopSedDrugFent
							, PreopSedDrugGlyco
							, PreopSedDrugKet
							, PreopSedDrugLoraz
							, PreopSedDrugMidaz
							, PreopSedDrugMorph
							, PreopSedDrugPent
							, CreateDate
							, LastUpdate
							, UpdateBy)
							SELECT 4
							, AC.CaseNumber
							, PreopO2Sat
							, PreopOxygen
							, PLocTransDT
							, PreopSed
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedRte ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugAtro ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugDem ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugDex ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugDiaz ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugFent ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugGlyco ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugKet ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugLoraz ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugMidaz ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugMorph ELSE NULL END
							, CASE WHEN ANEST_SRC.PreopSed = 1 THEN PreopSedDrugPent ELSE NULL END
							, GetDate()
							, GetDate()
							, 'CHOP_AUTOMATION' 
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_PREOPMEDS] ANEST_SRC INNER JOIN @AnestCaseList AC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM  --CHANGE TABLE NAME
							WHERE AC.RowID = @i

							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 0
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_PREOPMEDS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  --CHANGE TABLE NAME
							WHERE AC.RowID = @i
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'AnesthesiaPreopMeds',@EventID,1,@DBVrsn; --CHANGE TABLE NAME
					END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 4
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_PREOPMEDS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  --CHANGE TABLE NAME
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE ANEST_SRC 
					SET ANEST_SRC.PendingImport = 2
					FROM [dbo].[CHOP_CCAS_ANESTHESIA_PREOPMEDS] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  --CHANGE TABLE NAME
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
