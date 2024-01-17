/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.CASELINKNUM)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE TRIGGER tr_CHOP_CCAS_ANESTHESIA_TECHNIQUE ON CHOP_CCAS_ANESTHESIA_TECHNIQUE
AFTER UPDATE, INSERT  as


DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @AnesthesiaFK int
DECLARE @AnestCaseList TABLE (CaseNumber int, CaseLinkNum int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @AnestCaseList(CaseNumber, CaselinkNum, DBVrsn, RowID)
	SELECT CaseNumber, ANEST_SRC.CASELINKNUM, C.CDataVrsn, ROW_NUMBER() OVER (ORDER BY CaseNumber) 
	FROM [dbo].[CHOP_CCAS_ANESTHESIA_TECHNIQUE] ANEST_SRC INNER JOIN dbo.Cases C ON ANEST_SRC.CaseLinkNum = C.CaseLinkNum 
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	ORDER BY CaseNumber
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CaseNumber, @DBVrsn = DBVrsn, @AnesthesiaFK = CaseLinkNum FROM @AnestCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT APM.CaseNumber FROM AnesthesiaTechnique APM WHERE APM.CaseNumber = @EventID)  
				BEGIN
					BEGIN TRY
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE ANEST_TRGT
							SET ANEST_TRGT.INDUCTIONDT=ANEST_SRC.INDUCTIONDT
								,ANEST_TRGT.INDTYPEINH=ANEST_SRC.INDTYPEINH
								,ANEST_TRGT.INDAGENTINHALSEVO=ANEST_SRC.INDAGENTINHALSEVO
								,ANEST_TRGT.INDAGENTINHALISO=ANEST_SRC.INDAGENTINHALISO
								,ANEST_TRGT.INDTYPEIV=ANEST_SRC.INDTYPEIV
								,ANEST_TRGT.INDAGENTIVSODT=ANEST_SRC.INDAGENTIVSODT
								,ANEST_TRGT.INDAGENTIVKET=ANEST_SRC.INDAGENTIVKET
								,ANEST_TRGT.INDAGENTIVETOM=ANEST_SRC.INDAGENTIVETOM
								,ANEST_TRGT.INDAGENTIVPROP=ANEST_SRC.INDAGENTIVPROP
								,ANEST_TRGT.INDAGENTIVFENT=ANEST_SRC.INDAGENTIVFENT
								,ANEST_TRGT.INDAGENTIVMID=ANEST_SRC.INDAGENTIVMID
								,ANEST_TRGT.INDAGENTIVDEX=ANEST_SRC.INDAGENTIVDEX
								,ANEST_TRGT.INDAGENTIVSUF=ANEST_SRC.INDAGENTIVSUF
								,ANEST_TRGT.INDAGENTIVREM=ANEST_SRC.INDAGENTIVREM
								,ANEST_TRGT.INDTYPEIM=ANEST_SRC.INDTYPEIM
								,ANEST_TRGT.INDAGENTIMKET=ANEST_SRC.INDAGENTIMKET
								,ANEST_TRGT.INDAGENTIMMID=ANEST_SRC.INDAGENTIMMID
								,ANEST_TRGT.REGIONALANES=ANEST_SRC.REGIONALANES
								,ANEST_TRGT.REGANESSITE=ANEST_SRC.REGANESSITE
								,ANEST_TRGT.REGANESDRUGBUP=ANEST_SRC.REGANESDRUGBUP
								,ANEST_TRGT.REGANESDRUGBUPFEN=ANEST_SRC.REGANESDRUGBUPFEN
								,ANEST_TRGT.REGANESDRUGCLON=ANEST_SRC.REGANESDRUGCLON
								,ANEST_TRGT.REGANESDRUGFEN=ANEST_SRC.REGANESDRUGFEN
								,ANEST_TRGT.REGANESDRUGHYDRO=ANEST_SRC.REGANESDRUGHYDRO
								,ANEST_TRGT.REGANESDRUGLIDO=ANEST_SRC.REGANESDRUGLIDO
								,ANEST_TRGT.REGANESDRUGMORPH=ANEST_SRC.REGANESDRUGMORPH
								,ANEST_TRGT.REGANESDRUGROP=ANEST_SRC.REGANESDRUGROP
								,ANEST_TRGT.REGANESDRUGROPFEN=ANEST_SRC.REGANESDRUGROPFEN
								,ANEST_TRGT.REGANESDRUGTETRA=ANEST_SRC.REGANESDRUGTETRA
								,ANEST_TRGT.REGANESDRUGOTH=ANEST_SRC.REGANESDRUGOTH
								,ANEST_TRGT.INTNERVEINF=ANEST_SRC.INTNERVEINF 
								,ANEST_TRGT.REGFIELDBLOCK=ANEST_SRC.REGFIELDBLOCK
 								,LastUpdate = GetDate()
 								,UpdateBy = 'CHOP_AUTOMATION'
							FROM AnesthesiaTechnique ANEST_TRGT INNER JOIN @AnestCaseList AC ON ANEST_TRGT.CaseNumber = AC.CaseNumber  
							INNER JOIN [dbo].[CHOP_CCAS_ANESTHESIA_TECHNIQUE] ANEST_SRC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 0
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_TECHNIQUE] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'AnesthesiaTechnique',@EventID,1,@DBVrsn;
						
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
								FROM [dbo].[CHOP_CCAS_ANESTHESIA_TECHNIQUE] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.CaseNumber FROM Cases C INNER JOIN @AnestCaseList AC ON C.CaseNumber = AC.CaseNumber WHERE AC.RowID = @i)
				
				BEGIN TRY
					BEGIN TRAN
						INSERT INTO dbo.AnesthesiaTechnique (HospitalID,Casenumber,INDUCTIONDT,INDTYPEINH,INDAGENTINHALSEVO,INDAGENTINHALISO,INDTYPEIV,INDAGENTIVSODT,INDAGENTIVKET,INDAGENTIVETOM,INDAGENTIVPROP,
                        INDAGENTIVFENT,INDAGENTIVMID,INDAGENTIVDEX,INDAGENTIVSUF,INDAGENTIVREM,INDTYPEIM,INDAGENTIMKET,INDAGENTIMMID,REGIONALANES,REGANESSITE,REGANESDRUGBUP,REGANESDRUGBUPFEN,REGANESDRUGCLON,
                        REGANESDRUGFEN,REGANESDRUGHYDRO,REGANESDRUGLIDO,REGANESDRUGMORPH,REGANESDRUGROP,REGANESDRUGROPFEN,REGANESDRUGTETRA,REGANESDRUGOTH,INTNERVEINF,REGFIELDBLOCK,CreateDate,LastUpdate,UpdateBy)
						SELECT 4, AC.CaseNumber,INDUCTIONDT,INDTYPEINH,INDAGENTINHALSEVO,INDAGENTINHALISO,INDTYPEIV,INDAGENTIVSODT,INDAGENTIVKET,INDAGENTIVETOM,INDAGENTIVPROP,
                        INDAGENTIVFENT,INDAGENTIVMID,INDAGENTIVDEX,INDAGENTIVSUF,INDAGENTIVREM,INDTYPEIM,INDAGENTIMKET,INDAGENTIMMID,REGIONALANES,REGANESSITE,REGANESDRUGBUP,REGANESDRUGBUPFEN,REGANESDRUGCLON,
                        REGANESDRUGFEN,REGANESDRUGHYDRO,REGANESDRUGLIDO,REGANESDRUGMORPH,REGANESDRUGROP,REGANESDRUGROPFEN,REGANESDRUGTETRA,REGANESDRUGOTH,INTNERVEINF,REGFIELDBLOCK, GetDate(), GetDate(), 'CHOP_AUTOMATION' 
						FROM [dbo].[CHOP_CCAS_ANESTHESIA_TECHNIQUE] ANEST_SRC INNER JOIN @AnestCaseList AC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM 
						WHERE AC.RowID = @i

						UPDATE ANEST_SRC 
						SET ANEST_SRC.PendingImport = 0
						FROM [dbo].[CHOP_CCAS_ANESTHESIA_TECHNIQUE] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'AnesthesiaTechnique',@EventID,1,@DBVrsn;

				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 4
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_TECHNIQUE] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE ANEST_SRC 
					SET ANEST_SRC.PendingImport = 2
					FROM [dbo].[CHOP_CCAS_ANESTHESIA_TECHNIQUE] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
