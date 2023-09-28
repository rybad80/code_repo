
ALTER TRIGGER "dbo"."TR_CHOP_CCAS_ANESTHESIA_AIRWAY" ON CHOP_CCAS_ANESTHESIA_AIRWAY
AFTER UPDATE, INSERT

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @AnesthesiaFK int
DECLARE @AnestCaseList TABLE (CaseNumber int, CaseLinkNum int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @AnestCaseList(CaseNumber, CaselinkNum, DBVrsn, RowID)
	SELECT CaseNumber, ANEST_SRC.CASELINKNUM, C.CDataVrsn, ROW_NUMBER() OVER (ORDER BY CaseNumber) 
	FROM [dbo].[CHOP_CCAS_ANESTHESIA_AIRWAY] ANEST_SRC INNER JOIN dbo.Cases C ON ANEST_SRC.CaseLinkNum = C.CaseLinkNum 
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	ORDER BY CaseNumber
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CaseNumber, @DBVrsn = DBVrsn, @AnesthesiaFK = CaseLinkNum FROM @AnestCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT APM.CaseNumber FROM AnesthesiaAirway APM WHERE APM.CaseNumber = @EventID)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE ANEST_TRGT
							SET ANEST_TRGT.AirwayType=ANEST_SRC.AirwayType,
							    ANEST_TRGT.AirwaySizeLMA=ANEST_SRC.AirwaySizeLMA,
								ANEST_TRGT.AirwaySizeIntub=ANEST_SRC.AirwaySizeIntub,
								ANEST_TRGT.Cuffed=ANEST_SRC.Cuffed,
								ANEST_TRGT.AirwaySite=ANEST_SRC.AirwaySite,
								ANEST_TRGT.AirwayInsitu=ANEST_SRC.AirwayInsitu,
								ANEST_TRGT.EndobroncIso=ANEST_SRC.EndobroncIso,
								ANEST_TRGT.EndobroncIsoMeth=ANEST_SRC.EndobroncIsoMeth,
								ANEST_TRGT.EndOfInductDT=ANEST_SRC.EndOfInductDT, 
								ANEST_TRGT.ICUTYPEVENT=ANEST_SRC.ICUTYPEVENT, 
								LastUpdate = GetDate(), 
								UpdateBy = 'CHOP_AUTOMATION'
							FROM AnesthesiaAirway ANEST_TRGT INNER JOIN @AnestCaseList AC ON ANEST_TRGT.CaseNumber = AC.CaseNumber  
							INNER JOIN [dbo].[CHOP_CCAS_ANESTHESIA_AIRWAY] ANEST_SRC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 0
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_AIRWAY] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'AnesthesiaAirway',@EventID,1,@DBVrsn;
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
								FROM [dbo].[CHOP_CCAS_ANESTHESIA_AIRWAY] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
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
						INSERT INTO dbo.AnesthesiaAirway (HospitalID,Casenumber,AirwayType,AirwaySizeLMA,AirwaySizeIntub,Cuffed,AirwaySite,AirwayInsitu,EndobroncIso,EndobroncIsoMeth,EndOfInductDT,ICUTYPEVENT,CreateDate,LastUpdate,UpdateBy)
						SELECT 4, AC.CaseNumber, AIRWAYTYPE,AIRWAYSIZELMA,AIRWAYSIZEINTUB,CUFFED,AIRWAYSITE,AIRWAYINSITU,ENDOBRONCISO,ENDOBRONCISOMETH,ENDOFINDUCTDT,ICUTYPEVENT, GetDate(), GetDate(), 'CHOP_AUTOMATION' 
						FROM [dbo].[CHOP_CCAS_ANESTHESIA_AIRWAY] ANEST_SRC INNER JOIN @AnestCaseList AC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM 
						WHERE AC.RowID = @i

						UPDATE ANEST_SRC 
						SET ANEST_SRC.PendingImport = 0
						FROM [dbo].[CHOP_CCAS_ANESTHESIA_AIRWAY] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'AnesthesiaAirway',@EventID,1,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 4
							FROM [dbo].[CHOP_CCAS_ANESTHESIA_AIRWAY] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE ANEST_SRC 
					SET ANEST_SRC.PendingImport = 2
					FROM [dbo].[CHOP_CCAS_ANESTHESIA_AIRWAY] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
