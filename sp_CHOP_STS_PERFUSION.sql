
/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.CASELINKNUM)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/
--CREATE TRIGGER [dbo].[tr_CHOP_STS_PERFUSION] ON [dbo].[CHOP_STS_PERFUSION]
--AFTER UPDATE, INSERT  as
ALTER PROCEDURE "dbo"."sp_CHOP_STS_PERFUSION" as

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @AnesthesiaFK int
DECLARE @AnestCaseList TABLE (CaseNumber int, CaseLinkNum int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @AnestCaseList(CaseNumber, CaselinkNum, DBVrsn, RowID)
	SELECT CaseNumber, ANEST_SRC.CASELINKNUM,  C.CDataVrsn, ROW_NUMBER() OVER (ORDER BY CaseNumber) 
	FROM [dbo].[CHOP_STS_PERFUSION] ANEST_SRC INNER JOIN dbo.Cases C ON ANEST_SRC.CaseLinkNum = C.CaseLinkNum 
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	ORDER BY CaseNumber
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CaseNumber, @DBVrsn = DBVrsn, @AnesthesiaFK = CaseLinkNum FROM @AnestCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT APM.CaseNumber FROM PERFUSION APM WHERE APM.CaseNumber = @EventID)  
				BEGIN
					BEGIN TRY
					  BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE ANEST_TGT
							SET 
								ANEST_TGT.CPBTm = ANEST_SRC.CPBTm,
								ANEST_TGT.XClampTm = ANEST_SRC.XClampTm,
								ANEST_TGT.DHCATm = ANEST_SRC.DHCATm,
								ANEST_TGT.TempSiteBla = ANEST_SRC.TempSiteBla,
								ANEST_TGT.LowCTmpBla = ANEST_SRC.LowCTmpBla,
								ANEST_TGT.TempSiteEso = ANEST_SRC.TempSiteEso,
								ANEST_TGT.LowCTmpEso = ANEST_SRC.LowCTmpEso,
								ANEST_TGT.TempSiteNas = ANEST_SRC.TempSiteNas,
								ANEST_TGT.LowCTmpNas = ANEST_SRC.LowCTmpNas,
								ANEST_TGT.TempSiteRec = ANEST_SRC.TempSiteRec,
								ANEST_TGT.LowCTmpRec = ANEST_SRC.LowCTmpRec,
								ANEST_TGT.TempSiteTym = ANEST_SRC.TempSiteTym,
								ANEST_TGT.LowCTmpTym = ANEST_SRC.LowCTmpTym,
								ANEST_TGT.TempSiteOth = ANEST_SRC.TempSiteOth,
								ANEST_TGT.LowCTmpOth = ANEST_SRC.LowCTmpOth,
								ANEST_TGT.RewarmTime = ANEST_SRC.RewarmTime,
								ANEST_TGT.CPerfUtil = ANEST_SRC.CPerfUtil,
								ANEST_TGT.CPerfTime = ANEST_SRC.CPerfTime,
								ANEST_TGT.CPerfCanInn = ANEST_SRC.CPerfCanInn,
								ANEST_TGT.CPerfCanRSub = ANEST_SRC.CPerfCanRSub,
								ANEST_TGT.CPerfCanRAx = ANEST_SRC.CPerfCanRAx,
								ANEST_TGT.CPerfCanRCar = ANEST_SRC.CPerfCanRCar,
								ANEST_TGT.CPerfCanLCar = ANEST_SRC.CPerfCanLCar,
								ANEST_TGT.CPerfCanSVC = ANEST_SRC.CPerfCanSVC,
								ANEST_TGT.CPerfPer = ANEST_SRC.CPerfPer,
								ANEST_TGT.CPerfFlow = ANEST_SRC.CPerfFlow,
								ANEST_TGT.CPerfTemp = ANEST_SRC.CPerfTemp,
								ANEST_TGT.ABldGasMgt = ANEST_SRC.ABldGasMgt,
								ANEST_TGT.HCTPriCircA = ANEST_SRC.HCTPriCircA,
								ANEST_TGT.CplegiaDose = ANEST_SRC.CplegiaDose,
								ANEST_TGT.CplegSol = ANEST_SRC.CplegSol,
								ANEST_TGT.InflwOcclTm = ANEST_SRC.InflwOcclTm,
								ANEST_TGT.CerebralFlowType = ANEST_SRC.CerebralFlowType,
								ANEST_TGT.CPBPrimed = ANEST_SRC.CPBPrimed,
								ANEST_TGT.CplegiaDeliv = ANEST_SRC.CplegiaDeliv,
								ANEST_TGT.CplegiaType = ANEST_SRC.CplegiaType,
								ANEST_TGT.HCTFirst = ANEST_SRC.HCTFirst,
								ANEST_TGT.HCTLast = ANEST_SRC.HCTLast,
								ANEST_TGT.HCTPost = ANEST_SRC.HCTPost,
								ANEST_TGT.PRBC = ANEST_SRC.PRBC,
								ANEST_TGT.FFP = ANEST_SRC.FFP,
								ANEST_TGT.WholeBlood = ANEST_SRC.WholeBlood,
								ANEST_TGT.InducedFib = ANEST_SRC.InducedFib,
								ANEST_TGT.InducedFibTmMin = ANEST_SRC.InducedFibTmMin,
								ANEST_TGT.InducedFibTmSec = ANEST_SRC.InducedFibTmSec,
								ANEST_TGT.CoolTimePrior = ANEST_SRC.CoolTimePrior,
								ANEST_TGT.UltrafilPerform = ANEST_SRC.UltrafilPerform,
								ANEST_TGT.UltraFilPerfWhen = ANEST_SRC.UltraFilPerfWhen,
								ANEST_TGT.AnticoagUsed = ANEST_SRC.AnticoagUsed,
								ANEST_TGT.AnticoagUnfHep = ANEST_SRC.AnticoagUnfHep,
								ANEST_TGT.AnticoagArg = ANEST_SRC.AnticoagArg,
								ANEST_TGT.AnticoagBival = ANEST_SRC.AnticoagBival,
								ANEST_TGT.AnticoagOth = ANEST_SRC.AnticoagOth,			
								LastUpdate = GetDate(), 
								UpdatedBy = 'CHOP_AUTOMATION'
							FROM PERFUSION ANEST_TGT INNER JOIN @AnestCaseList AC ON ANEST_TGT.CaseNumber = AC.CaseNumber  
							INNER JOIN [dbo].[CHOP_STS_PERFUSION] ANEST_SRC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM  
							INNER JOIN Cases c ON c.CaseNumber = AC.CaseNumber 
							WHERE AC.RowID = @i
							

							UPDATE Cases
							   SET Cases.HeightCm = ANEST_SRC.HEIGHTCM,
								   Cases.WeightKg = ANEST_SRC.WEIGHTKG
                            FROM Cases  INNER JOIN @AnestCaseList AC ON Cases.CASELINKNUM = AC.CaseLinkNum  
							            INNER JOIN [dbo].[CHOP_STS_PERFUSION] ANEST_SRC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM 


							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 0
							FROM [dbo].[CHOP_STS_PERFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
							WHERE AC.RowID = @i



						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
							DECLARE @DB int, @HospitalizationID int
							DECLARE @NYSActive int, @NYSVrsn nchar(10), @PAC3DBVrsn varchar(10)
							SELECT @NYSActive = Enabled FROM AppConfiguration WHERE Feature = 'NYSCSRSCongenital'

							SELECT @EventID = C.CaseNumber , @DBVrsn = C.CDataVrsn, @NYSVrsn = YEAR(ISNULL(SurgDt, GetDate()))  FROM Cases C 
							WHERE C.CaseNumber = @EventID

							EXEC Validation_Call_ByTableEventID 'Perfusion',@EventID,1,@DBVrsn;

							IF @NYSActive = 1
								EXEC dbo.NYS_Validation_Call_ByTableEventID 'Perfusion', @EventID,@NYSVrsn

							IF EXISTS(SELECT CaseNumber FROM PC4Operative WHERE CaseNumber = @EventID)
								BEGIN
									SET @EventID = 0
									SET @DBVrsn = 0
									SET @DB = 7

									SELECT TOP 1 @DBVrsn = PC4HospDataVrsn, @PAC3DBVrsn = PAChospVrsn, @HospitalizationID = H.HospitalizationID 
									FROM Cases C INNER JOIN Hospitalization H ON C.HospitalizationID = H.HospitalizationID
									WHERE C.CaseNumber = @EventID
			
									UPDATE PC4Operative 
									SET  PC4Operative.CPBTm = P.CPBTm
										,PC4Operative.XClampTm = P.XClampTm
									    ,PC4Operative.DHCATm = P.DHCATm
										,PC4Operative.CPerfUtil = P.CPerfUtil
										,PC4Operative.CPerfTm = P.CPerfTime
									FROM Perfusion P join PC4Operative PC4 on P.CaseNumber = PC4.CaseNumber									                                          
									WHERE P.CaseNumber = @EventID

									SELECT Top 1 @EventID = EncounterID FROM PC4Encounter WHERE HospitalizationID = @HospitalizationID
									IF @EventID IS NOT NULL AND @EventID > 0
										BEGIN
											EXEC Validation_Call_OneToManyDetail 'PC4Operative',@EventID,@DB,@DBVrsn;
										END

									SET @EventID = 0
									SET @DB = 8
			
									SELECT Top 1 @EventID = PACEncounterID FROM PACEncounter WHERE HospitalizationID = @HospitalizationID
									IF @EventID IS NOT NULL AND @EventID > 0
										BEGIN
											EXEC Validation_Call_OneToManyDetail 'PC4Operative',@EventID,@DB,@PAC3DBVrsn;
										END
								END
				
						


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
								FROM [dbo].[CHOP_STS_PERFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
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
						INSERT INTO dbo.PERFUSION (CaseNumber,	HospitalID,	CPBTm,	XClampTm,	DHCATm,	TempSiteBla,	LowCTmpBla,	TempSiteEso,	LowCTmpEso,	TempSiteNas,	LowCTmpNas,	TempSiteRec,	LowCTmpRec,	TempSiteTym,	LowCTmpTym,	TempSiteOth,	LowCTmpOth,	RewarmTime,	CPerfUtil,	CPerfTime,	CPerfCanInn,	CPerfCanRSub,	CPerfCanRAx,	CPerfCanRCar,	CPerfCanLCar,	CPerfCanSVC,	CPerfPer,	CPerfFlow,	CPerfTemp,	ABldGasMgt,	HCTPriCircA,	CplegiaDose,  CplegSol,		CreateDate,	LastUpdate,	UpdatedBy,	InflwOcclTm,	CerebralFlowType,	CPBPrimed,	CplegiaDeliv,	CplegiaType,	HCTFirst,	HCTLast,	HCTPost,	PRBC,	FFP,	WholeBlood,	InducedFib,	InducedFibTmMin,	InducedFibTmSec,	CoolTimePrior,	UltrafilPerform,	UltraFilPerfWhen,	AnticoagUsed,	AnticoagUnfHep,	AnticoagArg,	AnticoagBival,	AnticoagOth)
						SELECT	AC.CaseNumber,	4,	CPBTm,	XClampTm,	DHCATm,	TempSiteBla,	LowCTmpBla,	TempSiteEso,	LowCTmpEso,	TempSiteNas,	LowCTmpNas,	TempSiteRec,	LowCTmpRec,	TempSiteTym,	LowCTmpTym,	TempSiteOth,	LowCTmpOth,	RewarmTime,	CPerfUtil,	CPerfTime,	CPerfCanInn,	CPerfCanRSub,	CPerfCanRAx,	CPerfCanRCar,	CPerfCanLCar,	CPerfCanSVC,	CPerfPer,	CPerfFlow,	CPerfTemp,	ABldGasMgt,	HCTPriCircA,	CplegiaDose,  CplegSol,		getdate(),	getdate(),	'CHOP_AUTOMATION',	InflwOcclTm,	CerebralFlowType,	CPBPrimed,	CplegiaDeliv,	CplegiaType,	HCTFirst,	HCTLast,	HCTPost,	PRBC,	FFP,	WholeBlood,	InducedFib,	InducedFibTmMin,	InducedFibTmSec,	CoolTimePrior,	UltrafilPerform,	UltraFilPerfWhen,	AnticoagUsed,	AnticoagUnfHep,	AnticoagArg,	AnticoagBival,	AnticoagOth
						FROM [dbo].[CHOP_STS_PERFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM 
						                                           INNER JOIN Cases c ON c.CaseNumber = AC.CaseNumber 
						WHERE AC.RowID = @i

						UPDATE Cases
						   SET Cases.HeightCm = ANEST_SRC.HEIGHTCM,
								   Cases.WeightKg = ANEST_SRC.WEIGHTKG
                            FROM Cases  INNER JOIN @AnestCaseList AC ON Cases.CASELINKNUM = AC.CaseLinkNum  
							            INNER JOIN [dbo].[CHOP_STS_PERFUSION] ANEST_SRC ON AC.CaseLinkNum = ANEST_SRC.CASELINKNUM 

						UPDATE ANEST_SRC 
						SET ANEST_SRC.PendingImport = 0
						FROM [dbo].[CHOP_STS_PERFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
						SELECT @NYSActive = Enabled FROM AppConfiguration WHERE Feature = 'NYSCSRSCongenital'

						SELECT @EventID = C.CaseNumber , @DBVrsn = C.CDataVrsn, @NYSVrsn = YEAR(ISNULL(SurgDt, GetDate()))  FROM Cases C 
						WHERE C.CaseNumber = @EventID

						EXEC Validation_Call_ByTableEventID 'Perfusion',@EventID,1,@DBVrsn;

						IF @NYSActive = 1
							EXEC dbo.NYS_Validation_Call_ByTableEventID 'Perfusion', @EventID,@NYSVrsn

						IF EXISTS(SELECT CaseNumber FROM PC4Operative WHERE CaseNumber = @EventID)
							BEGIN
								SET @EventID = 0
								SET @DBVrsn = 0
								SET @DB = 7

								SELECT TOP 1 @DBVrsn = PC4HospDataVrsn, @PAC3DBVrsn = PAChospVrsn, @HospitalizationID = H.HospitalizationID 
								FROM Cases C INNER JOIN Hospitalization H ON C.HospitalizationID = H.HospitalizationID
								WHERE C.CaseNumber = @EventID
			
								UPDATE PC4Operative 
									SET  PC4Operative.CPBTm = P.CPBTm
										,PC4Operative.XClampTm = P.XClampTm
									    ,PC4Operative.DHCATm = P.DHCATm
										,PC4Operative.CPerfUtil = P.CPerfUtil
										,PC4Operative.CPerfTm = P.CPerfTime
									FROM Perfusion P join PC4Operative PC4 on P.CaseNumber = PC4.CaseNumber									                                          
									WHERE P.CaseNumber = @EventID

								SELECT Top 1 @EventID = EncounterID FROM PC4Encounter WHERE HospitalizationID = @HospitalizationID
								IF @EventID IS NOT NULL AND @EventID > 0
									BEGIN
										EXEC Validation_Call_OneToManyDetail 'PC4Operative',@EventID,@DB,@DBVrsn;
									END

								SET @EventID = 0
								SET @DB = 8
			
								SELECT Top 1 @EventID = PACEncounterID FROM PACEncounter WHERE HospitalizationID = @HospitalizationID
								IF @EventID IS NOT NULL AND @EventID > 0
									BEGIN
										EXEC Validation_Call_OneToManyDetail 'PC4Operative',@EventID,@DB,@PAC3DBVrsn;
									END
							END
				
					END

                  
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE ANEST_SRC 
							SET ANEST_SRC.PendingImport = 4
							FROM [dbo].[CHOP_STS_PERFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE ANEST_SRC 
					SET ANEST_SRC.PendingImport = 2
					FROM [dbo].[CHOP_STS_PERFUSION] ANEST_SRC INNER JOIN @AnestCaseList AC ON ANEST_SRC.CASELINKNUM = AC.CaseLinkNum  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END

