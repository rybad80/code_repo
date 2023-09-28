ALTER PROCEDURE "dbo"."sp_CHOP_PEDIPERFORM" as


DECLARE @EventID as int
DECLARE @DB as int
DECLARE @DBVrsn as varchar(10)
DECLARE @casenumber as int
DECLARE @i int
DECLARE @numrows int 
DECLARE @PediperformValidationList TABLE (CaseNumber int, 
DB int, 
DBVrsn varchar(5), 
RowID int)

SET @DB = 9
SET @DBVrsn = '1.0'
SET @EventID = @CaseNumber


SET NOCOUNT ON
			BEGIN
				BEGIN TRAN
					UPDATE TARGET_TBL
					SET 
                        target_tbl.CASENUMBER=stg_tbl.CASENUMBER,
						target_tbl.REOP=stg_tbl.REOP,
						target_tbl.primerbctreat=stg_tbl.primerbctreat,                         
						target_tbl.ASSTPERFUSED=stg_tbl.ASSTPERFUSED,
						target_tbl.STUDENTPERFUSED=stg_tbl.STUDENTPERFUSED,
						target_tbl.ATSUSED=stg_tbl.ATSUSED,
						target_tbl.ATSCOLLVOL=stg_tbl.ATSCOLLVOL,
						target_tbl.ATSRETVOL=stg_tbl.ATSRETVOL,
						target_tbl.ARTPUMPTY=stg_tbl.ARTPUMPTY,
						target_tbl.PUMPBOOTSZ=stg_tbl.PUMPBOOTSZ,
						target_tbl.ARTLINETY=stg_tbl.ARTLINETY,
						target_tbl.ARTPORESZ=stg_tbl.ARTPORESZ,
						target_tbl.ARTLINESZ=stg_tbl.ARTLINESZ,
						target_tbl.VENLINESZ=stg_tbl.VENLINESZ,
						target_tbl.HEMOCONTY=stg_tbl.HEMOCONTY,
						target_tbl.OXYGENATORTY=stg_tbl.OXYGENATORTY,
						target_tbl.BIOCOAT=stg_tbl.BIOCOAT,
						target_tbl.BIOTYPE=stg_tbl.BIOTYPE,
						target_tbl.VENRESERVOIRTY=stg_tbl.VENRESERVOIRTY,
						target_tbl.PRIMEVOL=stg_tbl.PRIMEVOL,
						target_tbl.PRIMEMEDOTHERVOL=stg_tbl.PRIMEMEDOTHERVOL,
						target_tbl.PRIMEMEDUSED=stg_tbl.PRIMEMEDUSED,
						target_tbl.ARTTEMPHIGH=stg_tbl.ARTTEMPHIGH,
						target_tbl.LWSTTEMP=stg_tbl.LWSTTEMP,
						target_tbl.LWSTTEMPSRC=stg_tbl.LWSTTEMPSRC,
						target_tbl.CPBSEPTEMP=stg_tbl.CPBSEPTEMP,
						target_tbl.PHMGMT=stg_tbl.PHMGMT,
						target_tbl.PHSTATWARM=stg_tbl.PHSTATWARM,
						target_tbl.PHSTATCOOL=stg_tbl.PHSTATCOOL,
						target_tbl.PHSTATCOOLTHRESH=stg_tbl.PHSTATCOOLTHRESH,
						target_tbl.CPLEGIASYSTEM=case when coalesce(stg_tbl.CPLEGIAVOL,0) = 0 then 0 else stg_tbl.CPLEGIASYSTEM end,
						target_tbl.CPLEGIABLOODRATIO=stg_tbl.CPLEGIABLOODRATIO,
						target_tbl.CPLEGIACRYSTRATIO=stg_tbl.CPLEGIACRYSTRATIO,
						target_tbl.CPLEGIAVOL=stg_tbl.CPLEGIAVOL,
						target_tbl.CPLEGIACRYSVOL=stg_tbl.CPLEGIACRYSVOL,
						target_tbl.VENDRAINAUG=stg_tbl.VENDRAINAUG,
						target_tbl.VENDRAINAUGLOC=stg_tbl.VENDRAINAUGLOC,
						target_tbl.VENDRAINAUGMAX=stg_tbl.VENDRAINAUGMAX,
						target_tbl.AUTOCIRC=stg_tbl.AUTOCIRC,
						target_tbl.AUTOCIRCPRIMEVOL=stg_tbl.AUTOCIRCPRIMEVOL,
						target_tbl.AUTOHARV=stg_tbl.AUTOHARV,
						target_tbl.AUTOHARVPRECPBVOL=stg_tbl.AUTOHARVPRECPBVOL,
						target_tbl.AUTOHARVCPBVOL=stg_tbl.AUTOHARVCPBVOL,
						target_tbl.AUTOHARVPOSTCPBVOL=stg_tbl.AUTOHARVPOSTCPBVOL,
						target_tbl.AUTOHARVVOL=stg_tbl.AUTOHARVVOL,
						target_tbl.BLOODPRODUSED=stg_tbl.BLOODPRODUSED,
						target_tbl.CPBCRYOVOL=stg_tbl.CPBCRYOVOL,
						target_tbl.CPBFFPVOL=stg_tbl.CPBFFPVOL,
						target_tbl.CPBPLATVOL=stg_tbl.CPBPLATVOL,
						target_tbl.CPBRBCVOL=stg_tbl.CPBRBCVOL,
						target_tbl.CPBWHOLEBLOODVOL=stg_tbl.CPBWHOLEBLOODVOL,
						target_tbl.NONCPBCRYOVOL=stg_tbl.NONCPBCRYOVOL,
						target_tbl.NONCPBFFPVOL=stg_tbl.NONCPBFFPVOL,
						target_tbl.NONCPBPLATVOL=stg_tbl.NONCPBPLATVOL,
						target_tbl.NONCPBRBCVOL=stg_tbl.NONCPBRBCVOL,
						target_tbl.NONCPBWHOLEBLOODVOL=stg_tbl.NONCPBWHOLEBLOODVOL,
						target_tbl.PRIMECRYOVOL=stg_tbl.PRIMECRYOVOL,
						target_tbl.PRIMEFFPVOL=stg_tbl.PRIMEFFPVOL,
						target_tbl.PRIMEPLATVOL=stg_tbl.PRIMEPLATVOL,
						target_tbl.PRIMERBCVOL=stg_tbl.PRIMERBCVOL,
						target_tbl.PRIMEWHOLEBLOODVOL=stg_tbl.PRIMEWHOLEBLOODVOL,
						target_tbl.MODULTRAFILT=stg_tbl.MODULTRAFILT,
						target_tbl.MODULTRAFILTTY=stg_tbl.MODULTRAFILTTY,
						target_tbl.MODULTRAFILTTM=stg_tbl.MODULTRAFILTTM,
						target_tbl.MODULTRAFILTVOLREM=stg_tbl.MODULTRAFILTVOLREM,
						target_tbl.ULTRAFILT=stg_tbl.ULTRAFILT,
						target_tbl.ZEROBALULTRAFILT=stg_tbl.ZEROBALULTRAFILT,
						target_tbl.ZBUFVOL=stg_tbl.ZBUFVOL,
						target_tbl.RESIDPUMPVOL=stg_tbl.RESIDPUMPVOL,
						target_tbl.RESIDVOLPROCESS=stg_tbl.RESIDVOLPROCESS,
						target_tbl.RESIDPROCESSRETURN=stg_tbl.RESIDPROCESSRETURN,
						target_tbl.IRRIGATESOLVOL=stg_tbl.IRRIGATESOLVOL,
						target_tbl.WALLWASTEVOL=stg_tbl.WALLWASTEVOL,
						target_tbl.COLLVOL=stg_tbl.COLLVOL,
						target_tbl.ULTRAFILTVOL=stg_tbl.ULTRAFILTVOL,
						target_tbl.MEDVOLONCPB=stg_tbl.MEDVOLONCPB,
						target_tbl.FLUIDBAL=stg_tbl.FLUIDBAL,
						target_tbl.CPBURINEVOL=stg_tbl.CPBURINEVOL,
						target_tbl.CRYSVOL=stg_tbl.CRYSVOL,
						target_tbl.ACTBASE=stg_tbl.ACTBASE,
						target_tbl.ACTPOSTHEPARIN=stg_tbl.ACTPOSTHEPARIN,
						target_tbl.ACTMINCPB=stg_tbl.ACTMINCPB,
						target_tbl.ACTMAXCPB=stg_tbl.ACTMAXCPB,
						target_tbl.ACTPOSTPROT=stg_tbl.ACTPOSTPROT,
						target_tbl.HEPCONMEASURED=stg_tbl.HEPCONMEASURED,
						target_tbl.CREATLST=case when coalesce(stg_tbl.CREATLST,0) = 0 then 0 else stg_tbl.CREATLST end,
						target_tbl.HCTBASE=stg_tbl.HCTBASE,
						target_tbl.HCTLASTPRECPB=stg_tbl.HCTLASTPRECPB,
						target_tbl.HCTFIRST=stg_tbl.HCTFIRST,
						target_tbl.LWSTHCT=stg_tbl.LWSTHCT,
						target_tbl.HCTLAST=stg_tbl.HCTLAST,
						target_tbl.HCTPOSTPRO=stg_tbl.HCTPOSTPRO,
						target_tbl.HCTFIRSTICU=stg_tbl.HCTFIRSTICU,
						target_tbl.LACTATEFIRSTOR=stg_tbl.LACTATEFIRSTOR,
						target_tbl.LACTATELASTPRECPB=stg_tbl.LACTATELASTPRECPB,
						target_tbl.LACTATEFIRSTONCPB=stg_tbl.LACTATEFIRSTONCPB,
						target_tbl.LACTATELASTONCPB=stg_tbl.LACTATELASTONCPB,
						target_tbl.LACTATEPOSTPRO=stg_tbl.LACTATEPOSTPRO,
						target_tbl.INTRAOPDEATH=stg_tbl.INTRAOPDEATH,
						target_tbl.CREATFIRSTICU=stg_tbl.CREATFIRSTICU,
						target_tbl.CREATMAX48=stg_tbl.CREATMAX48,
						target_tbl.LACTATEMAX24=stg_tbl.LACTATEMAX24,
						target_tbl.CHESTTUBEOUTLT24=stg_tbl.CHESTTUBEOUTLT24,
						target_tbl.LastUpdate=getdate(),
						target_tbl.UpdateBy='CHOP_AUTOMATION'
               --select *
			        FROM PediPERForm TARGET_TBL
					INNER JOIN v_load_chop_PediPERForm STG_TBL on TARGET_TBL.CaseNumber = STG_TBL.CaseNumber
					inner join cases on target_tbl.CaseNumber = cases.CaseNumber
					where cases.SurgDt >= '2022-07-01'

				COMMIT TRAN	
					  
				BEGIN TRAN
					INSERT INTO PediPERForm (
					            tgt.CaseNumber, 
								tgt.PrimeMedOtherVol, 
								tgt.PrimeVol, 
								tgt.primerbctreat, 
								tgt.ArtTempHigh, 
								tgt.LwstTemp, 
								tgt.LwstTempSrc, 
								tgt.CPBSepTemp, 
								tgt.CplegiaSystem, 
								tgt.CPlegiaBloodRatio, 
								tgt.CPlegiaCrystRatio, 
								tgt.CplegiaVol, 
								tgt.CplegiaCrysVol, 
								tgt.pHMgmt, 
								tgt.pHStatCool, 
								tgt.pHStatCoolThresh, 
								tgt.pHStatWarm, 
								tgt.VenDrainAug, 
								tgt.VenDrainAugLoc, 
								tgt.VenDrainAugMax, 
								tgt.AutoCirc, 
								tgt.AutoCircPrimeVol, 
								tgt.AutoHarv, 
								tgt.AutoHarvVol, 
								tgt.AutoHarvPreCPBVol, 
								tgt.AutoHarvCPBVol, 
								tgt.AutoHarvPostCPBVol, 
								tgt.ChestTubeOutLT24, 
								tgt.ResidVolProcess, 
								tgt.ResidProcessReturn, 
								tgt.BloodProdUsed, 
								tgt.CPBCryoVol, 
								tgt.CPBFFPVol, 
								tgt.CPBPlatVol, 
								tgt.CPBRBCVol, 
								tgt.CPBWholeBloodVol, 
								tgt.NonCPBCryoVol, 
								tgt.NonCPBFFPVol, 
								tgt.NonCPBPlatVol, 
								tgt.NonCPBRBCVol, 
								tgt.NonCPBWholeBloodVol, 
								tgt.PrimeCryoVol, 
								tgt.PrimeFFPVol, 
								tgt.PrimePlatVol, 
								tgt.PrimeRBCVol, 
								tgt.PrimeWholeBloodVol, 
								tgt.CPBUrineVol, 
								tgt.ModUltraFilt, 
								tgt.ModUltraFiltTm, 
								tgt.ModUltraFiltTy, 
								tgt.ModUltraFiltVolRem, 
								tgt.UltraFilt, 
								tgt.ZeroBalUltraFilt, 
								tgt.UltraFiltVol, 
								tgt.ResidPumpVol, 
								tgt.IrrigateSolVol, 
								tgt.WallWasteVol, 
								tgt.ATSCollVol, 
								tgt.ATSRetVol, 
								tgt.CrysVol, 
								tgt.CollVol, 
								tgt.MedVolonCPB, 
								tgt.FluidBal, 
								tgt.ACTBase, 
								tgt.ACTMaxCPB, 
								tgt.ACTMinCPB, 
								tgt.ACTPostHeparin, 
								tgt.ACTPostProt, 
								tgt.CreatFirstICU, 
								tgt.CreatLst, 
								tgt.CreatMax48, 
								tgt.HctBase, 
								tgt.HctFirst, 
								tgt.LwstHct, 
								tgt.HctFirstICU, 
								tgt.HctLast, 
								tgt.HctLastPreCPB, 
								tgt.HctPostPro, 
								tgt.HepConMeasured, 
								tgt.LactateFirstOR, 
								tgt.LactateLastPreCPB, 
								tgt.LactateFirstOnCPB, 
								tgt.LactateLastOnCPB, 
								tgt.LactatePostPro, 
								tgt.LactateMax24, 
								tgt.ArtLineTy, 
								tgt.ArtLineSz, 
								tgt.ArtPoreSz, 
								tgt.ArtPumpTy, 
								tgt.VenLineSz, 
								tgt.ATSUsed, 
								tgt.BioCoat, 
								tgt.BioType, 
								tgt.HemoConTy, 
								tgt.OxygenatorTy, 
								tgt.PumpBootSz, 
								tgt.VenReservoirTy, 
								tgt.ZBUFVol, 
								tgt.Reop, 
								tgt.PrimeMedUsed, 
								tgt.AsstPerfUsed, 
								tgt.StudentPerfUsed, 
								tgt.IntraOpDeath, 
								CreateDate, 
								LastUpdate, 
								UpdateBy)
					SELECT 
					        STG.CaseNumber, 
							STG.PrimeMedOtherVol, 
							STG.PrimeVol, 
							stg.primerbctreat, 
							STG.ArtTempHigh, 
							STG.LwstTemp, 
							STG.LwstTempSrc, 
							STG.CPBSepTemp, 
							case when coalesce(STG.CPLEGIAVOL,0) = 0 then 0 else STG.CPLEGIASYSTEM end, 
							STG.CPlegiaBloodRatio, 
							STG.CPlegiaCrystRatio, 
							STG.CplegiaVol, 
							STG.CplegiaCrysVol, 
							STG.pHMgmt, 
							STG.pHStatCool, 
							STG.pHStatCoolThresh, 
							STG.pHStatWarm, 
							STG.VenDrainAug, 
							STG.VenDrainAugLoc, 
							STG.VenDrainAugMax, 
							STG.AutoCirc, 
							STG.AutoCircPrimeVol, 
							STG.AutoHarv, 
							STG.AutoHarvVol, 
							STG.AutoHarvPreCPBVol, 
							STG.AutoHarvCPBVol, 
							STG.AutoHarvPostCPBVol, 
							STG.ChestTubeOutLT24, 
							STG.ResidVolProcess, 
							STG.ResidProcessReturn, 
							STG.BloodProdUsed, 
							STG.CPBCryoVol, 
							STG.CPBFFPVol, 
							STG.CPBPlatVol, 
							STG.CPBRBCVol, 
							STG.CPBWholeBloodVol, 
							STG.NonCPBCryoVol, 
							STG.NonCPBFFPVol, 
							STG.NonCPBPlatVol, 
							STG.NonCPBRBCVol, 
							STG.NonCPBWholeBloodVol, 
							STG.PrimeCryoVol, 
							STG.PrimeFFPVol, 
							STG.PrimePlatVol, 
							STG.PrimeRBCVol, 
							STG.PrimeWholeBloodVol, 
							STG.CPBUrineVol, 
							STG.ModUltraFilt, 
							STG.ModUltraFiltTm, 
							STG.ModUltraFiltTy, 
							STG.ModUltraFiltVolRem, 
							STG.UltraFilt, 
							STG.ZeroBalUltraFilt, 
							STG.UltraFiltVol, 
							STG.ResidPumpVol, 
							STG.IrrigateSolVol, 
							STG.WallWasteVol, 
							STG.ATSCollVol, 
							STG.ATSRetVol, 
							STG.CrysVol, 
							STG.CollVol, 
							STG.MedVolonCPB, 
							STG.FluidBal, 
							STG.ACTBase, 
							STG.ACTMaxCPB, 
							STG.ACTMinCPB, 
							STG.ACTPostHeparin, 
							STG.ACTPostProt, 
							STG.CreatFirstICU, 
							case when coalesce(STG.CreatLst,0) = 0 then 0 else STG.CreatLst end,
							STG.CreatMax48, 
							STG.HctBase, 
							STG.HctFirst, 
							STG.LwstHct, 
							STG.HctFirstICU, 
							STG.HctLast, 
							STG.HctLastPreCPB, 
							STG.HctPostPro, 
							STG.HepConMeasured, 
							STG.LactateFirstOR, 
							STG.LactateLastPreCPB, 
							STG.LactateFirstOnCPB, 
							STG.LactateLastOnCPB, 
							STG.LactatePostPro, 
							STG.LactateMax24, 
							STG.ArtLineTy, 
							STG.ArtLineSz, 
							STG.ArtPoreSz, 
							STG.ArtPumpTy, 
							STG.VenLineSz, 
							STG.ATSUsed, 
							STG.BioCoat, 
							STG.BioType, 
							STG.HemoConTy, 
							STG.OxygenatorTy, 
							STG.PumpBootSz, 
							STG.VenReservoirTy, 
							STG.ZBUFVol, 
							STG.Reop, 
							STG.PrimeMedUsed, 
							STG.AsstPerfUsed, 
							STG.StudentPerfUsed, 
							STG.IntraOpDeath, 
							getdate(), 
							getdate(),'CHOP_AUTOMATION'
			         FROM v_load_chop_PediPERForm STG 
					      left join cases on stg.casenumber = cases.CaseNumber
					      LEFT JOIN PediPERForm TGT ON STG.CaseNumber = TGT.CaseNumber					    
					WHERE tgt.casenumber is null and cases.casenumber is not null and cases.SurgDt >= '2022-07-01'
					
 
                COMMIT TRAN



				BEGIN TRAN
					UPDATE TARGET_TBL
					SET
					TARGET_TBL.CaseNumber=STG_TBL.CaseNumber, 
					TARGET_TBL.ArtCanTyp=STG_TBL.ArtCanTyp, 
					TARGET_TBL.ArtCanSz=STG_TBL.ArtCanSz, 
					TARGET_TBL.SortNum=STG_TBL.SortNum
              --select *
			        FROM PediPERForm_ArtCannula TARGET_TBL 
		   			inner join pediperform on TARGET_TBL.casenumber = pediperform.CaseNumber
					INNER JOIN v_load_chop_PediPERForm_ArtCannula STG_TBL on TARGET_TBL.CaseNumber = STG_TBL.CaseNumber and TARGET_TBL.SortNum = STG_TBL.SortNum

				COMMIT TRAN	
					  
				BEGIN TRAN
					INSERT INTO PediPERForm_ArtCannula (tgt.CaseNumber, 
							tgt.ArtCanTyp, 
							tgt.ArtCanSz, 
							tgt.SortNum)
					SELECT STG.CaseNumber, 
							STG.ArtCanTyp, 
							STG.ArtCanSz, 
							STG.SortNum
					FROM v_load_chop_PediPERForm_ArtCannula STG 
					 left join cases on stg.casenumber = cases.CaseNumber
					LEFT JOIN PediPERForm_ArtCannula TGT ON STG.CaseNumber = TGT.CaseNumber and STG.SortNum = TGT.SortNum
					WHERE tgt.SortNum is null and cases.SurgDt >= '2022-07-01'

				COMMIT TRAN

              

				BEGIN TRAN
					UPDATE TARGET_TBL
					SET
					TARGET_TBL.CaseNumber=STG_TBL.CaseNumber, 
					TARGET_TBL.PrimeMedName=STG_TBL.PrimeMedName, 
					TARGET_TBL.PrimeMedDose=STG_TBL.PrimeMedDose, 
					TARGET_TBL.PrimeMedVol=STG_TBL.PrimeMedVol, 
					TARGET_TBL.SortNum=STG_TBL.SortNum, 

					target_tbl.LastUpdate=getdate(), 
					target_tbl.UpdateBy='CHOP_AUTOMATION'
               --select * 
			        FROM PediPERForm_Medications TARGET_TBL 
					INNER JOIN v_load_chop_PediPERForm_Medications STG_TBL on TARGET_TBL.CaseNumber = STG_TBL.CaseNumber and TARGET_TBL.SortNum = STG_TBL.SortNum

				COMMIT TRAN	
					  
				BEGIN TRAN
					INSERT INTO PediPERForm_Medications (tgt.CaseNumber, 
							tgt.PrimeMedName, 
							tgt.PrimeMedDose, 
							tgt.PrimeMedVol, 
							tgt.SortNum,tgt.CreateDate, 
							tgt.LastUpdate, 
							tgt.UpdateBy)
					 SELECT STG.CaseNumber, 
							STG.PrimeMedName, 
							STG.PrimeMedDose, 
							STG.PrimeMedVol, 
							STG.SortNum, 
							getdate(), 
							getdate(),'CHOP_AUTOMATION'
					FROM v_load_chop_PediPERForm_Medications STG 
					left join cases on stg.casenumber = cases.CaseNumber
					LEFT JOIN PediPERForm_Medications TGT ON STG.CaseNumber = TGT.CaseNumber and STG.SortNum = TGT.SortNum
					WHERE tgt.SortNum is null and cases.SurgDt >= '2022-07-01'

				COMMIT TRAN


				BEGIN TRAN
					UPDATE TARGET_TBL
					SET
					TARGET_TBL.CaseNumber=STG_TBL.CaseNumber, 
					TARGET_TBL.PrimeFluid=STG_TBL.PrimeFluid, 
					TARGET_TBL.PrimeFluidVol=STG_TBL.PrimeFluidVol, 
					TARGET_TBL.SortNum=STG_TBL.SortNum, 

					target_tbl.UpdateDate=getdate(), 
					target_tbl.UpdateBy='CHOP_AUTOMATION'
              --select *
			        FROM PediPERForm_PrimeFluids TARGET_TBL 
		   			inner join pediperform on TARGET_TBL.casenumber = pediperform.CaseNumber
					INNER JOIN v_load_chop_PediPERForm_PrimeFluids STG_TBL on TARGET_TBL.CaseNumber = STG_TBL.CaseNumber and TARGET_TBL.SortNum = STG_TBL.SortNum

				COMMIT TRAN	
					  
				BEGIN TRAN
					INSERT INTO PediPERForm_PrimeFluids (tgt.CaseNumber, 
							tgt.PrimeFluid, 
							tgt.PrimeFluidVol, 
							tgt.SortNum,tgt.CreateDate, 
							tgt.updateDate, 
							tgt.UpdateBy)
					SELECT STG.CaseNumber, 
							STG.PrimeFluid, 
							STG.PrimeFluidVol, 
							STG.SortNum, 
							getdate(), 
							getdate(),'CHOP_AUTOMATION'
					FROM v_load_chop_PediPERForm_PrimeFluids STG
					left join cases on stg.casenumber = cases.CaseNumber
					LEFT JOIN PediPERForm_PrimeFluids TGT ON STG.CaseNumber = TGT.CaseNumber and STG.SortNum = TGT.SortNum
					WHERE tgt.SortNum is null and cases.SurgDt >= '2022-07-01'

				COMMIT TRAN


				BEGIN TRAN
					UPDATE TARGET_TBL
					SET
						 TARGET_TBL.CaseNumber=STG_TBL.CaseNumber, 
						TARGET_TBL.VenCanTyp=STG_TBL.VenCanTyp, 
						TARGET_TBL.VenCanStyle=STG_TBL.VenCanStyle, 
						TARGET_TBL.VenCanSz=STG_TBL.VenCanSz, 
						TARGET_TBL.SortNum=STG_TBL.SortNum
               --select *
			        FROM PediPERForm_VenCannula TARGET_TBL 
		   			inner join pediperform on TARGET_TBL.casenumber = pediperform.CaseNumber
					INNER JOIN v_load_chop_PediPERForm_VenCannula STG_TBL on TARGET_TBL.CaseNumber = STG_TBL.CaseNumber and TARGET_TBL.SortNum = STG_TBL.SortNum

				COMMIT TRAN	
					  
				BEGIN TRAN
					INSERT INTO PediPERForm_VenCannula (tgt.CaseNumber, 
							tgt.VenCanTyp, 
							tgt.VenCanStyle, 
							tgt.VenCanSz, 
							tgt.SortNum)
					SELECT STG.CaseNumber, 
							STG.VenCanTyp, 
							STG.VenCanStyle, 
							STG.VenCanSz, 
							STG.SortNum
					FROM v_load_chop_PediPERForm_VenCannula STG 
					left join cases on stg.casenumber = cases.CaseNumber
					LEFT JOIN PediPERForm_VenCannula TGT ON STG.CaseNumber = TGT.CaseNumber and STG.SortNum = TGT.SortNum
					WHERE tgt.SortNum is null and cases.SurgDt >= '2022-07-01'

				COMMIT TRAN

              BEGIN TRAN
				
				INSERT INTO @PediperformValidationList(CaseNumber, 

							DB, 
							DBVrsn, 
							RowID)
				select PediPERForm.CaseNumber, 
							@DB, 
							@DBVrsn, 
						row_number() over (partition by 1 order by surgdt)
				from  PediPERForm inner join cases on PediPERForm.CaseNumber = cases.CaseNumber 
				and surgdt > '2022-07-01'

				SELECT @numrows = @@RowCount, @i = 1
				WHILE (@i <= @numrows) 
					   BEGIN
							  SELECT 
							    @EventID = CaseNumber, 
								@DBVrsn = DBVrsn, 
								@DB = DB FROM @PediperformValidationList WHERE RowID = @i

							  EXEC Validation_Call_ByTableEventID 'Perfusion',@EventID,@DB,@DBVrsn;
							  EXEC Validation_Call_ByTableEventID 'PediPERForm',@EventID,@DB,@DBVrsn;
							  EXEC Validation_Call_OneToManyDetail 'PediPERForm_ArtCannula',@EventID,@DB,@DBVrsn;
							  EXEC Validation_Call_OneToManyDetail 'PediPERForm_VenCannula',@EventID,@DB,@DBVrsn;
							  EXEC Validation_Call_OneToManyDetail 'PediPERForm_Medications',@EventID,@DB,@DBVrsn;
							  EXEC Validation_Call_OneToManyDetail 'PediPERForm_PrimeFluids',@EventID,@DB,@DBVrsn;

						SET @i = @i + 1     
					 END

					
				COMMIT TRAN
				
          END
