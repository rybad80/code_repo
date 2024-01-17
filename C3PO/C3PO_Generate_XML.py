import pandas as pd
import pyodbc as pyo
import os
import numpy as np
import csv
import datetime
from lxml import etree as et
import math
from IPython.display import display


# #############################################################################
# pull env variables
# #############################################################################

if "DSN" in os.environ:
    dsn = f'DSN={ os.environ["DSN"] }'
else:
    dsn = 'DSN=CDWPRD'

if "NZ_USER" in os.environ:
    uid = os.environ["NZ_USER"]
    pwd = os.environ["NZ_PASSWORD"]
    cnxn = pyo.connect(dsn,uid=uid,pwd=pwd)
else:
    cnxn = pyo.connect(dsn)
    
output_path = r"\\chop.edu\departmentshare\DS4\CARDIO\People\SHARED\C3PO"    
    
    
    
# #############################################################################
# read in sql file
# #############################################################################
with open("C3PO_SENSIS_QUERY.sql", 'r') as f:
    sql = f.read()


# #############################################################################
# execute sql and load into dataframe
# #############################################################################
chunksize=50
recs_chunk = pd.read_sql(sql, cnxn,chunksize=chunksize)
#recs = pd.read_sql(sql, cnxn)


# #############################################################################
# assemble XML
# #############################################################################

#for recs_chunk in recs:
chunk_num=1
for chunk in recs_chunk:
    root = et.Element('BCHCPOR3DataImport')
    for row in chunk.iterrows():

        ## Create XML structure
        Patient = et.SubElement(root, 'Patient')

        CaseInfo = et.SubElement(Patient, 'CaseInfo')
        HospitalID = et.SubElement(CaseInfo, 'HospitalID')
        CathDate = et.SubElement(CaseInfo, 'CathDate')
        Operator = et.SubElement(CaseInfo, 'Operator')
        SensisRefNum = et.SubElement(CaseInfo, 'SensisRefNum')

        CaseClinicalInfo = et.SubElement(Patient, 'CaseClinicalInfo')
        PatientAge = et.SubElement(CaseClinicalInfo, 'PatientAge')
        PatientAgeType = et.SubElement(CaseClinicalInfo, 'PatientAgeType')
        PatientSex = et.SubElement(CaseClinicalInfo, 'PatientSex')
        PatientWeight = et.SubElement(CaseClinicalInfo, 'PatientWeight')
        PatientHeight = et.SubElement(CaseClinicalInfo, 'PatientHeight')
        PatientBSA = et.SubElement(CaseClinicalInfo, 'PatientBSA')
        STSDiagCode = et.SubElement(CaseClinicalInfo, 'STSDiagCode')
        PrevCathlast90dInd = et.SubElement(CaseClinicalInfo, 'PrevCathlast90dInd')
        PrevSurglast90dInd = et.SubElement(CaseClinicalInfo, 'PrevSurglast90dInd')
        GenSyndromeInd = et.SubElement(CaseClinicalInfo, 'GenSyndromeInd')
        NonCardiacProbInd = et.SubElement(CaseClinicalInfo, 'NonCardiacProbInd')
        NonCardiacProbValues = et.SubElement(CaseClinicalInfo, 'NonCardiacProbValues')

        CaseProcedureInfo = et.SubElement(Patient, 'CaseProcedureInfo')
        FluroTime = et.SubElement(CaseProcedureInfo, 'FluroTime')
        TotalDap = et.SubElement(CaseProcedureInfo, 'TotalDap')
        SheathCathInDateTime = et.SubElement(CaseProcedureInfo, 'SheathCathInDateTime')
        SheathCathOutDateTime = et.SubElement(CaseProcedureInfo, 'SheathCathOutDateTime')
        BloodTransfusion = et.SubElement(CaseProcedureInfo, 'BloodTransfusion')

        CaseEOCAdmDisposition = et.SubElement(Patient, 'CaseEOCAdmDisposition')
        AdmissionSource = et.SubElement(CaseEOCAdmDisposition, 'AdmissionSource')
        PostCathLocation = et.SubElement(CaseEOCAdmDisposition, 'PostCathLocation')
        IsUnplannedAdmission = et.SubElement(CaseEOCAdmDisposition, 'IsUnplannedAdmission')
        AdmitGreaterThan48HrsPriorToCath = et.SubElement(CaseEOCAdmDisposition, 'AdmitGreaterThan48HrsPriorToCath')
        DischargeGreaterThan48HrsPostCath = et.SubElement(CaseEOCAdmDisposition, 'DischargeGreaterThan48HrsPostCath')
        deathlessthan72hrspostcath = et.SubElement(CaseEOCAdmDisposition, 'deathlessthan72hrspostcath')
        #IsAliveAtDischarge = et.SubElement(CaseEOCAdmDisposition, 'IsAliveAtDischarge')

        Hemodynamics = et.SubElement(Patient, 'Hemodynamics')
        SingleVentriclePhysiology = et.SubElement(Hemodynamics, 'SingleVentriclePhysiology')
        SVEDPGreaterThanOrEqualTo18mmHg = et.SubElement(Hemodynamics, 'SVEDPGreaterThanOrEqualTo18mmHg')
        MVSatLessThan60Percent = et.SubElement(Hemodynamics, 'MVSatLessThan60Percent')
        QpQsGreaterThan1Point5 = et.SubElement(Hemodynamics, 'QpQsGreaterThan1Point5')
        SysSatLessThan95Percent = et.SubElement(Hemodynamics, 'SysSatLessThan95Percent')
        PASysGreaterThanOrEqualTo45mmHg = et.SubElement(Hemodynamics, 'PASysGreaterThanOrEqualTo45mmHg')
        PVRGreaterThan3WU = et.SubElement(Hemodynamics, 'PVRGreaterThan3WU')
        SysSatLessThan78Percent = et.SubElement(Hemodynamics, 'SysSatLessThan78Percent')
        PAMeanLessThanOrEqualTo17mmHg = et.SubElement(Hemodynamics, 'PAMeanLessThanOrEqualTo17mmHg')
        MVSatLessThan50Percent = et.SubElement(Hemodynamics, 'MVSatLessThan50Percent')    

        AdverseEvents = et.SubElement(Patient, 'AdverseEvents')
        MajorAdverseEvent = et.SubElement(AdverseEvents,'MajorAdverseEvent')
        AeCode = et.SubElement(MajorAdverseEvent, 'AeCode')
        AeSeriousness = et.SubElement(MajorAdverseEvent, 'AeSeriousness')

        OtherAdverseEvent = et.SubElement(AdverseEvents, 'OtherAdverseEvent')
        oAeCode = et.SubElement(OtherAdverseEvent, 'AeCode')
        oAeSeriousness = et.SubElement(OtherAdverseEvent, 'AeSeriousness')
        
        AENotes = et.SubElement(AdverseEvents, 'AENotes')

        RequiredResources = et.SubElement(Patient, 'RequiredResources')


        ## Map data from Dataframe columns to XML Element
        HospitalID.text = str(row[1]['HOSPITALID'])
        CathDate.text = str(row[1]['CATHDATE'])
        Operator.text = str(row[1]['OPERATOR'])
        
        SensisRefNum.text = str(row[1]['SENSISREFNUM'])

        PatientAge.text = str(row[1]['PATIENTAGE'])
        PatientAgeType.text = str(row[1]['PATIENTAGETYPE'])
        PatientSex.text = str(row[1]['PATIENTSEX'])
        PatientWeight.text = str(row[1]['PATIENTWEIGHT'])
        PatientHeight.text = str(row[1]['PATIENTHEIGHT'])
        PatientBSA.text = str(row[1]['PATIENTBSA'])
        STSDiagCode.text = str(row[1]['STSDIAGCODE'])
        PrevCathlast90dInd.text = str(row[1]['PREVCATHLAST90DIND'])
        PrevSurglast90dInd.text = str(row[1]['PREVSURGLAST90DIND'])
        GenSyndromeInd.text = str(row[1]['GENSYNDROMEIND'])
        NonCardiacProbInd.text = str(row[1]['NONCARDIACPROBIND'])
        NonCardiacProbValues.text = str(row[1]['NONCARDIACPROBVALUES']) 

        FluroTime.text = str(row[1]['FLUROTIME']) 
        TotalDap.text = str(row[1]['TOTALDAP']) 
        SheathCathInDateTime.text = str(row[1]['SHEATHCATHINDATETIME']) 
        SheathCathOutDateTime.text = str(row[1]['SHEATHCATHOUTDATETIME']) 
        BloodTransfusion.text = str(row[1]['BLOODTRANSFUSION']) 

        AdmissionSource.text = str(row[1]['ADMISSIONSOURCE']) 
        PostCathLocation.text = str(row[1]['POSTCATHLOCATION']) 
        IsUnplannedAdmission.text = str(row[1]['ISUNPLANNEDADMISSION']) 
        AdmitGreaterThan48HrsPriorToCath.text = str(row[1]['ADMITGREATERTHAN48HRSPRIORTOCATH']) 
        DischargeGreaterThan48HrsPostCath.text = str(row[1]['DISCHARGEGREATERTHAN48HRSPOSTCATH']) 
        deathlessthan72hrspostcath.text = str(row[1]['DEATHLESSTHAN72HRSPOSTCATH'])
        #IsAliveAtDischarge.text = str(row[1]['ISALIVEATDISCHARGE']) 

        SingleVentriclePhysiology.text = str(row[1]['SINGLEVENTRICLEPHYSIOLOGY'])
        SVEDPGreaterThanOrEqualTo18mmHg.text = str(row[1]['SVEDPGREATERTHANOREQUALTO18MMHG'])
        MVSatLessThan60Percent.text = str(row[1]['MVSATLESSTHAN60PERCENT'])
        QpQsGreaterThan1Point5.text = str(row[1]['QPQSGREATERTHAN1POINT5'])
        SysSatLessThan95Percent.text = str(row[1]['SYSSATLESSTHAN95PERCENT'])
        PASysGreaterThanOrEqualTo45mmHg.text = str(row[1]['PASYSGREATERTHANOREQUALTO45MMHG'])
        PVRGreaterThan3WU.text = str(row[1]['PVRGREATERTHAN3WU'])
        SysSatLessThan78Percent.text = str(row[1]['SYSSATLESSTHAN78PERCENT'])
        PAMeanLessThanOrEqualTo17mmHg.text = str(row[1]['PAMEANLESSTHANOREQUALTO17MMHG'])
        MVSatLessThan50Percent.text = str(row[1]['MVSATLESSTHAN50PERCENT']) 

        AeCode.text = str(row[1]['MAJ_AECODE'])
        AeSeriousness.text = str(row[1]['MAJ_AESERIOUSNESS'])   

        oAeCode.text = str(row[1]['OTH_AECODE'])
        oAeSeriousness.text = str(row[1]['OTH_AESERIOUSNESS'])
        
        AENotes.text = str(row[1]['AENOTES'])

        RequiredResources = et.SubElement(Patient, 'RequiredResources')
        
# #############################################################################
# write XML file to folder
# #############################################################################

        xml_output = et.tostring(root, pretty_print=True).decode('utf-8')
      

    file_name = f'C3PO Import File-{str(datetime.datetime.now())[0:10]}_{chunk_num}.xml'
    full_path = os.path.join(output_path, file_name)
    file_write = open(full_path,"w")
    file_write.write(xml_output)
    file_write.close()

    chunk_num+=1  

