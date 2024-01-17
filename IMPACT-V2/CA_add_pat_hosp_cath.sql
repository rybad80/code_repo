select * from cathdata WHERE HOSPID = 46665 AUX5 = '19-0687'

SELECT * FROM Demographics WHERE MEDRECN = '56367377'

insert into demographics (HospitalID,medrecn,patLname,patFName,dob,CreateDate,UpdatedBy)
select 4	,'56367377',	'Gomez-Celestino',	'Minerva',	'2019-02-22 00:00:00',	GETDATE(),	GETDATE()

SELECT PATID FROM demographics WHERE MEDRECN = '56367377'

insert into hospitalization (PatID,HospitalID,IMPACTDataVrsn, CreateDate,UpdateBy)
select 27797, 4, 2.0, getdate(), getdate()

SELECT * FROM hospitalization WHERE PATID = 29191

insert into CathData (patid, hospid, emreventid, Aux5)
select 27797,46665 , 2061456723,  19-0737

select * from CathHistory where hospitalizationid = 48581


SELECT * FROM CATHDATA WHERE AUX5 = '19-0687'
SELECT * FROM CHOP_IMPACT_CATHDATA WHERE surg_enc_id in (2061456723)

[

