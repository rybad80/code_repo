	DROP TABLE CHOP_IMPACT_CATHPROCEDURES;
	DROP TABLE CHOP_IMPACT_CATHDIAGNOSIS;
	
	CREATE TABLE CHOP_IMPACT_CATHPROCEDURES 
	 (SURG_ENC_ID  NVARCHAR(50)      NOT NULL,
	SPECIFICPROCID            INT     NOT NULL,
	LOADDT      DATETIME  NOT NULL, 
    MD5     VARCHAR(50)     NOT NULL, 
    PENDINGIMPORT     INT     NOT NULL);
    
	CREATE TABLE CHOP_IMPACT_CATHDIAGNOSIS 
	 (SURG_ENC_ID  NVARCHAR(50)      NOT NULL,
	PREPROCCARDDIAGID            INT      NOT NULL,
	DIAGNOSISNAME  NVARCHAR(255)      NULL,
	LOADDT      DATETIME  NOT NULL, 
    MD5     VARCHAR(50)     NOT NULL, 
    PENDINGIMPORT     INT     NOT NULL)   ; 


ALTER TABLE CHOP_IMPACT_CATHPROCEDURES ADD CONSTRAINT PK_CHOP_IMPACT_CATHPROCEDURES PRIMARY KEY (SURG_ENC_ID,SPECIFICPROCID);
ALTER TABLE CHOP_IMPACT_CATHDIAGNOSIS ADD CONSTRAINT PK_CHOP_IMPACT_CATHDIAGNOSIS PRIMARY KEY (SURG_ENC_ID,PREPROCCARDDIAGID);
