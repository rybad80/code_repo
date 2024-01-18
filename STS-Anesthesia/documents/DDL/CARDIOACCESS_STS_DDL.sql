CREATE TABLE [dbo].[CHOP_STS_CASES](
	CASENUMBER INT PRIMARY KEY,
	LOG_ID INT,
	PAT_MRN_ID VARCHAR(100),
	SURGERY_DATE DATETIME,
	IN_ROOM NVARCHAR(25),
	PROC_START_INCISION NVARCHAR(25),
	PROC_CLOSE_INCISION NVARCHAR(25),
	OUT_ROOM NVARCHAR(25)
) 