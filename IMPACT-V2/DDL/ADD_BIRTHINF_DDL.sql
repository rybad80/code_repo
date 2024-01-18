select * INTO
CHOP_IMPACT_CATHPATANATOMY_bkp
from CHOP_IMPACT_CATHPATANATOMY;

--SELECT * FROM CHOP_IMPACT_CATHPATANATOMY_bkp;

--drop table CHOP_IMPACT_CATHPATANATOMY;



SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CHOP_IMPACT_CATHPATANATOMY](
	[PAT_MRN_ID] [nvarchar](10) NOT NULL,
	[DIGEORGESYND] [int] NULL,
	[ALAGILLESYND] [int] NULL,
	[HERNIA] [int] NULL,
	[MARFANSYND] [int] NULL,
	[DOWNSYND] [int] NULL,
	[HETEROTAXY] [int] NULL,
	[NOONANSYND] [int] NULL,
	[RUBELLA] [int] NULL,
	[TRISOMY13] [int] NULL,
	[TRISOMY18] [int] NULL,
	[TURNERSYND] [int] NULL,
	[WILLIAMSBEURENSYND] [int] NULL,
	[PREMATURE] [int] NULL,
	[GESTAGEWEEKS] [int] NULL,
    [BIRTHWTKG] [decimal](5,3) NULL,
	[LOADDT] [datetime] NOT NULL,
	[MD5] [varchar](50) NOT NULL,
	[PENDINGIMPORT] [int] NOT NULL,
 CONSTRAINT [PK_CHOP_IMPACT_CATHPATANATOMY] PRIMARY KEY CLUSTERED 
(
	[PAT_MRN_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
;




INSERT INTO [dbo].[CHOP_IMPACT_CATHPATANATOMY]
           ([PAT_MRN_ID]
           ,[DIGEORGESYND]
           ,[ALAGILLESYND]
           ,[HERNIA]
           ,[MARFANSYND]
           ,[DOWNSYND]
           ,[HETEROTAXY]
           ,[NOONANSYND]
           ,[RUBELLA]
           ,[TRISOMY13]
           ,[TRISOMY18]
           ,[TURNERSYND]
           ,[WILLIAMSBEURENSYND]
           ,[LOADDT]
           ,[MD5]
           ,[PENDINGIMPORT])
SELECT 
       [PAT_MRN_ID]
           ,[DIGEORGESYND]
           ,[ALAGILLESYND]
           ,[HERNIA]
           ,[MARFANSYND]
           ,[DOWNSYND]
           ,[HETEROTAXY]
           ,[NOONANSYND]
           ,[RUBELLA]
           ,[TRISOMY13]
           ,[TRISOMY18]
           ,[TURNERSYND]
           ,[WILLIAMSBEURENSYND]
           ,[LOADDT]
           ,[MD5]
           ,[PENDINGIMPORT]
FROM 
    CHOP_IMPACT_CATHPATANATOMY_bkp


--drop table CHOP_IMPACT_CATHPATANATOMY_bkp;