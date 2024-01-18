import pandas as pd
import pyodbc
import os
import time
import json
import math
from datetime import date
from datetime import datetime


########################################
## define functions - for local dev, add DSN to your ODBC (ex. "SENSIS")
########################################


def nzsql_to_mssql_ddl(table_name,src_system):
#function to compile CREATE TABLE statement and convert mssql to nzsql data types    
    if   src_system == 'SENSIS':
        conn_str = pyodbc.connect(r'DSN=Sensis-CATSENPW1;UID=local_reader;PWD=_______')
    elif src_system == 'JOCAPS':
        conn_str = 'Driver={NetezzaSQL};servername=CHOPDW;port=5480;database=CDWPRD;username=rybad;password=_______;'        
    elif src_system == 'SYNGOECHO':
        conn_str = pyodbc.connect(r'Syngo Echo - AcusonDB;CCIS_Data_Access=local_reader;PWD=_______')
    elif src_system == 'MUSE':
        conn_str = pyodbc.connect(r'DSN=Muse;UID=CCIS_Data_Access;PWD=_______') 
    elif src_system == 'CENTRIPETUS':
        conn_str = pyodbc.connect(r'DSN=Centripetus;UID=CARDIO;PWD=_______')
    elif src_system == 'CENTRIPETUS_DEV':
        conn_str = pyodbc.connect(r'DSN=CENTRIPETUS-DEV;UID=CARDIO;PWD=_______')            
    elif src_system == 'CENTRIPETUS-Test':
        conn_str = pyodbc.connect(r'DSN=Centripetus-Test;UID=CARDIO;PWD=_______')
    elif src_system == 'CLINIBASE':
        conn_str = pyodbc.connect('DRIVER={SQL Server};Server=QSQLA177;DATABASE=Clinibase_Repository;UID=CCIS_Data_Access;PWD=_______') 
    elif src_system == 'HEALTHVIEW':
        conn_str = pyodbc.connect('DRIVER={SQL Server};Server=PSQLA129;DATABASE=Apollo;UID=CCIS_Data_Access;PWD=_______') 
    
    query = '''
               SELECT
                   COLUMN_NAME,
                   DATATYPE,
                   CASE WHEN COLUMN_ORDER = 1 THEN NULLABLE ELSE NULLABLE+',' END AS NULLABLE
                FROM
                    (  
                    SELECT  	
                                UPPER(c.name) COLUMN_NAME,
                                CASE WHEN typ.name in ('datetime' ,'smalldatetime')
                                      THEN 'VARCHAR(50)'
                                      WHEN typ.name ='BIT'
                                      THEN 'BOOLEAN'
                                      WHEN typ.name ='MONEY'
                                      THEN 'VARCHAR(10)'
                                      WHEN typ.name ='TINYINT'
                                      THEN 'VARCHAR(1)'                                           
                                     WHEN typ.name = 'varchar' 
                                      THEN UPPER(typ.name)+'('+cast(abs(c.max_length) as varchar(255))+')'
                                     WHEN typ.name in ('nchar','nvarchar') 
                                      THEN UPPER(typ.name)+'('+cast(abs(c.max_length/2) as varchar(255))+')'
                                   ELSE UPPER(typ.name) END AS DATATYPE,
                                CASE WHEN c.is_nullable = 0 THEN 'NOT NULL' ELSE 'NULL' END AS NULLABLE,
                                C.column_id,
                                ROW_NUMBER() OVER (PARTITION BY TBL.name ORDER BY C.COLUMN_ID DESC) COLUMN_ORDER

                    FROM    
                        sys.columns c	INNER JOIN	SYS.tables tbl ON C.object_id = TBL.object_id
                                        INNER JOIN	sys.types typ ON c.user_type_id = typ.user_type_id
                                            LEFT JOIN (select ic.object_id,ic.index_id,ic.column_id, i.is_primary_key 
                                                        from sys.index_columns ic 
                                                             inner JOIN sys.indexes i 
                                                               ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
                                                               AND is_primary_key = 1) ic
                                                  ON ic.object_id = c.object_id AND ic.column_id = c.column_id					     
                    WHERE tBL.name = '{table_name}'
                     ) A
               ORDER BY column_id
                 ''' 
    ddl_query = query.format(table_name=table_name)  

    ddl=pd.read_sql(ddl_query,conn_str)

    return ddl.to_string(index=False,header=None);



def ddl_pk(table_name):
#function to identify primary key(s) on the table  
    if src_system == 'SENSIS':
        conn_str = pyodbc.connect(r'DSN=Sensis-CATSENPW1;UID=local_reader;PWD=_______')
    elif src_system == 'SYNGOECHO':
        conn_str = pyodbc.connect(r'Syngo Echo - AcusonDB;CCIS_Data_Access=local_reader;PWD=_______')
    elif src_system == 'MUSE':
        conn_str = pyodbc.connect(r'DSN=Muse;UID=CCIS_Data_Access;PWD=_______') 
    elif src_system == 'CENTRIPETUS':
        conn_str = pyodbc.connect(r'DSN=Centripetus;UID=CARDIO;PWD=_______')
    elif src_system == 'CENTRIPETUS_DEV':
        conn_str = pyodbc.connect(r'DSN=CENTRIPETUS-DEV;UID=CARDIO;PWD=_______')            
    elif src_system == 'CLINIBASE':
        conn_str = pyodbc.connect('DRIVER={SQL Server};Server=QSQLA177;DATABASE=Clinibase_Repository;UID=CCIS_Data_Access;PWD=_______')        
    elif src_system == 'HEALTHVIEW':
        conn_str = pyodbc.connect('DRIVER={SQL Server};Server=PSQLA129;DATABASE=Apollo;UID=CCIS_Data_Access;PWD=_______')         
           
    query = '''
                SELECT distinct
                            STUFF((
                            SELECT ',' + UPPER(c1.name )
                            FROM sys.columns c1	
                            WHERE c1.OBJECT_ID=C2.OBJECT_ID  and is_nullable = 0
                            FOR XML PATH ('')
                        ), 1, 1, '')
                FROM sys.columns c2	INNER JOIN	SYS.tables tbl2 ON C2.object_id = TBL2.object_id
                WHERE tBL2.name = '{table_name}'  and c2.is_nullable = 0
                 '''
    pk_query = query.format(table_name=table_name)  

    pk=pd.read_sql(pk_query,conn_str)
    
    if pk.empty == False:
        return pk.to_string(index=False,header=None); 

    else: 
        return '';    

def run_ddl(sql, conn_str):
#function to execute DDL

    if tgt_system == 'CDW_ODS_DEV':
        conn_str = pyodbc.connect('Driver={NetezzaSQL};servername=uat.cdw.chop.edu;port=5480;database=CDW_ODS_DEV;username=rybad;password=_______')
    
    cursor = conn_str.cursor()
    cursor.execute(sql)
    conn_str.commit()
    
def get_cnxn(env='dev'):
    '''returns a pyodbc connection to cdw dev or prod'''
    if env == 'JOCAPS':
        cnxn_str = 'Driver={NetezzaSQL};servername=CHOPDW;port=5480;database=CDWPRD;username=rybad;password=_______;'
    elif env == 'SENSIS':
        cnxn_str = 'DSN=Sensis-CATSENPW1;UID=local_reader;PWD=_______;'        
    elif env == 'uat':
        cnxn_str = 'Driver={NetezzaSQL};servername=uat.cdw.chop.edu;port=5480;database=CDW_ODS_UAT;username=;password=_______;'
    elif env == 'CLINIBASE':
        cnxn_str = 'DRIVER={SQL Server};Server=QSQLA177;DATABASE=Clinibase_Repository;UID=CCIS_Data_Access;PWD=_______;'                
    elif env == 'MUSE':
        cnxn_str = 'DSN=Muse;UID=CCIS_Data_Access;PWD=_______'   
    elif env == 'CENTRIPETUS':
        cnxn_str = 'DSN=Centripetus;UID=CARDIO;PWD=_______'
    elif env == 'CENTRIPETUS_DEV':
        cnxn_str = 'DSN=CENTRIPETUS-DEV;UID=CARDIO;PWD=_______'      
    elif env == 'HEALTHVIEW':
        cnxn_str = 'DSN=HEALTHVIEW;UID=CCIS_Data_Access;PWD=_______' 
    else:
        cnxn_str = 'Driver={NetezzaSQL};servername=uat.cdw.chop.edu;port=5480;database=CDW_ODS_DEV;username=rybad;password=_______;'
    return pyodbc.connect(cnxn_str)    



def execute_sql(sql, env='dev'):
    cnxn = get_cnxn(env)
    cursor = cnxn.cursor()
    row_ct = cursor.execute(sql).rowcount
    cnxn.commit()
    return row_ct

def sql_to_df(sql, env='dev', quiet=False):
    '''executes the sql provided against cdw dev or prod'''
    cnxn = get_cnxn(env)
    df = pd.read_sql(sql, cnxn)
    return df

def df_clean_strings(df):
    '''strips all strings in df and removes all chars in bad_chars list'''
    df = df.applymap(lambda x: x.strip() if type(x) is str else x)
    for ch in BAD_CHARS:
        df = df.applymap(lambda x: x.replace(ch, '') if type(x) is str else x)
    return df

def df_round_floats(df):
    '''rounding float characters upto 5 decimals'''
    df = df.applymap(lambda x: round(x,FLOAT_DIGITS) if type(x) is float else x)
    return df

def infer_dtypes(df, as_is_cols=[]):
    """
    Infer datatypes of a pandas dataframe and their Netezza equivalent.
    Will not change the type of any column in the exclude list.
    Returns a dict of col_name -> NETEZZA data types.
    """
    data_types = {}
    netezza_types = {
        'int64': 'BIGINT',
        'float64': 'DECIMAL(18,' + str(FLOAT_DIGITS) + ')',
        'datetime64[ns]': 'DATETIME',#DATE
        'object': 'VARCHAR',
        'bool': 'BOOLEAN'}
    for col_name in df.columns:
        my_dtype = df[col_name].dtype
        if df[col_name].dtype == 'bool':
            data_types[col_name] = netezza_types['bool']
        elif df[col_name].dtype == 'object':
            data_types[col_name] = netezza_types['object']      
        elif df[col_name].dtype == 'int64':
            data_types[col_name] = netezza_types['int64']
        elif df[col_name].dtype == 'float64':
            data_types[col_name] = netezza_types['float64']
        elif df[col_name].dtype == 'datetime64[ns]':
            data_types[col_name] = 'VARCHAR'         
        else:
            data_types[col_name] = netezza_types['varchar']
    return data_types

def infer_col_lengths(df, data_types):
    """
    Gets the longest value in each columns for strings.
    Needed to specify varchar lengths.
    """
    col_lengths = {}
    for col_name in df.columns:
        if data_types[col_name] == 'VARCHAR':
            # find the longest string in the column
            if math.isnan(df[col_name].astype(str).str.len().max()):
                col_lengths[col_name]='(50)' #default to 50 if not explicitly a varchar column
            else:
                max_rec_len = max(1,int(df[col_name].astype(str).str.len().max()))
                #print(str(max_rec_len))
                #print(df[col_name])
                col_lengths[col_name] = '(' + str(max_rec_len+16) + ')'     #padding with 16 chars to be safe 
        else:
            # we dont need to do any length specs for ints, floats, datetimes
            col_lengths[col_name] = ''
    return col_lengths

def infer_col_names(df):
    """
    Just need to uppercase-ize and remove and blanks from column names.
    We also have some checks for bad columns names, will append _
    """
    headers = {}
    #key_words = ['USER', 'HOURS', 'TIME', 'POSITION', 'COMMENT', 'ORDER', 'SHOW']
    for col_name in df.columns:
        header = col_name.upper()
        header = header.replace(' ', '_')
        header = header.replace('.', '_')
        header = header.replace('/', '_per_')
        #if header in key_words:
            #header = header + '_'
        headers[col_name] = header
    return headers

def drop_table_sql(table_name):
    '''
    returns the sql needed to drop a table if it exists in netezza
    '''
    return 'drop table ' + table_name + ' if exists;'

def nzload_sql(table_name, load_file, log_dir, header_dict, dtype_dict,
               longest_dict):
    '''
    creates the nzload sql for loading to cdw
    '''
    nz_sql = "create table " + table_name + " as \n"            + "select * from external '" + load_file + "' ("
    for col_name in header_dict:
        nz_sql = nz_sql + "\n " + str(header_dict[col_name]) + " "                + str(dtype_dict[col_name]) + str(longest_dict[col_name]) + ","
    # remove last character to handle dangling sql comma
    nz_sql = nz_sql[:-1] + ")" + '''
    USING (
      REMOTESOURCE 'ODBC'
      DELIMITER ','
      SKIPROWS 1
      MAXERRORS 1
      ENCODING 'internal'
      LOGDIR '__logdir__'
      DATESTYLE YMD
      DATEDELIM '-'
      TIMESTYLE '24HOUR'
      TIMEDELIM ':'
      QUOTEDVALUE Double
      BoolStyle TRUE_FALSE
    );
'''
    nz_sql = nz_sql.replace('__logdir__', log_dir)
    #print(nz_sql)
    return nz_sql

def df_to_cdw(df, table_name, prod_or_dev='dev', grant_access=True,
              as_is_cols=[], hist_sched='ad hoc', hist_type='unknown',
              hist_source_id=-999, quiet=False):
    '''creates a table in CDW via NZ Load with the contents of df
    This performs a full drop and reload, and thus will replace any data that
    already exists. It creates a csv in the current working directory for
    loading. The load log is also created:
    ../cwd/load_files/__table_name__.csv
    ../cwd/load_logs/
    This function performs some simple cleaning a dtype inference.
    grant_access: by default, access will be granted to all users of the 
    new table.
    If your date columns are not already formatted as dates, they will be
    created as VARCHAR in CDW. Use something like this to format them first:
    df[col] = pd.to_datetime(df[col])
    '''
    starting_rows = len(df)
    if starting_rows == 0:
        print('Warning: No records in DF, no table created')
        return df
    df = df_clean_strings(df)
    df = df_round_floats(df)
    data_types = infer_dtypes(df, as_is_cols=as_is_cols)
    col_lengths = infer_col_lengths(df, data_types)
    headers = infer_col_names(df)
    if not os.path.exists(LOAD_FILE_PATH):
        os.makedirs(LOAD_FILE_PATH)
    load_file = os.path.join(LOAD_FILE_PATH, table_name + '.csv')
    df.to_csv(os.path.join(load_file), index=False)
    if not os.path.exists(LOG_FILE_PATH):
        os.makedirs(LOG_FILE_PATH)
    drop_sql = drop_table_sql(table_name)
    execute_sql(drop_sql, prod_or_dev)
    nz_sql = nzload_sql(table_name, load_file, LOG_FILE_PATH,
                        headers, data_types, col_lengths)
    loaded_rows = execute_sql(nz_sql, prod_or_dev) #this step executes the entire load
    #print(nz_sql) #uncomment this to obtain the load script for manual runs
    return 'records successfully loaded'


########################################
## SET VARIABLES HERE
########################################
table_list = (
#'FETAL_IMAGES',
#'ADDITIONALSTUDIES_IMAGES',
#'EVENT_FETAL',
#'EP_IMAGES', #make sure it ends with a comma
)

read_table_list = table_list
src_system = 'HEALTHVIEW'
table_prefix = src_system
tgt_system = 'CDW_ODS_DEV'


BAD_CHARS = ['"', '\n', '\r']

FLOAT_DIGITS = 5
NZ_LOG_PATH='C:\\Users\\rybad\\Desktop\\' #you'll need to set this up locally )
LOAD_FILE_PATH = os.path.join(NZ_LOG_PATH,'load_files')
LOG_FILE_PATH = os.path.join(NZ_LOG_PATH,'log_files')

########################################
## loop through all table names, select * from table and load to target
########################################

for val in read_table_list:
    src_table_name = val
    select_star_sql =   (f''' 
                         SELECT * 
                         FROM {src_table_name} 
                         
                         ''')

    print(f'{src_table_name} load begin')
    df=sql_to_df(select_star_sql,env=src_system)
    len_df = len(df)
    print(f'loading {len_df} rows')
    tgt_table_name = f'{src_system}_{src_table_name}'
    df_to_cdw(df=df, table_name=tgt_table_name, prod_or_dev='dev')
    
    print(f'{tgt_table_name} load complete')
    