import pandas as pd
import pyodbc
import os
import numpy as np
import csv
import time
import datetime
import sqlalchemy
import logging
from sqlalchemy import create_engine

# ###########
# New User to add
# ###########

#last_name = 'Lowry'
#first_name = 'Meghan'
last_name = input('Enter Last Name: ')
first_name = input('Enter First Name: ')


# ###########
# CardioAccess SQL Server
# ###########

ca_sql_uid = 'cardio'
ca_sql_pwd = 'access'
ca_server = 'PSQLA012'
driver = 'SQL Server'
ca_database = 'Centripetus'
ca_sql_cnxn = pyodbc.connect(f'DRIVER={driver};SERVER={ca_server};DATABASE={ca_database}',uid=ca_sql_uid,pwd=ca_sql_pwd)

# ###########
# Sensis SQL Server
# ###########
 
sen_sql_uid = 'local_reader'
sen_sql_pwd = '$enS1s_R3ad'
sen_server = 'CATHSENPW1'
driver = 'SQL Server'
sen_database = 'mismain'
sen_sql_cnxn = pyodbc.connect(f'DRIVER={driver};SERVER={sen_server};DATABASE={sen_database}',uid=sen_sql_uid,pwd=sen_sql_pwd)


# ###########
# Lookup User in Sensis
# ###########
sensis_sql = f'''
   select
         code,
         meaning
    from 
         dicpnname
   where 
         meaning like ('%{last_name}%{first_name}%')
'''

sen_recs = pd.read_sql(sensis_sql, sen_sql_cnxn)
#print(sen_recs)
try:
    sen_user_id = sen_recs['code'].apply(str)[0]
    print('user exists in Sensis')
    print(sen_recs)

    # ###########
    # Lookup User in CardioAccess
    # ###########
    ca_sql = f'''
    select 
          ContactID,
          ContactIDFT,
          FirstName,
          LastName
     from 
          contacts
    where 
          LastName like ('%{last_name}%')
         and FirstName like  ('%{first_name}%')
    '''
    ##check record to make sure it is the correct/only one
    ca_recs = pd.read_sql(ca_sql, ca_sql_cnxn)
    print('here is the user record as it exists in CardioAccess')
    print(ca_recs)
    ca_user_id = ca_recs['ContactID'].apply(str)[0]

    proceed_input = input('Ok to run script to load Sensis ID into CardioAccess? (Y/N)')
    
except:
    print('user has not been entered into Sensis')    


if proceed_input == 'Y' or proceed_input == 'y':
    cursor = ca_sql_cnxn.cursor()

    cursor.execute(f'''
                    UPDATE contacts
                    SET ContactIDFT = '{sen_user_id}'
                    WHERE ContactID = '{ca_user_id}'
                    ''')
    cursor.commit()

    ##verify changes were successful
    ca_recs = pd.read_sql(ca_sql, ca_sql_cnxn)
    print('CardioAccess update complete for {first_name} {last_name{}')
    print(ca_recs)
else:   
    sys.exit()