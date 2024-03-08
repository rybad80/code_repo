from datetime import datetime as dt
import pysnow
import tabulate
import os
import time
import pandas as pd
import numpy as np
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import json
from datetime import date, datetime, timedelta
from pandas import json_normalize

# #############################################################################
# Service Now Environment Variables
# #############################################################################
runtime = dt.now().strftime('%Y-%m-%d__%H%M')
runtime_nice = dt.now().strftime('%Y-%m-%d  %H:%M')

ENV = 'PROD' # DEV, TEST, PROD
#USER = 'int_arintake'
#PW = 'Arintake2017!!'
USER=os.getenv("SNOW_CREDENTIALS_USR")
PW=os.getenv("SNOW_CREDENTIALS_PSW")
print(USER)
print(PW)
#verify_path = os.environ["REQUESTS_CA_BUNDLE"]


# #############################################################################
# JIRA api setup variables
# #############################################################################
import jira
import parse
import urllib3
import logging

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO
)


USERNAME='rybad'
JIRA_TOKEN='KB7yNU0HwTq0uARQBfNp75AA'
USERNAME2=os.getenv("JIRA_CREDENTIALS_USR")
JIRA_TOKEN2=os.getenv("JIRA_CREDENTIALS_PSW")
print(USERNAME2)
print(JIRA_TOKEN2)

#JIRA_AUTH = (f"{ USERNAME }@email.chop.edu", os.getenv("JIRA_TOKEN"))
JIRA_AUTH = (f"{ USERNAME }@email.chop.edu", JIRA_TOKEN)

chop_jira = jira.JIRA("https://chopdna.atlassian.net", basic_auth=JIRA_AUTH)

# #############################################################################
# Define functions
# #############################################################################
#cwd=os.getcwd()
#print(cwd)
    
def make_api_call(url, parameter_value):
    # You may need to customize the headers or other parameters based on your API
    params = {'sys_id': parameter_value}
    response = requests.get(url=url, params=params,auth=(USER,PW))
    dict_data = response.json()
    return dict_data

def make_tasknumber_api_call(url, parameter_value):
    # You may need to customize the headers or other parameters based on your API
    params = {'number': parameter_value}
    response = requests.get(url=url, params=params,auth=(USER,PW))
    dict_data = response.json()
    return dict_data

# #############################################################################
# Pull all Revenue Cycle Analytics JIRA tasks
# #############################################################################
current_jira_issues = pd.DataFrame()
rca_issue_list=[]
jira_search_query1 = """
labels = RCA
"""
jira_issues = chop_jira.search_issues(jira_search_query1)
for issue in jira_issues:
    rca_issue = issue.fields.summary[:11]
    rca_issue_list.append(rca_issue)
    #print(rca_issue)

current_jira_issues = pd.DataFrame.from_dict(rca_issue_list)
current_jira_issues.columns=['number']
#print(current_jira_issues)

# #############################################################################
# Pull all Revenue Cycle Analytics SN tasks
# #############################################################################
task=requests.get(url='''https://chop.service-now.com/api/now/v2/table/task''',auth=(USER,PW),
                       params={'assignment_group':'1e7a5c17873b3010cef221b2debb3551'})
print('Task Data Acquired')
task_dict_data = task.json()
task_json_data = json.dumps(task_dict_data, indent=4, sort_keys=True)
snow_task_dict= json.loads(task_json_data)
snow_task_df_all = pd.json_normalize(snow_task_dict['result']) 

snow_task_df_all = pd.DataFrame(snow_task_df_all)
snow_task_df_all['open_date'] = pd.to_datetime(snow_task_df_all['opened_at']).dt.date
lookback30=date.today()-timedelta(days=30)
today=date.today()
snow_task_df_30day = snow_task_df_all.loc[(snow_task_df_all['open_date']>lookback30) & (snow_task_df_all['open_date']<today)] #looks back 30 days
snow_task_df_30day = snow_task_df_30day[snow_task_df_30day["number"].str.contains("TASK")]
snow_task_df_30day = snow_task_df_30day[['number']]
#print(snow_task_df_30day)
#rint(current_jira_issues)
tasks_to_load_compare_df = snow_task_df_30day.drop_duplicates().merge(current_jira_issues.drop_duplicates(), left_on = 'number', right_on = 'number',
                   how='left', indicator=True)
tasks_to_load_compare_df.loc[tasks_to_load_compare_df._merge=='left_only',tasks_to_load_compare_df.columns!='_merge']
tasks_to_load_df = tasks_to_load_compare_df[tasks_to_load_compare_df['_merge']=='left_only']
tasks_to_load_df = tasks_to_load_df.drop(columns='_merge')
print(tasks_to_load_df)

# #############################################################################
# Pull new Revenue Cycle Analytics SN tasks that need to be loaded
# #############################################################################

snow_df_all = pd.DataFrame()
for _, row in tasks_to_load_df.iterrows():
    number=row['number']
    url=f'https://chop.service-now.com/api/now/v2/table/task'
    parameter_value = number
    response = make_tasknumber_api_call(url,parameter_value)
    #print(response)
    json_data = json.dumps(response, indent=4, sort_keys=True)
    #print(json_data)
    snow_dict= json.loads(json_data)
    snow_df = pd.json_normalize(snow_dict['result']) 
    #print(snow_df)
    snow_df_all = snow_df_all.append(snow_df)

snow_task_df_all = pd.DataFrame(snow_df_all)
if(snow_task_df_all.empty == False):
    
    snow_task_df_all = snow_task_df_all[['active','additional_assignee_list','close_notes','closed_at','comments_and_work_notes','description','number','opened_at','short_description','sys_created_by','sys_id','task_effective_number','work_end','work_start','assignment_group.value','opened_by.value','parent.value']]
    snow_task_df_all.rename(columns={'assignment_group.value':'assignment_group','opened_by.value':'opened_by',' assigned_to.value':'assigned_to','parent.value':'parent_value'}, inplace=True)

    len_snow_task_df_all = len(snow_task_df_all)
    print(f'{len_snow_task_df_all} rows of Task Data to be loaded')
    
    # #############################################################################
    # Iterate over Request items and pull ones that correspond to Tasks pulled above
    # #############################################################################
    snow_df_all = pd.DataFrame()
    for index, row in snow_task_df_all.iterrows():
        table='sc_req_item'
        url=f'https://chop.service-now.com/api/now/v2/table/{table}'
        parameter_value = row['parent_value']
        #print([parameter_value])
        response = make_api_call(url,parameter_value)
        #print(response)
        json_data = json.dumps(response, indent=4, sort_keys=True)
        #print(json_data)
        snow_dict= json.loads(json_data)
        snow_df = pd.json_normalize(snow_dict['result']) 
        #print(snow_df)
        snow_df_all = snow_df_all.append(snow_df)
    #print(snow_df_all)
    print('Request Data Acquired')

    snow_request_df_all = pd.DataFrame(snow_df_all)
    #print(snow_request_df_all)

    snow_request_df_all = snow_request_df_all[['sys_id','task_effective_number', 'short_description','description','u_requested_for.value']]
    snow_request_df_all.rename(columns={'u_requested_for.value':'requested_for','description':'request_description'}, inplace=True)
    #snow_request_df_all.to_csv('snow_request_df.csv')
    len_snow_request_df = len(snow_request_df_all)
    print(f'{len_snow_request_df} rows of Request Data retrieved')



    # #############################################################################
    # Iterate over Users and pull ones that correspond to Requests pulled above
    # #############################################################################
    snow_df_all = pd.DataFrame()
    for index, row in snow_request_df_all.iterrows():
        table='sys_user'
        url=f'https://chop.service-now.com/api/now/v2/table/{table}'
        parameter_value = row['requested_for']
        #print([parameter_value])
        response = make_api_call(url,parameter_value)
        #print(response)
        json_data = json.dumps(response, indent=4, sort_keys=True)
        #print(json_data)
        snow_dict= json.loads(json_data)
        snow_df = pd.json_normalize(snow_dict['result']) 
        #print(snow_df)
        snow_df_all = snow_df_all.append(snow_df)
    #print(snow_df_all)
    print('User Data Acquired')


    snow_user_df_all = pd.DataFrame(snow_df_all)
    snow_user_df_dups = snow_user_df_all[['first_name','last_name','name','sys_id','email']]
    snow_user_df = snow_user_df_dups.drop_duplicates()
    #snow_user_df.to_csv('snow_user_df.csv')
    len_snow_user_df = len(snow_user_df)
    print(f'{len_snow_user_df} rows of User Data retrieved')
    
    # #############################################################################
    # Join Tasks + Requests + Users
    # #############################################################################
    snow_reqs_tasks_df = pd.merge(snow_task_df_all,snow_request_df_all, how = 'inner', left_on='parent_value', right_on='sys_id')
    #snow_reqs_tasks_df.to_csv('snow_reqs_tasks_df.csv')

    snow_reqs_tasks_user_df = pd.merge(snow_reqs_tasks_df, snow_user_df, how = 'left', left_on='requested_for', right_on='sys_id')
    #snow_tasks_users_df=snow_task_df.merge(snow_user_df,how="left",on="sys_id")
    #print(snow_reqs_tasks_user_df)
    snow_reqs_tasks_user_df_final = snow_reqs_tasks_user_df[['task_effective_number_x','task_effective_number_y','description','short_description_x','request_description','name','email','opened_at']]
    snow_reqs_tasks_user_df_final.rename(columns={'task_effective_number_x':'task_number','task_effective_number_y':'item_number','short_description_x':'short_description'}, inplace=True)
    #snow_reqs_tasks_user_df_final.to_csv('snow_reqs_tasks_user_df.csv')

    print(snow_reqs_tasks_user_df_final.to_markdown())



    # #############################################################################
    # Write New Requests to JIRA
    # #############################################################################

    cr='\r\n'

    for _,row in snow_reqs_tasks_user_df_final.iterrows():
        summary = row["task_number"]+' - '+row["short_description"] 
        comment = row["description"]    
        description = row["request_description"]    
        requestor = row["name"]
        new_issue = chop_jira.create_issue(
            project="RCA",
            summary=summary,
            description=description+chr(10)+chr(10)+comment+chr(10)+chr(10)+f' Requested By:{requestor}'+chr(10)+chr(10),
            labels=['RCA'],
            issuetype={"name": "Task"},
        )

    data_load_rows = len(snow_reqs_tasks_user_df_final)    
    print(f'{data_load_rows} Service Now Requests added to JIRA')
    
    
elif (snow_task_df_all.empty == False):
    print('No Records Found to Load')
    exit()    