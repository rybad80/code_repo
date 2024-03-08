import logging
import pandas as pd
import os
from itertools import chain
import requests
import json
from datetime import datetime
from redcap import Project
import jira
import parse
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO
)

# #############################################################################
# redcap api setup
# #############################################################################
api_url = 'https://redcap.chop.edu/api/'
api_key = 'C9F11788CCEAEF6D5AF3EF303C4EC9E4'
project = Project(api_url, api_key, verify_ssl=False)

now = datetime.now()
currentdate = now.strftime("%Y-%m-%d")

# #############################################################################
# Get today's requests from REDCap
# #############################################################################

loaded_reqs = project.export_records(format='df')
loaded_reqs.columns = map(str.upper, loaded_reqs.columns)
loaded_reqs_df = pd.DataFrame(loaded_reqs,columns = ['cc_data_request_id','REQUEST_TITLE', 'DATE_OF_REQUEST','STUDY_DESCRIPTION', 'COMMENT','REQUESTOR_NAME','LOADED_TO_JIRA','ATTACH'])

daily_req_df=loaded_reqs_df.loc[loaded_reqs_df['LOADED_TO_JIRA'].isnull()]


# #############################################################################
# JIRA api setup variables
# #############################################################################

#USERNAME = os.getenv("USERNAME")
USERNAME='rybad'
JIRA_TOKEN='KB7yNU0HwTq0uARQBfNp75AA'
#JIRA_AUTH = (f"{ USERNAME }@email.chop.edu", os.getenv("JIRA_TOKEN"))
JIRA_AUTH = (f"{ USERNAME }@email.chop.edu", JIRA_TOKEN)

chop_jira = jira.JIRA("https://chopdna.atlassian.net", basic_auth=JIRA_AUTH)

# #############################################################################
# Write New Requests to JIRA
# #############################################################################

cr='\r\n'

for _,row in daily_req_df.iterrows():
    summary = row["REQUEST_TITLE"]
    description = row["STUDY_DESCRIPTION"]    
    comment = row["COMMENT"] 
    requestor = row["REQUESTOR_NAME"]
    attach = row["ATTACH"]
    new_issue = chop_jira.create_issue(
        project="DA",
        summary=summary,
        description=description+chr(10)+chr(10)+f'{comment}'+chr(10)+chr(10)+f' Requested By:{requestor}'+chr(10)+chr(10)+f' Attachment:{attach}',
        labels=['Cardiac'],
        issuetype={"name": "Task"},
    )

# #############################################################################
# set REDCap records to True for "Loaded to JIRA"
# #############################################################################

daily_req_df['LOADED_TO_JIRA']=1
daily_req_df.index.name = 'cc_data_request_id'
daily_req_df['cc_data_request_id'] = daily_req_df.index
daily_req_df.columns = map(str.lower, daily_req_df.columns)
daily_req_upd = daily_req_df[["cc_data_request_id", "loaded_to_jira"]]
print(daily_req_upd)
response = project.import_records(daily_req_upd)
