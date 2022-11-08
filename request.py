import csv
import datetime
from os import stat
import requests
import pandas as pd
import msal
from django.utils.dateparse import parse_datetime                                               

class BearerAuth(requests.auth.AuthBase):                                                                           #class for Bearer authentification
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r
    
with open('access/values.txt', encoding="utf-8") as vf:                                                             #read txt file with authority values
    authorityValues = vf.read().splitlines() 

logFile = open('log/refresh_logs.csv', 'a', encoding='utf-8')
fieldnames = ['workspaceName', 'responseStatusCode', 'responseStatus', 'dataFlowName', 'refreshDateTime',
              'refreshStatusCode', 'refreshStatus', 'currentTime']
writer = csv.DictWriter(logFile, fieldnames = fieldnames)
if stat("log/refresh_logs.csv").st_size == 0:
    writer.writeheader()    
authority_url = 'https://login.microsoftonline.com/' + authorityValues[0]
client_id = authorityValues[1]
client_secret = authorityValues[2]
scope = ['https://analysis.windows.net/powerbi/api/.default']
app = msal.ConfidentialClientApplication(client_id,authority=authority_url,
                                         client_credential=client_secret)                                           #auth into PBI
result = app.acquire_token_for_client(scopes=scope)                             
Bearer = result['access_token']                                                                                     #get access token (bearer)
try:
    workspaceRequest = requests.get('https://api.powerbi.com/v1.0/myorg/groups', 
                                 auth = BearerAuth(Bearer))
    workspaceRequest.raise_for_status()
    workspaceInfo = workspaceRequest.json()
    for wk in workspaceInfo['value']:
        workspaceID = wk['id']
        dataFlowsInfo = requests.get('https://api.powerbi.com/v1.0/myorg/groups/' + workspaceID +'/dataflows', 
                                    auth = BearerAuth(Bearer)).json()
        dataFlowID = []
        for dv in dataFlowsInfo['value']:
            dataFlowID.append(dv['objectId'])
        for i in dataFlowID:
            try:
                dataFlowRequest = requests.get('https://api.powerbi.com/v1.0/myorg/groups/' + workspaceID + '/dataflows/'+i ,       
                                auth = BearerAuth(Bearer))
                dataFlowRequest.raise_for_status()
                dataFlowData = dataFlowRequest.json()
                dataFlowName = dataFlowData['name']  
                refreshTime = parse_datetime(dataFlowData['entities'][0]['partitions'][0]['refreshTime']).replace(minute = 0, second = 0, microsecond = 0)         #get last update time in UTC
                currentTime = datetime.datetime.utcnow().replace(minute = 0, second = 0, microsecond = 0)                                                    #get current time in UTC
                if (refreshTime.date() != currentTime.date()) | ((refreshTime.date() == currentTime.date()) & (refreshTime.hour != currentTime.hour)):                                                  #check if dataflow was already updated today
                    refresh = requests.post('https://api.powerbi.com/v1.0/myorg/groups/' + workspaceID +                    #refresh if it is not
                                            '/dataflows/' + i + '/refreshes?processType=default',
                                            auth = BearerAuth(Bearer), data= {"notifyOption": "MailOnFailure"})        
                    refreshStatusCode = (str)(refresh.status_code)
                    refreshStatus = refresh.reason                                                  
                else:                    
                    refreshStatus = 'NotNeeded'
                    refreshStatusCode = '-' 

            except requests.HTTPError as dfce:
                dataFlowName = 'DataflowConnectionError'                             
                currentTime = str(datetime.datetime.utcnow())                                              
                refreshTime =  '-'
                refreshStatus = 'DataflowConnectionError'
                refreshStatusCode = '-'

            writer.writerows([{ "workspaceName": wk['name'],"responseStatusCode": dataFlowRequest.status_code,               
                                "responseStatus":dataFlowRequest.reason, "refreshStatus": refreshStatus,
                                "dataFlowName": dataFlowName, "refreshDateTime":refreshTime,
                                "refreshStatusCode":refreshStatusCode, "currentTime":currentTime}])
except  requests.HTTPError as wsce:
    dataFlowName = 'WorkspaceConnectionError'                             
    currentTime = str(datetime.datetime.utcnow())                                              
    refreshTime =  '-'
    refreshStatus = 'WorkspaceConnectionError'
    refreshStatusCode = '-'
    writer.writerows([{ "workspaceName": 'WorkspaceConnectionError',"responseStatusCode": workspaceRequest.status_code,               
                        "responseStatus":workspaceRequest.reason, "refreshStatus": refreshStatus,
                        "dataFlowName": dataFlowName, "refreshDateTime":refreshTime,
                        "refreshStatusCode":refreshStatusCode, "currentTime":currentTime}])