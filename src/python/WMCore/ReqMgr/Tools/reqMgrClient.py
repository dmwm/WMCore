#!/usr/bin/env python
"""
    This client encapsulates several basic queries to request manager.
    This uses ReqMgr rest api through HTTP
    url parameter is normally 'cmsweb.cern.ch'
"""

# system modules
import os
import re
import sys
import json
import urllib
import urllib2
import httplib

# default headers for PUT and POST methods
HEADERS={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}

def requestManagerGet(url, request, retries=4):
    """
    Queries ReqMgr through a HTTP GET method
    in every request manager query 
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url
    retries: number of retries
    """
    cert = os.getenv('X509_USER_PROXY')
    ckey = os.getenv('X509_USER_PROXY')
    for _ in range(retries):
        conn = httplib.HTTPSConnection(url, cert_file=cert, key_file=ckey)
        conn.request("GET", request)
        resp = conn.getresponse()
        request = json.load(resp)  
        if 'exception' not in request:
            return request
    raise Exception('Maximum queries to ReqMgr exceeded')

def requestManagerPost(url, request, params, head = HEADERS):
    """
    Performs some operation on ReqMgr through
    an HTTP POST method.
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url for the POST method
    params: a dict with the POST parameters
    """
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                    key_file = os.getenv('X509_USER_PROXY'))
    headers = head
    encodedParams = urllib.urlencode(params)
    conn.request("POST", request, encodedParams, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return data

def requestManagerPut(url, request, params, head = HEADERS):
    """
    Performs some operation on ReqMgr through
    an HTTP PUT method.
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url for the POST method
    params: a dict with the PUT parameters
    head: optional headers param. If not given it takes default value (HEADERS)
    """
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                    key_file = os.getenv('X509_USER_PROXY'))
    headers = head
    encodedParams = urllib.urlencode(params)
    conn.request("PUT", request, encodedParams, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return data

class WorkflowManager(object):
    def __init__(self, workflow, url='cmsweb.cern.ch'):
        "Request workflow information from ReqMgr and provide APIs for its attributes"
        self.workflow = workflow
        self.winfo = requestManagerGet(url,'/reqmgr/reqMgr/request?requestName='+workflow)
        self.output_datasets = requestManagerGet(url,'/reqmgr/reqMgr/outputDatasetsByRequestName?requestName='+workflow)

    def request_status(self):
        """Retrieves workflow status"""
        return self.winfo.get('RequestStatus', '')

    def request_type(self):
        return self.winfo.get('RequestType', '')

    def request_priority(self):
        return self.winfo.get('RequestPriority', 0)

    def run_whitelist(self):
        return self.winfo.get('RunWhiteList', [])

    def block_whitelist(self):
        return self.winfo.get('BlockWhiteList', [])

    def input_dataset(self):
        return self.winfo.get('InputDataset', '')

    def team(self):
        """Retrieves the team on which the wf is assigned"""
        return self.winfo.get('teams', ['NoTeam'])[0]

    def output_datasets(self):
        """returns the output datasets for a given workfow"""
        return self.output_datasets

    def input_events(self):
        """
        Gets the inputs events of a given workflow
        depending of the kind of workflow
        """
        request = self.winfo
        requestType = request['RequestType']
        #if request is montecarlo or Step0, the numer of
        #input events is by the requsted events
        if requestType == 'MonteCarlo' or requestType == 'LHEStepZero':
            if 'RequestNumEvents' in request:
                if request['RequestNumEvents']>0:
                    return request['RequestNumEvents']
            if 'RequestSizeEvents' in request:
                return request['RequestSizeEvents']
            else:
                return 0
        if requestType == 'TaskChain':
            return handleTaskChain(request)

        #if request is not montecarlo, then we need to check the size
        #of input datasets
        #This loops fixes the white and blacklists in the workflow
        #information,
        for listitem in ["RunWhitelist", "RunBlacklist",
                        "BlockWhitelist", "BlockBlacklist"]:
            if listitem in request:
                #if empty
                if request[listitem]=='[]' or request[listitem]=='':
                    request[listitem]=[]
                #if there is not a list but some elements it creates a list
                if not isinstance(request[listitem], list):
                    # if doesn't contain "[" is a single block
                    if '[' not in request[listitem]:
                        #wrap in a list
                        request[listitem] = [request[listitem]]
                    #else parse a list
                    else:
                        request[listitem]= eval(request[listitem])
            #if not, an empty list will do        
            else:
                request[listitem]=[]

        inputDataSet=request['InputDataset']
        
        #it the request is rereco, we valiate white/black lists
        if requestType=='ReReco':
            # if there is block whte list, count only the selected block
            if request['BlockWhitelist']:
                events = dbs3.getEventCountDataSetBlockList(inputDataSet,request['BlockWhitelist'])
            # if there is block black list, substract them from the total
            if request['BlockBlacklist']:
                events = (dbs3.getEventCountDataSet(inputDataSet) - 
                        dbs3.getEventCountDataSet(inputDataSet,request['BlockBlacklist']))
                return events
            # same if a run whitelist
            if request['RunWhitelist']:
                events = dbs3.getEventCountDataSetRunList(inputDataSet, request['RunWhitelist'])
                return events
            # otherwize, the full lumi count
            else:
                events = dbs3.getEventCountDataset(inputDataSet)
                return events
        
        events = dbs3.getEventCountDataSet(inputDataSet)
        # if black list, subsctract them    
        if request['BlockBlacklist']:
            events=events-dbs3.getEventCountDataSetBlockList(inputDataSet, request['BlockBlacklist'])
        # if white list, only the ones in the whitelist.
        if request['RunWhitelist']:
            events=dbs3.getEventCountDataSetRunList(inputDataSet, request['RunWhitelist'])
        # if white list of blocks
        if request['BlockWhitelist']:
            events=dbs3.getEventCountDataSetBlockList(inputDataSet, request['BlockWhitelist'])

        if 'FilterEfficiency' in request:
            return float(request['FilterEfficiency'])*events
        else:
            return events

def getOutputEvents(dataset):
    """
    Gets the output events depending on the type
    if the request
    """
    return dbs3.getEventCountDataSet(dataset)
    
def closeOutWorkflow(url, workflowname):
    """
    Closes out a workflow by changing the state to closed-out
    This does not care about cascade workflows
    """
    params = {"requestName" : workflowname,"status" : "closed-out"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def closeOutWorkflowCascade(url, workflowname):
    """
    Closes out a workflow, it will search for any Resubmission requests 
    for which the given request is a parent and announce them too.
    """
    params = {"requestName" : workflowname, "cascade" : True}
    data = requestManagerPost(url,"/reqmgr/reqMgr/closeout", params)
    return data

def announceWorkflow(url, workflowname):
    """
    Sets a workflow state to announced
    This does not care about cascade workflows
    """
    params = {"requestName" : workflowname,"status" : "announced"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def announceWorkflowCascade(url, workflowname):
    """
    Sets a workflow state to announced, it will search for any Resubmission requests 
    for which the given request is a parent and announce them too.
    """
    params = {"requestName" : workflowname, "cascade" : True}
    data = requestManagerPost(url,"/reqmgr/reqMgr/announce", params)
    return data


def setWorkflowApproved(url, workflowname):
    """
    Sets a workflow state to assignment-approved
    """
    params = {"requestName" : workflowname,"status" : "assignment-approved"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def setWorkflowRunning(url, workflowname):
    """
    Sets a workflow state to running
    """
    params = {"requestName" : workflowname,"status" : "running"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def rejectWorkflow(url, workflowname):
    """
    Sets a workflow state to rejected
    """
    params = {"requestName" : workflowname,"status" : "rejected"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def abortWorkflow(url, workflowname):
    """
    Sets a workflow state to aborted
    """
    params = {"requestName" : workflowname,"status" : "aborted"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def cloneWorkflow(url, workflowname):
    """
    This clones a request
    """
    headers={"Content-Length": 0}
    params = {}
    data = requestManagerPut(url,"/reqmgr/reqMgr/clone/", params, headers)
    return data

def submitWorkflow(url, schema):
    """
    This submits a workflow into the ReqMgr, can be used for cloning
    and resubmitting workflows
    url: the instance ued, i.e. 'cmsweb.cern.ch'
    schema: A dictionary with the parameters needed to create
    the workflow
    
    """
    data = requestManagerPost(url,"/reqmgr/create/makeSchema", schema)
    return data

def handleTaskChain(request):
    # Check if it's MC from scratch
    if 'RequestNumEvents' in request['Task1']:
        if request['Task1']['RequestNumEvents'] is not None:
            return request['Task1']['RequestNumEvents']

    blockWhitelist = blockBlacklist = runWhitelist = runBlacklist = []
    if 'InputDataset' in request['Task1']:
        inputDataSet=request['Task1']['InputDataset']
        if 'BlockWhitelist' in request['Task1']:
            blockWhitelist=request['Task1']['BlockWhitelist']
        if 'BlockBlacklist' in request['Task1']:
            blockBlacklist=request['Task1']['BlockBlacklist']
        if 'RunWhitelist' in request['Task1']:
            runWhitelist=request['Task1']['RunWhitelist']
        if 'RunBlacklist' in request['Task1']:
            runBlacklist=request['Task1']['RunBlacklist']

        if blockWhitelist:
            return dbs3.getEventCountDataSetBlockList(inputDataSet,blockWhitelist)
        if blockBlacklist:
            return dbs3.getEventCountDataset(inputDataSet) - dbs3.getEventCountDataSetBlockList(inputDataSet,blockBlacklist)
        if runWhitelist:
            return dbs3.getEventCountDataSetRunList(inputDataSet, runWhitelist)
        else:
            return dbs3.getEventCountDataset(inputDataSet)

### TODO: implement multi white/black list
#        if len(blockWhitelist)>0 and len(runWhitelist)>0:
#            print "Hey, you have block and run white list :-D"
#            return getRunLumiCountDatasetBlockList(inputDataSet,BlockWhitelist)
#        elif len(blockWhitelist)>0 and len(runWhitelist)==0:
#            print "Hey, you have block white list but NOT run white list :-D"
#        elif len(blockWhitelist)==0 and len(runWhitelist)>0:
#            print "Hey, you have NO block white list but you do have run white list :-D"
#            return getRunLumiCountDatasetList(inputDataSet, runWhitelist)
#        elif len(blockWhitelist)==0 and len(runWhitelist)==0:
#            print "Hey, you have NO block and run white list :-D"
#
#        if len(BlockBlacklist)>0 and len(runBlacklist)>0:
#            print "Hey, you have block and run black list :-("
#            return getRunLumiCountDataset(inputDataSet)-getRunLumiCountDatasetBlockList(inputDataSet,BlockBlacklist)
#        elif len(BlockBlacklist)>0 and len(runBlacklist)==0:
#            print "Hey, you have block black list but NOT run black list :-("
#        elif len(BlockBlacklist)==0 and len(runBlacklist)>0:
#            print "Hey, you have NO block black list but you do have run black list :-("
#        elif len(BlockBlacklist)==0 and len(runBlacklist)==0:
#            print "Hey, you have NO block and run black list :-("





