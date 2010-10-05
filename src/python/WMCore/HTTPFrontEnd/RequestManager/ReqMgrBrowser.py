#!/usr/bin/env python

import WMCore.RequestManager.RequestDB.Settings.RequestStatus as RequestStatus
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import parseRunList, parseBlockList
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.Cache.WMConfigCache import ConfigCache 
from PSetTweaks.PSetTweak import PSetHolder, PSetTweak
from WMCore.Services.Requests import JSONRequests
import WMCore.HTTPFrontEnd.RequestManager.Sites

import cherrypy
import logging
import os.path
import pickle
import urllib
from WMCore.WebTools.Page import TemplatedPage

class ReqMgrBrowser(TemplatedPage):
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        # Take a guess
        self.templatedir = __file__.rsplit('/', 1)[0]
        self.urlPrefix = '%s/download/?filepath=' % config.reqMgrHost
        self.fields = ['RequestName', 'Group', 'Requestor', 'RequestType', 'ReqMgrRequestBasePriority', 'RequestStatus', 'Complete', 'Success']
        self.calculatedFields = {'Written': 'percentWritten', 'Merged':'percentMerged', 'Complete':'percentComplete', 'Success' : 'percentSuccess'}
        self.linkedFields = {'RequestName':'requestDetails'}
        self.detailsFields = ['RequestName', 'RequestType', 'Requestor', 'CMSSWVersion', 'ScramArch', 'GlobalTag', 'RequestSizeEvents', 
            'InputDataset', 'PrimaryDataset', 'AcquisitionEra', 'ProcessingVersion', 
            'RunWhitelist', 'RunBlacklist', 'BlockWhitelist', 'BlockBlacklist', 
            'RequestWorkflow', 'Scenario', 'PrimaryDataset']

        self.adminMode = True
        # don't allow mass editing.  Make people click one at a time.
        #self.adminFields = {'RequestStatus':'statusMenu', 'ReqMgrRequestBasePriority':'priorityMenu'}
        self.adminFields = {}
        self.requests = []
        self.configCacheUrl = config.configCacheUrl
        self.configDBName = config.configDBName
        self.workloadDir = config.workloadCache
        self.jsonSender = JSONRequests(config.reqMgrHost)
        self.sites = WMCore.HTTPFrontEnd.RequestManager.Sites.sites()

        self.mergedLFNBases = {"ReReco" : ["/store/backfill/1", "/store/backfill/2", "/store/data"],
                               "MonteCarlo" : ["/store/backfill/1", "/store/backfill/2", "/store/mc"]}

    def index(self):
        requests = self.getRequests()
        print str(requests)
        tableBody = self.drawRequests(requests)
        return self.templatepage("ReqMgrBrowser", fields=self.fields, tableBody=tableBody)
    index.exposed = True

    def search(self, value, field):
        filteredRequests = []
        requests =  self.getRequests() 
        for request in requests:
           if request[field].find(value) != -1:
               filteredRequests.append(request)
        requests = filteredRequests
        tableBody = self.drawRequests(requests)
        return self.templatepage("ReqMgrBrowser", fields=self.fields, tableBody=tableBody)
        
    search.exposed = True
        
    def getRequests(self):
        return self.jsonSender.get("/reqMgr/request")[0]

    def splitting(self, requestName):
        """
        _splitting_

        Retrieve the current values for splitting parameters for all tasks in
        the spec.  Format them in the manner that the splitting page expects
        and pass them to the template.
        """
        request = self.jsonSender.get("/reqMgr/request/"+requestName)[0]
        helper, pfn = self.workloadHelper(request)
        splittingDict = helper.listJobSplittingParametersByTask()
        taskNames = splittingDict.keys()
        taskNames.sort()

        splitInfo = []
        for taskName in taskNames:
            # We basically stringify the splitting params dictionary and pass
            # that to the splitting page as javascript.  We need to change
            # boolean values to strings as the javascript true is different from
            # the python True.
            if "split_files_between_job" in splittingDict[taskName]:
                splittingDict[taskName]["split_files_between_job"] = str(splittingDict[taskName]["split_files_between_job"])
                
            splitInfo.append({"splitAlgo": splittingDict[taskName]["algorithm"],
                              "splitParams": str(splittingDict[taskName]),
                              "taskType": splittingDict[taskName]["type"],
                              "taskName": taskName})

        return self.templatepage("Splitting", requestName = requestName,
                                 taskInfo = splitInfo, taskNames = taskNames)
    splitting.exposed = True
            
    def handleSplittingPage(self, requestName, splittingTask, splittingAlgo,
                            **submittedParams):
        """
        _handleSplittingPage_

        Parse job splitting parameters sent from the splitting parameter update
        page.  Pull down the request and modify the new spec applying the
        updated splitting parameters.
        """
        splitParams = {}
        if splittingAlgo == "FileBased":
            splitParams["files_per_job"] = submittedParams["files_per_job"]
        elif splittingAlgo == "TwoFileBased":
            splitParams["files_per_job"] = submittedParams["two_files_per_job"]            
        elif splittingAlgo == "LumiBased":
            splitParams["lumis_per_job"] = submittedParams["lumis_per_job"]
            if str(submittedParams["split_files_between_job"]) == "True":
                splitParams["split_files_between_job"] = True
            else:
                splitParams["split_files_between_job"] = False                
        elif splittingAlgo == "EventBased":
            splitParams["events_per_job"] = submittedParams["events_per_job"]
            
        request = self.jsonSender.get("/reqMgr/request/"+requestName)[0]
        helper, pfn = self.workloadHelper(request)
        helper.setJobSplittingParameters(splittingTask, splittingAlgo, splitParams)
        helper.save(pfn)
        return "Successfully updated splitting parameters for " + splittingTask \
               + " " + self.detailsBackLink(requestName)
    handleSplittingPage.exposed = True

    def requestDetails(self, requestName):
        result = ""
        request = self.jsonSender.get("/reqMgr/request/"+requestName)[0]
        helper, pfn = self.workloadHelper(request)

        docId = None
        d = helper.data.request.schema.dictionary_()
        d['RequestWorkflow'] = request['RequestWorkflow']
        if d.has_key('ProdConfigCacheID') and d['ProdConfigCacheID'] != "":
            docId = d['ProdConfigCacheID']        
        self.addHtmlLinks(d)
        assignments= self.jsonSender.get('/reqMgr/assignment?request='+requestName)[0]
        adminHtml = self.statusMenu(requestName, request['RequestStatus']) \
                  + ' Priority ' + self.priorityMenu(requestName, request['ReqMgrRequestBasePriority'])
        return self.templatepage("Request", requestName=requestName,
                                detailsFields = self.detailsFields, requestSchema=d,
                                workloadDir = self.workloadDir, 
                                docId=docId, assignments=assignments,
                                adminHtml = adminHtml,
                                messages=request['RequestMessages'],
                                updateDictList=request['RequestUpdates'])
                                 
        return result
    requestDetails.exposed = True

    def showOriginalConfig(self, docId):
        configCache = ConfigCache(self.configCacheUrl, self.configDBName)
        configCache.loadByID(docId)
        configString =  configCache.getConfig()
        if configString == None:
            return "Cannot find document " + str(docId) + " in Couch DB"
        return '<pre>' + configString + '</pre>'
    showOriginalConfig.exposed = True

    def showTweakFile(self, docId):
        configCache = ConfigCache(self.configCacheUrl, self.configDBName)
        configCache.loadByID(docId)
        return str(configCache.getPSetTweaks()).replace('\n', '<br>')
    showTweakFile.exposed = True

    def showWorkload(self, filepath):
        helper = WMWorkloadHelper()
        helper.load(filepath)
        return str(helper.data).replace('\n', '<br>')
    showWorkload.exposed = True
 
    def addHtmlLinks(self, d):
        for key, value in d.iteritems():
            if isinstance(value, str) and value.startswith('http'):
                target = value
                # assume CVS browsers need extra tags
                if 'cvs' in target:
                    target += '&view=markup'
                d[key] = '<a href="%s">%s</a>' % (target, value)
        print d

    def remakeWorkload(self, requestName):
        request = self.jsonSender.get("/reqMgr/request/"+requestName)[0]
        # Should really get by RequestType
        workloadMaker = WorkloadMaker(requestName)
        # I'm getting requests and requestSchema confused
        workloadMaker.loadRequestSchema(request)
        workload = workloadMaker.makeWorkload()
        workloadCache = getWorkloadCache()
        request['RequestWorkflow'] = workloadCache.checkIn(workload)
                
    remakeWorkload.exposed = True

    def drawRequests(self, requests):
        result = ""
        for request in requests:
            # see if this is slower
            #request = self.jsonSender.get("/reqMgr/request/"+request["RequestName"])[0]
            result += self.drawRequest(request)
        return result;

    def drawRequest(self, request):
        # make a table row
        html = '<tr>'
        requestName = request['RequestName']
        for field in self.fields:
            # hanole any fields that have functions linked to them
            html += '<td>'
            if self.adminMode and self.adminFields.has_key(field):
                method = getattr(self, self.adminFields[field])
                html += method(requestName, str(request[field]))
            elif self.linkedFields.has_key(field):
                html += self.linkedTableEntry(self.linkedFields[field], str(request[field]))
            elif self.calculatedFields.has_key(field):
                method = getattr(self, self.calculatedFields[field])
                html += method(request)
            else:
                html += str(request[field]) 
            html += '</td>'
        html += '</tr>\n'
        return html

    def linkedTableEntry(self, methodName, entry):
        """ Makes an HTML table entry with the entry name, and a link to a
        method with that entry as an argument"""
        return '<a href="%s/%s">%s</a>' % (methodName, entry, entry)

    def statusMenu(self, requestName, defaultField):
        """ Makes an HTML menu for setting the status """
        html = defaultField + '&nbsp<SELECT NAME="%s:status"> <OPTION></OPTION>' % requestName
        for field in RequestStatus.NextStatus[defaultField]:
	    html += '<OPTION>%s</OPTION>' % field
        html += '</SELECT>'
        return html

    def priorityMenu(self, requestName, defaultPriority):
        return '%s &nbsp<input type="text" size=2 name="%s:priority">' % (defaultPriority, requestName)

    def percentWritten(self, request):
        maxPercent = 0
        for update in request["RequestUpdates"]:
            if update.has_key("events_written") and request["RequestSizeEvents"] != 0:
                percent = update["events_written"] / request["RequestSizeEvents"]
                if percent > maxPercent:
                    maxPercent = percent
            if update.has_key("files_written") and request["RequestSizeFiles"] != 0:
                percent = update["files_written"] / request["RequestSizeFiles"]
                if percent > maxPercent:
                    maxPercent = percent
        return "%i%%" % maxPercent

    def percentMerged(self, request):
        maxPercent = 0
        for update in request["RequestUpdates"]:
            if update.has_key("events_merged") and request["RequestSizeEvents"] != 0:
                percent = update["events_merged"] / request["RequestSizeEvents"]
                if percent > maxPercent:
                    maxPercent = percent
            if update.has_key("files_merged") and request["RequestSizeFiles"] != 0:
                percent = update["files_merged"] / request["RequestSizeFiles"]
                if percent > maxPercent:
                    maxPercent = percent
        return "%i%%" % maxPercent

    def percentComplete(self, request):
        #return self.biggestUpdate('percent_complete', request)
        pct = request.get('percent_complete', 0)
        return "%i%%" % pct

    def percentSuccess(self, request):
        #return self.biggestUpdate('percent_success', request)
        pct = request.get('percent_success', 0)
        return "%i%%" % pct


    def biggestUpdate(self, field, request):
        max = 0
        for update in request["RequestUpdates"]:
            if update.has_key(field):
                max = update[field]
        return "%i%%" % max


    def doAdmin(self, **kwargs):
        # format of kwargs is {'requestname:status' : 'approved', 'requestname:priority' : '2'}
        message = ""
        for k,v in kwargs.iteritems():
            if k.endswith(':status'): 
                requestName = k.split(':')[0]
                status = v
                priority = kwargs[requestName+':priority']
                if status != "" or priority != "":
                    message += self.updateRequest(requestName, status, priority)
        return message
    doAdmin.exposed = True

    def detailsBackLink(self, requestName):
        return  ' <A HREF=requestDetails/%s>Details</A> <A HREF=".">Back</A><BR>' % requestName

    def updateRequest(self, requestName, status, priority):
        urd = '/reqMgr/request/' + requestName + '?'
        message = "Changed " + requestName
        if status != "":
            urd += 'status='+status
            message += ' status='+status
        if priority != "":
            if status != "":
                urd += '&'
            urd += 'priority='+priority
            message += ' priority='+priority
        self.jsonSender.put(urd)
        if status == "assigned":
           # make a page to choose teams
           return self.assignmentPage(requestName)
        return message + self.detailsBackLink(requestName)

    def assignmentPage(self, requestName):
        teams = self.jsonSender.get('/reqMgr/team')[0]
        requestType = self.jsonSender.get('/reqMgr/request/%s' % requestName)[0]["RequestType"]
        # get assignments
        assignments = self.jsonSender.get('/reqMgr/assignment?request=%s' % requestName)[0]
        # might be a list, or a dict team:priority
        if isinstance(assignments, dict):
            assignments = assignments.keys()
        return self.templatepage("Assign", requestName=requestName, teams=teams, 
                   assignments=assignments, sites=self.sites, mergedLFNBases = self.mergedLFNBases[requestType])
    assignmentPage.exposed = True
    
    def workloadHelper(self, request):
        """ Returns a WMWorkloadHelper and a pfn for the workload in the request """
        helper = WMWorkloadHelper()
        pfn = os.path.join(self.workloadDir, request['RequestWorkflow'])
        helper.load(pfn)
        return helper, pfn

    def handleAssignmentPage(self, *args, **kwargs):
        """ handles some checkboxes """
        result = ""
        requestName = kwargs["RequestName"]
        assignments = self.jsonSender.get('/reqMgr/assignment?request=%s' % requestName)[0]
        request = self.jsonSender.get("/reqMgr/request/"+requestName)[0]
        helper, pfn = self.workloadHelper(request)
        schema = helper.data.request.schema
        # look for teams
        teams = []
        for key, value in kwargs.iteritems():
            if type(value) == type(""):
                kwargs[key] = value.strip()
                setattr(schema, key, value.strip())
            else:
                kwargs[key] = value
                setattr(schema, key, value)
                
            if key.startswith("Team"):
                team = key[4:]
                if not team in assignments:
                    teams.append(team)
                    self.jsonSender.put('/reqMgr/assignment/%s/%s' % (urllib.quote(team), requestName))
                    result += "Assigned to team %s\n" % team
        if teams == [] and assignments == []:
            raise cherrypy.HTTPError(400, "Must assign to one or more teams")

        if kwargs.has_key("StdJobSplitAlgo"):
            schema["StdJobSplitAlgo"] = splitAlgo
            d = {}
            if splitAlgo == "FileBased":
                 d = {'files_per_job' : kwargs["filesPerJob"] }
            elif splitAlgo == "LumiBased":
                 d = {'lumis_per_job' : kwargs["lumisPerJob"],
                      'split_files_between_job':kwargs["splitFilesBetweenJob"]}
            elif splitAlgo == "EventBased":
                 d = {'events_per_job': kwargs["eventsPerJob"]}
            else:
                  raise RuntimeError("Cannot find splitting algo " + splitAlgo)
            schema["StdJobSplitArgs"] = d

        if kwargs.has_key("SkimJobSplitAlgo"):
            skimSplitAlgo = kwargs["SkimJobSplitAlgo"]
            schema["SkimJobSplitAlgo"] = skimSplitAlgo
            files_per_job = 0
            if skimSplitAlgo == "FileBased":
               files_per_job = kwargs["skimFilesPerJob"]
            elif skimSplitAlgo == "TwoFileBased":
               files_per_job = kwargs["skimTwoFilesPerJob"]
            else:
                  raise RuntimeError("Cannot find splitting algo " + skimSplitAlgo)
            schema["SkimJobSplitArgs"] = {'files_per_job': files_per_job}

        helper.setSiteWhitelist(self.parseSite(kwargs,"SiteWhitelist"))
        helper.setSiteBlacklist(self.parseSite(kwargs,"SiteBlacklist"))
        helper.setProcessingVersion(kwargs["ProcessingVersion"])
        helper.setAcquisitionEra(kwargs["AcquisitionEra"])
        helper.setLFNBase(kwargs["MergedLFNBase"], kwargs["UnmergedLFNBase"])
        helper.setMergeParameters(kwargs["MinMergeSize"], kwargs["MaxMergeSize"], kwargs["MaxMergeEvents"])
        helper.save(pfn)
        result += self.detailsBackLink(requestName)
        return result
    handleAssignmentPage.exposed = True

    def modifyWorkload(self, requestName, workload, requestType,
                       runWhitelist=None, runBlacklist=None, blockWhitelist=None, blockBlacklist=None):
        """ handles the "Modify" button of the requestDetails page """
        
        if workload == None or not os.path.exists(workload):
            raise RuntimeError, "Cannot find workload " + workload
        helper = WMWorkloadHelper()
        helper.load(workload)
        schema = helper.data.request.schema
        message = ""
        #inputTask = helper.getTask(requestType).data.input.dataset
        if runWhitelist != "" and runWhitelist != None:
           l = parseRunList(runWhitelist)
           schema.RunWhitelist = l
           helper.setRunWhitelist(l)
           message += 'Changed runWhiteList to %s<br>' % l
        if runBlacklist != "" and runBlacklist != None:
           l = parseRunList(runBlacklist)
           schema.RunBlacklist = l
           helper.setRunBlacklist(l)
           message += 'Changed runBlackList to %s<br>' % l
        if blockWhitelist != "" and blockWhitelist != None:
           l = parseBlockList(blockWhitelist)
           schema.BlockWhitelist = l
           helper.setBlockWhitelist(l)
           message += 'Changed blockWhiteList to %s<br>' % l
        if blockBlacklist != "" and blockBlacklist != None:
           l = parseBlockList(blockBlacklist)
           schema.BlockBlacklist = l
           helper.setBlockBlacklist(l)
           message += 'Changed blockBlackList to %s<br>' % l
        helper.save(workload)
        return message + self.detailsBackLink(requestName)
    modifyWorkload.exposed = True

    def parseSite(self, kw, name):
        """ puts site whitelist & blacklists into nice format"""
        value = kw.get(name, [])
        if value == None:
            value = []
        if not isinstance(value, list):
            value = [value]
        return value
