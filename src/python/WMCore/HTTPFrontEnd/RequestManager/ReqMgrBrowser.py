#!/usr/bin/env python
""" Main Module for browsing and modifying requests """

import WMCore.RequestManager.RequestDB.Settings.RequestStatus as RequestStatus
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import parseRunList, parseBlockList, parseSite, allSoftwareVersions
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.Cache.WMConfigCache import ConfigCache 
from WMCore.Services.Requests import JSONRequests
import WMCore.HTTPFrontEnd.RequestManager.Sites

import cherrypy
import json
import os.path
import urllib
import types

from WMCore.WebTools.Page import TemplatedPage

def detailsBackLink(requestName):
    """ HTML to return to the details of this request """
    return  ' <A HREF=requestDetails/%s>Details</A> <A HREF=".">Back</A><BR>' % requestName

def linkedTableEntry(methodName, entry):
    """ Makes an HTML table entry with the entry name, and a link to a
    method with that entry as an argument"""
    return '<a href="%s/%s">%s</a>' % (methodName, entry, entry)

def statusMenu(requestName, defaultField):
    """ Makes an HTML menu for setting the status """
    html = defaultField + '&nbsp<SELECT NAME="%s:status"> <OPTION></OPTION>' % requestName
    for field in RequestStatus.NextStatus[defaultField]:
        html += '<OPTION>%s</OPTION>' % field
    html += '</SELECT>'
    return html

def priorityMenu(requestName, defaultPriority):
    """ Returns HTML for a box to set priority """
    return '%s &nbsp<input type="text" size=2 name="%s:priority">' % (defaultPriority, requestName)

def biggestUpdate(field, request):
    """ Finds which of the updates has the biggest number """
    biggest = 0
    for update in request["RequestUpdates"]:
        if update.has_key(field):
            biggest = update[field]
    return "%i%%" % biggest

def addHtmlLinks(d):
    """ Any entry that starts with http becomes an HTML link """
    for key, value in d.iteritems():
        if isinstance(value, types.StringTypes) and value.startswith('http'):
            target = value
            # assume CVS browsers need extra tags
            if 'cvs' in target:
                target += '&view=markup'
            d[key] = '<a href="%s">%s</a>' % (target, value)


class ReqMgrBrowser(TemplatedPage):
    """ Main class for browsing and modifying requests """
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        # Take a guess
        self.templatedir = __file__.rsplit('/', 1)[0]
        self.urlPrefix = '%s/download/?filepath=' % config.reqMgrHost
        self.fields = ['RequestName', 'Group', 'Requestor', 'RequestType',
                       'ReqMgrRequestBasePriority', 'RequestStatus', 'Complete', 'Success']
        self.calculatedFields = {'Written': 'percentWritten', 'Merged':'percentMerged',
                                 'Complete':'percentComplete', 'Success' : 'percentSuccess'}
        # entries in the table that show up as HTML links for that entry
        self.linkedFields = {'Group':'group', 'Requestor':'user', 'RequestName':'requestDetails'}
        self.detailsFields = ['RequestName', 'RequestType', 'Requestor', 'CMSSWVersion',
                """ Main class for browsing and modifying requests """
            'ScramArch', 'GlobalTag', 'RequestSizeEvents',
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

    @cherrypy.expose
    def index(self):
        """ Main web page """
        requests = self.getRequests()
        print str(requests)
        tableBody = self.drawRequests(requests)
        return self.templatepage("ReqMgrBrowser", fields=self.fields, tableBody=tableBody)

    @cherrypy.expose
    def search(self, value, field):
        """ Search for a regular expression in a certain field of all requests """
        filteredRequests = []
        requests =  self.getRequests() 
        for request in requests:
            if request[field].find(value) != -1:
                filteredRequests.append(request)
        requests = filteredRequests
        tableBody = self.drawRequests(requests)
        return self.templatepage("ReqMgrBrowser", fields=self.fields, tableBody=tableBody)
        
    def getRequests(self):
        """ Get all requests """
        return self.jsonSender.get("/reqMgr/request")[0]

    @cherrypy.expose
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
        timeOutDict = helper.listTimeOutsByTask()
        taskNames = splittingDict.keys()
        taskNames.sort()

        splitInfo = []
        for taskName in taskNames:
            # We basically stringify the splitting params dictionary and pass
            # that to the splitting page as javascript.  We need to change
            # boolean values to strings as the javascript true is different from
            # the python True.  We'll also add the timeouts here.
            splittingDict[taskName]["timeout"] = timeOutDict[taskName]
            if "split_files_between_job" in splittingDict[taskName]:
                splittingDict[taskName]["split_files_between_job"] = str(splittingDict[taskName]["split_files_between_job"])
                
            splitInfo.append({"splitAlgo": splittingDict[taskName]["algorithm"],
                              "splitParams": str(splittingDict[taskName]),
                              "taskType": splittingDict[taskName]["type"],
                              "taskName": taskName})

        return self.templatepage("Splitting", requestName = requestName,
                                 taskInfo = splitInfo, taskNames = taskNames)
            
    @cherrypy.expose
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
        helper.setTaskTimeOut(splittingTask, int(submittedParams["timeout"]))
        helper.save(pfn)
        return "Successfully updated splitting parameters for " + splittingTask \
               + " " + detailsBackLink(requestName)

    @cherrypy.expose
    def requestDetails(self, requestName):
        """ A page showing the details for the requests """
        request = self.jsonSender.get("/reqMgr/request/"+requestName)[0]
        helper, pfn = self.workloadHelper(request)

        docId = None
        d = helper.data.request.schema.dictionary_()
        d['RequestWorkflow'] = request['RequestWorkflow']
        if d.has_key('ProdConfigCacheID') and d['ProdConfigCacheID'] != "":
            docId = d['ProdConfigCacheID']        
        addHtmlLinks(d)
        assignments = self.jsonSender.get('/reqMgr/assignment?request='+requestName)[0]
        adminHtml = statusMenu(requestName, request['RequestStatus']) \
                  + ' Priority ' + priorityMenu(requestName, request['ReqMgrRequestBasePriority'])
        return self.templatepage("Request", requestName=requestName,
                                detailsFields = self.detailsFields, requestSchema=d,
                                workloadDir = self.workloadDir, 
                                docId=docId, assignments=assignments,
                                adminHtml = adminHtml,
                                messages=request['RequestMessages'],
                                updateDictList=request['RequestUpdates'])
                                 

    @cherrypy.expose
    def showOriginalConfig(self, docId):
        """ Makes a link to the original text of the config """
        configCache = ConfigCache(self.configCacheUrl, self.configDBName)
        configCache.loadByID(docId)
        configString =  configCache.getConfig()
        if configString == None:
            return "Cannot find document " + str(docId) + " in Couch DB"
        return '<pre>' + configString + '</pre>'

    @cherrypy.expose
    def showTweakFile(self, docId):
        """ Makes a link to the dump of the tweakfile """
        configCache = ConfigCache(self.configCacheUrl, self.configDBName)
        configCache.loadByID(docId)
        return str(configCache.getPSetTweaks()).replace('\n', '<br>')

    @cherrypy.expose
    def showWorkload(self, filepath):
        """ Displays the workload """
        helper = WMWorkloadHelper()
        helper.load(filepath)
        return str(helper.data).replace('\n', '<br>')
 
    @cherrypy.expose
    def remakeWorkload(self, requestName):
        """ Rebuild the workload from the stored schema """
        request = self.jsonSender.get("/reqMgr/request/"+requestName)[0]
        # Should really get by RequestType
        workloadMaker = WorkloadMaker(requestName)
        # I'm getting requests and requestSchema confused
        workloadMaker.loadRequestSchema(request)
        workload = workloadMaker.makeWorkload()
        workloadCache = getWorkloadCache()
        request['RequestWorkflow'] = workloadCache.checkIn(workload)

    def drawRequests(self, requests):
        """ Display all requests """
        result = ""
        for request in requests:
            # see if this is slower
            #request = self.jsonSender.get("/reqMgr/request/"+request["RequestName"])[0]
            result += self.drawRequest(request)
        return result

    def drawRequest(self, request):
        """ make a table row with information from this request """
        html = '<tr>'
        requestName = request['RequestName']
        for field in self.fields:
            # hanole any fields that have functions linked to them
            html += '<td>'
            if self.adminMode and self.adminFields.has_key(field):
                method = getattr(self, self.adminFields[field])
                html += method(requestName, str(request[field]))
            elif self.linkedFields.has_key(field):
                html += linkedTableEntry(self.linkedFields[field], str(request[field]))
            elif self.calculatedFields.has_key(field):
                method = getattr(self, self.calculatedFields[field])
                html += method(request)
            else:
                html += str(request[field]) 
            html += '</td>'
        html += '</tr>\n'
        return html

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

    @cherrypy.expose
    def doAdmin(self, **kwargs):
        """  format of kwargs is {'requestname:status' : 'approved', 'requestname:priority' : '2'} """
        message = ""
        for k, v in kwargs.iteritems():
            if k.endswith(':status'): 
                requestName = k.split(':')[0]
                status = v
                priority = kwargs[requestName+':priority']
                if status != "" or priority != "":
                    message += self.updateRequest(requestName, status, priority)
        return message

    def updateRequest(self, requestName, status, priority):
        """ Changes the status or priority """
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
        return message + detailsBackLink(requestName)

    @cherrypy.expose
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
    
    def workloadHelper(self, request):
        """ Returns a WMWorkloadHelper and a pfn for the workload in the request """
        helper = WMWorkloadHelper()
        pfn = os.path.join(self.workloadDir, request['RequestWorkflow'])
        helper.load(pfn)
        return helper, pfn

    @cherrypy.expose
    def handleAssignmentPage(self, **kwargs):
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
            if isinstance(value, types.StringTypes):
                kwargs[key] = value.strip()
                setattr(schema, key, value.strip())
            if key.startswith("Team"):
                team = key[4:]
                if not team in assignments:
                    teams.append(team)
                    self.jsonSender.put('/reqMgr/assignment/%s/%s' % (urllib.quote(team), requestName))
                    result += "Assigned to team %s\n" % team
        if teams == [] and assignments == []:
            raise cherrypy.HTTPError(400, "Must assign to one or more teams")

        helper.setSiteWhitelist(parseSite(kwargs,"SiteWhitelist"))
        helper.setSiteBlacklist(parseSite(kwargs,"SiteBlacklist"))
        helper.setProcessingVersion(kwargs["ProcessingVersion"])
        helper.setAcquisitionEra(kwargs["AcquisitionEra"])
        helper.setLFNBase(kwargs["MergedLFNBase"], kwargs["UnmergedLFNBase"])
        helper.setMergeParameters(kwargs["MinMergeSize"], kwargs["MaxMergeSize"], kwargs["MaxMergeEvents"])
        helper.save(pfn)
        result += detailsBackLink(requestName)
        return result

    @cherrypy.expose
    def modifyWorkload(self, requestName, workload,
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
        return message + detailsBackLink(requestName)

    @cherrypy.expose
    def user(self, userName):
        """ Web page of details about the user, and sets user priority """
        userDict = json.loads(self.jsonSender.get('/reqMgr/user/%s' % userName)[0])
        requests = userDict['requests']
        priority = userDict['priority']
        groups = userDict['groups']
        allGroups = self.jsonSender.get('/reqMgr/group')[0]
        return self.templatepage("User", user=userName, groups=groups, 
            allGroups=allGroups, requests=requests, priority=priority)

    @cherrypy.expose
    def handleUserPriority(self, user, userPriority):
        """ Handles setting user priority """
        self.jsonSender.post('/reqMgr/user/%s?priority=%s' % (user, userPriority))
        return "Updated user %s priority to %s" % (user, userPriority)

    @cherrypy.expose
    def group(self, groupName):
        """ Web page of details about the user, and sets user priority """
        groupDict = json.loads(self.jsonSender.get('/reqMgr/group/%s' % groupName)[0])
        users = groupDict['users']
        priority = groupDict['priority']
        return self.templatepage("Group", group=groupName, users=users, priority=priority)

    @cherrypy.expose
    def handleGroupPriority(self, group=None, groupPriority=None):
        """ Handles setting group priority """
        self.jsonSender.post('/reqMgr/group/%s?priority=%s' % (group, groupPriority))
        return "Updated group %s priority to %s" % (group, groupPriority)

    @cherrypy.expose
    def users(self):
        """ Lists all users.  Should be paginated later """
        allUsers = self.jsonSender.get('/reqMgr/user')[0]
        return self.templatepage("Users", users=allUsers)

    @cherrypy.expose
    def handleAddUser(self, user, email=None):
        """ Handles setting user priority """
        self.jsonSender.put('/reqMgr/user/%s?email=%s' % (user, email))
        return "Added user %s" % user

    @cherrypy.expose
    def handleAddToGroup(self, user, group):
        """ Adds a user to the group """
        self.jsonSender.put('/reqMgr/group/%s/%s' % (group, user))
        return "Added %s to %s " % (user, group)

    @cherrypy.expose
    def groups(self):
        """ Lists all users.  Should be paginated later """
        allGroups = self.jsonSender.get('/reqMgr/group')[0]
        return self.templatepage("Groups", groups=allGroups)

    @cherrypy.expose
    def handleAddGroup(self, group):
        """ Handles adding a group """
        self.jsonSender.put('/reqMgr/group/%s' % group)
        return "Added group %s " % group

    @cherrypy.expose
    def teams(self):
        """ Lists all teams """
        teams = self.jsonSender.get('/reqMgr/team')[0]
        return self.templatepage("Teams", teams=teams)

    @cherrypy.expose
    def team(self, teamName):
        """ Details for a team """
        assignments = self.jsonSender.get('/reqMgr/assignment/%s' % teamName)[0]
        return self.templatepage("Team", team=teamName, requests=assignments.keys())

    @cherrypy.expose
    def handleAddTeam(self, team):
        """ Handles a request to add a team """
        self.jsonSender.put('/reqMgr/team/%s' % team)
        return "Added team %s" % team

    @cherrypy.expose
    def versions(self):
        """ Lists all versions """
        versions = self.jsonSender.get('/reqMgr/version')[0]
        versions.sort()
        return self.templatepage("Versions", versions=versions)

    @cherrypy.expose
    def handleAddVersion(self, version):
        """ Registers a version """
        self.jsonSender.put('/reqMgr/version/%s' % version)
        return "Added version %s" % version

    @cherrypy.expose
    def handleAllVersions(self):
        """ Registers all versions in the TC """
        currentVersions = self.jsonSender.get('/reqMgr/version')[0]
        allVersions = allSoftwareVersions()
        result = ""
        for version in allVersions:
            if not version in currentVersions:
               self.jsonSender.put('/reqMgr/version/%s' % version)
               result += "Added version %s<br>" % version
        if result == "":
            result = "Version list is up to date"
        return result

