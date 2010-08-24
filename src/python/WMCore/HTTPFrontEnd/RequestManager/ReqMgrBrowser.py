import WMCore.RequestManager.RequestDB.Settings.RequestStatus as RequestStatus
from WMCore.Cache.ConfigCache import WMConfigCache
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from PSetTweaks.PSetTweak import PSetHolder, PSetTweak
from WMCore.Services.Requests import JSONRequests
import WMCore.HTTPFrontEnd.RequestManager.Sites
#from WMCore.Agent.Harness import Harness
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
        self.fields = ['RequestName', 'Group', 'Requestor', 'RequestType', 'ReqMgrRequestBasePriority', 'ReqMgrRequestorBasePriority', 'ReqMgrGroupBasePriority', 'RequestStatus', 'Complete', 'Success']
        self.calculatedFields = {'Written': 'percentWritten', 'Merged':'percentMerged', 'Complete':'percentComplete', 'Success' : 'percentSuccess'}
        self.linkedFields = {'RequestName':'requestDetails'}
        self.adminMode = True
        self.adminFields = {'RequestStatus':'statusMenu', 'ReqMgrRequestBasePriority':'priorityMenu'}
        self.requests = []
        configCacheUrl = config.configCacheUrl
        self.configCache = WMConfigCache('reqmgr', configCacheUrl)
        self.workloadDir = config.workloadCache
        self.jsonSender = JSONRequests(config.reqMgrHost)
        self.sites = WMCore.HTTPFrontEnd.RequestManager.Sites.sites()


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
        
    def requestDetails(self, requestName):
        result = ""
        request = self.jsonSender.get("/reqMgr/request/"+requestName)[0]
        helper = WMWorkloadHelper()
        pfn = os.path.join(self.workloadDir, request['RequestWorkflow'])
        helper.load(pfn)

        docId = None
        try:
            # Header consists of links to orig. config, tweakfile,
            # and a command to remake the Workload
            # request['Configuration']
            config = helper.data.tasks.Production.steps.cmsRun1.application.command.configuration
            # see if it's in CouchDBURL/docId format
            parts = config.split('/')
            if len(parts) > 1:
                docId = parts[1]
        except:
            pass
        d = helper.data.request.schema.dictionary_()
        d['RequestWorkflow'] = request['RequestWorkflow']
        self.addHtmlLinks(d)
        assignments= self.jsonSender.get('/reqMgr/assignment?request='+requestName)[0]
        adminHtml = self.statusMenu(requestName, request['RequestStatus']) \
                  + ' Priority ' + self.priorityMenu(requestName, request['ReqMgrRequestBasePriority'])
        return self.templatepage("Request", requestSchema=d,
                                workloadDir = self.workloadDir, 
                                docId=docId, assignments=assignments,
                                adminHtml = adminHtml,
                                messages=request['RequestMessages'],
                                updateDictList=request['RequestUpdates'])
                                 
        return result
    requestDetails.exposed = True

    def showOriginalConfig(self, docId):
        configString =  self.configCache.getOriginalConfigByDocID(docId)
        return '<pre>' + configString  + '</pre>'
    showOriginalConfig.exposed = True

    def showFullConfig(self, docId):
        configString = str(pickle.loads(self.configCache.getConfigByDocID(docId)))
        return '<pre>' + configString  + '</pre>'
    showFullConfig.exposed = True

    def showTweakFile(self, docId):
        return str(pickle.loads(self.configCache.getTweakFileByDocID(docId))).replace('\n', '<br>')
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
            request = self.jsonSender.get("/reqMgr/request/"+request["RequestName"])[0]
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
        return self.biggestUpdate('percent_complete', request)

    def percentSuccess(self, request):
        return self.biggestUpdate('percent_success', request)

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
        # get assignments
        assignments = self.jsonSender.get('/reqMgr/assignment?request=%s' % requestName)[0]
        # might be a list, or a dict team:priority
        if isinstance(assignments, dict):
            assignments = assignments.keys()
        return self.templatepage("Assign", requestName=requestName, teams=teams, 
                   assignments=assignments, sites=self.sites)
    assignmentPage.exposed = True
    
    def handleAssignmentPage(self, *args, **kwargs):
        """ handles some checkboxes """
        result = ""
        requestName = kwargs["requestName"]
        request = self.jsonSender.get("/reqMgr/request/"+requestName)[0]
        helper = WMWorkloadHelper()
        pfn = os.path.join(self.workloadDir, request['RequestWorkflow'])
        helper.load(pfn)
        schema = helper.data.request.schema
        for key, value in kwargs.iteritems():
           if key == "siteWhitelist":
               result += "Site Whitelist changed to " + str(value) + "<BR>"
               helper.setSiteWhitelist(value)
               schema.SiteWhitelist = value
               helper.save(pfn)
           elif key == "siteBlacklist":
               result += "Site Blacklist changed to " + str(value) + "<BR>"
               helper.setSiteBlacklist(value)
               schema.SiteBlacklist = value
               helper.save(pfn)
           elif key == "requestName":
               pass
           elif value != None:
               #ChangeState.assignRequest(kwargs['requestName'], team) 
               self.jsonSender.put('/reqMgr/assignment/%s/%s' % (urllib.quote(key), requestName))
               result += "Assigned " + requestName + " to " + key  + "<BR>"
        result += self.detailsBackLink(requestName)
        return result
    handleAssignmentPage.exposed = True

    def modifyWorkload(self, requestName, workload, requestType, runWhitelist=None, runBlacklist=None, blockWhitelist=None, blockBlacklist=None):
        if workload == None or not os.path.exists(workload):
            raise RuntimeError, "Cannot find workload " + workload
        helper = WMWorkloadHelper()
        helper.load(workload)
        schema = helper.data.request.schema
        message = ""
        #inputTask = helper.getTask(requestType).data.input.dataset
        if runWhitelist != "" and runWhitelist != None:
           l = eval("[%s]"%runWhitelist)
           schema.RunWhitelist = l
           helper.setRunWhitelist(l)
           message += 'Changed runWhiteList to ' + str(l)
        if runBlacklist != "" and runBlacklist != None:
           l = eval("[%s]"%runBlacklist)
           schema.RunBlacklist = l
           helper.setRunBlacklist(l)
           message += 'Changed runBlackList to ' + str(l)
        if blockWhitelist != "" and blockWhitelist != None:
           l = eval("[%s]"%blockWhitelist)
           schema.BlockWhitelist = l
           helper.setBlockWhitelist(l)
           message += 'Changed blockWhiteList to ' + str(l)
        if blockBlacklist != "" and blockBlacklist != None:
           l = eval("[%s]"%blockBlacklist)
           schema.BlockBlacklist = l
           helper.setBlockBlacklist(l)
           message += 'Changed blockBlackList to ' + str(l)
        helper.save(workload)
        return message + self.detailsBackLink(requestName)
    modifyWorkload.exposed = True
