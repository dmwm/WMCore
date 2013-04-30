"""
Main Module for browsing and modifying requests.

"""


import WMCore.RequestManager.RequestDB.Settings.RequestStatus as RequestStatus
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Wrappers import JsonWrapper
import WMCore.Lexicon
import logging
import cherrypy
import threading
from WMCore.WebTools.WebAPI import WebAPI
import cgi

def detailsBackLink(requestName):
    """ HTML to return to the details of this request """
    return  ' <a href="details/%s">Details</A> <a href=".">Browse</A><BR>' % requestName

def linkedTableEntry(methodName, entry):
    """ Makes an HTML table entry with the entry name, and a link to a
    method with that entry as an argument"""
    return '<a href="%s/%s">%s</a>' % (methodName, entry, entry)

def statusMenu(requestName, defaultField):
    """ Makes an HTML menu for setting the status """
    html = defaultField + '&nbsp<select name="%s:status"> <option></option>' % requestName
    for field in RequestStatus.NextStatus[defaultField]:
        html += '<option>%s</option>' % field
    html += '</select>'
    return html

def biggestUpdate(field, request):
    """ Finds which of the updates has the biggest number """
    biggest = 0
    for update in request["RequestUpdates"]:
        if update.has_key(field):
            biggest = update[field]
    return "%i%%" % biggest

class ReqMgrBrowser(WebAPI):
    """ For browsing and modifying requests """
    def __init__(self, config):
        WebAPI.__init__(self, config)
        # Take a guess
        self.templatedir = config.templates
        self.fields = ['RequestName', 'Group', 'Requestor', 'RequestType',
                       "RequestPriority", 'RequestStatus', 'Complete', 'Success']
        self.calculatedFields = {'Written': 'percentWritten', 'Merged':'percentMerged',
                                 'Complete':'percentComplete', 'Success' : 'percentSuccess'}
        # entries in the table that show up as HTML links for that entry
        self.linkedFields = {'Group': '../admin/group',
                             'Requestor': '../admin/user',
                             'RequestName': 'details'}
        self.detailsFields = ['RequestName', 'RequestType', 'Requestor', 'CMSSWVersion',
            'ScramArch', 'GlobalTag', 'RequestNumEvents',
            'InputDataset', 'PrimaryDataset', 'AcquisitionEra', 'ProcessingVersion',
            'RunWhitelist', 'RunBlacklist', 'BlockWhitelist', 'BlockBlacklist',
            'RequestWorkflow', 'Scenario', 'Campaign', 'PrimaryDataset',
            'Acquisition Era', 'Processing Version', 'Merged LFN Base', 'Unmerged LFN Base',
            'Site Whitelist', 'Site Blacklist']

        self.adminMode = True
        self.adminFields = {}
        self.couchUrl = config.couchUrl
        self.configDBName = config.configDBName
        self.workloadDBName = config.workloadDBName
        self.yuiroot = config.yuiroot
        self.wmstatWriteURL = "%s/%s" % (self.couchUrl.rstrip('/'), config.wmstatDBName)
        self.acdcURL = "%s/%s" % (self.couchUrl.rstrip('/'), config.acdcDBName)
        cherrypy.engine.subscribe('start_thread', self.initThread)

    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    def validate(self, v, name=''):
        """ Checks if alphanumeric, tolerating spaces """
        try:
            WMCore.Lexicon.identifier(v)
        except AssertionError:
            raise cherrypy.HTTPError(400, "Bad input %s" % name)
        return v

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def search(self, value, field):
        """ Search for a regular expression in a certain field of all requests """
        filteredRequests = []
        requests = GetRequest.getRequests()
        for request in requests:
            if request[field].find(value) != -1:
                filteredRequests.append(request)
        requests = filteredRequests
        tableBody = self.drawRequests(requests)
        return self.templatepage("ReqMgrBrowser", yuiroot=self.yuiroot,
                                 fields=self.fields, tableBody=tableBody)
        
    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def index(self):
        requests = GetRequest.getRequests()
        tableBody = self.drawRequests(requests)
        return self.templatepage("ReqMgrBrowser", yuiroot=self.yuiroot,
                                 fields=self.fields, tableBody=tableBody)
        

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def splitting(self, requestName):
        """
        _splitting_

        Retrieve the current values for splitting parameters for all tasks in
        the spec.  Format them in the manner that the splitting page expects
        and pass them to the template.
        """
        self.validate(requestName)
        request = GetRequest.getRequestByName(requestName)
        helper = Utilities.loadWorkload(request)
        splittingDict = helper.listJobSplittingParametersByTask(performance = False)
        taskNames = splittingDict.keys()
        taskNames.sort()

        splitInfo = []
        for taskName in taskNames:
            jsonSplittingParams = JsonWrapper.dumps(splittingDict[taskName])
            splitInfo.append({"splitAlgo": splittingDict[taskName]["algorithm"],
                              "splitParams": jsonSplittingParams,
                              "taskType": splittingDict[taskName]["type"],
                              "taskName": taskName})

        return self.templatepage("Splitting", requestName = requestName,
                                 taskInfo = splitInfo, taskNames = taskNames)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
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
            splitParams["files_per_job"] = int(submittedParams["files_per_job"])
        elif splittingAlgo == "TwoFileBased":
            splitParams["files_per_job"] = int(submittedParams["two_files_per_job"])
        elif splittingAlgo == "LumiBased":
            splitParams["lumis_per_job"] = int(submittedParams["lumis_per_job"])
            if str(submittedParams["halt_job_on_file_boundaries"]) == "True":
                splitParams["halt_job_on_file_boundaries"] = True
            else:
                splitParams["halt_job_on_file_boundaries"] = False
        elif splittingAlgo == "EventAwareLumiBased":
            splitParams["events_per_job"] = int(submittedParams["avg_events_per_job"])
            splitParams["max_events_per_lumi"] = int(submittedParams["max_events_per_lumi"])
            if str(submittedParams["halt_job_on_file_boundaries_event_aware"]) == "True":
                splitParams["halt_job_on_file_boundaries"] = True
            else:
                splitParams["halt_job_on_file_boundaries"] = False
        elif splittingAlgo == "EventBased":
            splitParams["events_per_job"] = int(submittedParams["events_per_job"])
            if submittedParams.has_key("events_per_lumi"):
                splitParams["events_per_lumi"] = int(submittedParams["events_per_lumi"])
            if "lheInputFiles" in submittedParams:
                if str(submittedParams["lheInputFiles"]) == "True":
                    splitParams["lheInputFiles"] = True
                else:
                    splitParams["lheInputFiles"] = False
        elif splittingAlgo == "Harvest":
            splitParams["periodic_harvest_interval"] = int(submittedParams["periodic_harvest_interval"])
        elif 'Merg' in splittingTask:
            for field in ['min_merge_size', 'max_merge_size', 'max_merge_events', 'max_wait_time']:
                splitParams[field] = int(submittedParams[field])
        if "include_parents" in submittedParams.keys():
            if str(submittedParams["include_parents"]) == "True":
                splitParams["include_parents"] = True
            else:
                splitParams["include_parents"] = False

        self.validate(requestName)
        request = GetRequest.getRequestByName(requestName)
        helper = Utilities.loadWorkload(request)
        logging.info("SetSplitting " + requestName + splittingTask + splittingAlgo + str(splitParams))
        helper.setJobSplittingParameters(splittingTask, splittingAlgo, splitParams)
        Utilities.saveWorkload(helper, request['RequestWorkflow'])
        return "Successfully updated splitting parameters for " + splittingTask \
               + " " + detailsBackLink(requestName)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def details(self, requestName):
        """ A page showing the details for the requests """
        self.validate(requestName)
        try:
            request = Utilities.requestDetails(requestName)
        except AssertionError:
            raise cherrypy.HTTPError(404, "Cannot load request %s" % requestName)
        adminHtml = statusMenu(requestName, request['RequestStatus']) \
                  + ' Priority: ' + Utilities.priorityMenu(request)
        return self.templatepage("Request", requestName=requestName,
                                detailsFields=self.detailsFields,
                                requestSchema=request,
                                docId=request.get('ConfigCacheID', None),
                                assignments=request['Assignments'],
                                adminHtml=adminHtml,
                                messages=request['RequestMessages'],
                                updateDictList=request['RequestUpdates'])
        
        
    def _getConfigCache(self, requestName, processMethod):
        try:
            request = Utilities.requestDetails(requestName)
        except Exception, ex:
            msg = "Cannot find request %s, check logs." % requestName
            logging.error("%s, reason: %s" % (msg, ex))
            return msg
        url = request.get("ConfigCacheUrl", None) or self.couchUrl
        try:
            configCache = ConfigCache(url, self.configDBName)
            configDocId = request["ConfigCacheID"]
            configCache.loadByID(configDocId)
        except Exception, ex:
            msg = "Cannot find ConfigCache document %s on %s." % (configDocId, url)
            logging.error("%s, reason: %s" % (msg, ex))
            return msg
        return getattr(configCache, processMethod)()
            

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def showOriginalConfig(self, requestName):
        """
        Makes a link to the original text of the config document.
        
        """
        self.validate(requestName)
        return '<pre>' + self._getConfigCache(requestName, "getConfig") + '</pre>'


    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def showTweakFile(self, requestName):
        """
        Makes a link to the dump of the tweakfile.
        
        """
        self.validate(requestName)
        tweakString = self._getConfigCache(requestName, "getPSetTweaks")
        return str(tweakString).replace('\n', '<br>')


    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def showWorkload(self, requestName):
        """ Displays the workload """
        self.validate(requestName)

        try:
            request = GetRequest.getRequestByName(requestName)
        except (Exception, RuntimeError) as ex:
            raise cherrypy.HTTPError(400, "Invalid request.")

        request = Utilities.prepareForTable(request)
        helper = Utilities.loadWorkload(request)
        workloadText = str(helper.data)
        return cgi.escape(workloadText).replace("\n", "<br/>\n")

    def drawRequests(self, requests):
        """ Display all requests """
        result = ""
        for request in requests:
            # see if this is slower
            result += self.drawRequest(request)
        return result

    def drawRequest(self, request):
        """ make a table row with information from this request """
        html = '<tr>'
        requestName = request['RequestName']
        for field in self.fields:
            # hanole any fields that have functions linked to them
            entry = cgi.escape(str(request.get(field, '')))
            if field in self.calculatedFields:
                method = getattr(self, self.calculatedFields[field])
                entry = method(request)
            elif self.adminMode and self.adminFields.has_key(field):
                method = getattr(self, self.adminFields[field])
                entry = method(requestName, value)
            elif self.linkedFields.has_key(field):
                entry = linkedTableEntry(self.linkedFields[field], entry)
            html += '<td>%s</td>' % entry
        html += '</tr>\n'
        return html

    def percentWritten(self, request):
        """ Finds the biggest percentage among all the updates """
        maxPercent = 0
        for update in request["RequestUpdates"]:
            if update.has_key("events_written") and request["RequestNumEvents"] != 0:
                percent = update["events_written"] / request["RequestNumEvents"]
                if percent > maxPercent:
                    maxPercent = percent
            if update.has_key("files_written") and request["RequestSizeFiles"] != 0:
                percent = update["files_written"] / request["RequestSizeFiles"]
                if percent > maxPercent:
                    maxPercent = percent
        return "%i%%" % maxPercent

    def percentMerged(self, request):
        """ Finds the biggest percentage among all the updates """
        maxPercent = 0
        for update in request["RequestUpdates"]:
            if update.has_key("events_merged") and request["RequestNumEvents"] != 0:
                percent = update["events_merged"] / request["RequestNumEvents"]
                if percent > maxPercent:
                    maxPercent = percent
            if update.has_key("files_merged") and request["RequestSizeFiles"] != 0:
                percent = update["files_merged"] / request["RequestSizeFiles"]
                if percent > maxPercent:
                    maxPercent = percent
        return "%i%%" % maxPercent

    def percentComplete(self, request):
        pct = request.get('percent_complete', 0)
        return "%i%%" % pct

    def percentSuccess(self, request):
        pct = request.get('percent_success', 0)
        return "%i%%" % pct

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())    
    def doAdmin(self, **kwargs):
        """  format of kwargs is {'requestname:status' : 'approved', 'requestname:priority' : '2'} """
        message = ""
        for k, v in kwargs.iteritems():
            if k.endswith(':status'):
                requestName = k.split(':')[0]
                self.validate(requestName)
                status = v
                priority = kwargs[requestName+':priority']
                if priority != '':
                    Utilities.changePriority(requestName, priority, self.wmstatWriteURL)
                    message += "Changed priority for %s to %s.\n" % (requestName, priority)
                if status != "":
                    Utilities.changeStatus(requestName, status, self.wmstatWriteURL, self.acdcURL)
                    message += "Changed status for %s to %s\n" % (requestName, status)
                    if status == "assigned":
                        # make a page to choose teams
                        raise cherrypy.HTTPRedirect('/reqmgr/assign/one/%s' % requestName)
        return message + detailsBackLink(requestName)


    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    # FIXME needs to check if authorized, or original user
    def modifyWorkload(self, requestName, workload,
                       CMSSWVersion=None, GlobalTag=None,
                       runWhitelist=None, runBlacklist=None,
                       blockWhitelist=None, blockBlacklist=None,
                       ScramArch=None):
        """ handles the "Modify" button of the details page """
        self.validate(requestName)
        helper = WMWorkloadHelper()
        helper.load(workload)
        schema = helper.data.request.schema
        message = ""
        if runWhitelist != "" and runWhitelist != None:
            l = Utilities.parseRunList(runWhitelist)
            helper.setRunWhitelist(l)
            schema.RunWhitelist = l
            message += 'Changed runWhiteList to %s<br>' % l
        if runBlacklist != "" and runBlacklist != None:
            l = Utilities.parseRunList(runBlacklist)
            helper.setRunBlacklist(l)
            schema.RunBlacklist = l
            message += 'Changed runBlackList to %s<br>' % l
        if blockWhitelist != "" and blockWhitelist != None:
            l = Utilities.parseBlockList(blockWhitelist)
            helper.setBlockWhitelist(l)
            schema.BlockWhitelist = l
            message += 'Changed blockWhiteList to %s<br>' % l
        if blockBlacklist != "" and blockBlacklist != None:
            l = Utilities.parseBlockList(blockBlacklist)
            helper.setBlockBlacklist(l)
            schema.BlockBlacklist = l
            message += 'Changed blockBlackList to %s<br>' % l
        Utilities.saveWorkload(helper, workload)
        return message + detailsBackLink(requestName)
