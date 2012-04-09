#!/usr/bin/env python
""" Main Module for browsing and modifying requests """
import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement as ProdManagement
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrAuth import ReqMgrAuth
import WMCore.RequestManager.Clipboard.Insert as Clipboard
import WMCore.Lexicon
import logging
import cherrypy
import threading
import types

#from WMCore.WebTools.Page import TemplatedPage
from WMCore.WebTools.WebAPI import WebAPI

class Assign(WebAPI):
    """ Used by data ops to assign requests to processing sites"""
    def __init__(self, config):
        WebAPI.__init__(self, config)
        ReqMgrAuth.assign_roles = config.security_roles
        # Take a guess
        self.templatedir = config.templates
        self.couchUrl = config.couchUrl
        self.clipboardDB = config.clipboardDB
        cleanUrl = Utilities.removePasswordFromUrl(self.couchUrl)
        self.clipboardUrl = "%s/%s/_design/OpsClipboard/index.html" % (cleanUrl, self.clipboardDB)
        self.hold = config.hold
        self.configDBName = config.configDBName
        self.sites = Utilities.sites(config.sitedb)
        self.allMergedLFNBases =  [
            "/store/backfill/1", "/store/backfill/2", 
            "/store/data",  "/store/mc"]
        self.allUnmergedLFNBases = ["/store/unmerged", "/store/temp"]

        self.mergedLFNBases = {
             "ReReco" : ["/store/backfill/1", "/store/backfill/2", "/store/data"],
             "DataProcessing" : ["/store/backfill/1", "/store/backfill/2", "/store/data"],
             "ReDigi" : ["/store/backfill/1", "/store/backfill/2", "/store/data", "/store/mc"],
             "MonteCarlo" : ["/store/backfill/1", "/store/backfill/2", "/store/mc"],
             "RelValMC" : ["/store/backfill/1", "/store/backfill/2", "/store/mc"],
             "Resubmission" : ["/store/backfill/1", "/store/backfill/2", "/store/mc", "/store/data"],
             "MonteCarloFromGEN" : ["/store/backfill/1", "/store/backfill/2", "/store/mc"]}

        self.yuiroot = config.yuiroot
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
    @cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles)
    def one(self,  requestName):
        """ Assign a single request """
        self.validate(requestName)
        request =  GetRequest.getRequestByName(requestName)
        request = Utilities.prepareForTable(request)
        requestType = request["RequestType"]
        # get assignments
        teams = ProdManagement.listTeams()
        assignments = GetRequest.getAssignmentsByName(requestName)
        # might be a list, or a dict team:priority
        if isinstance(assignments, dict):
            assignments = assignments.keys()

        procVer = ""
        acqEra = ""
        helper = Utilities.loadWorkload(request)
        if helper.getAcquisitionEra() != None:
            acqEra = helper.getAcquisitionEra()
            if helper.getProcessingVersion() != None:
                procVer = helper.getProcessingVersion()
        dashboardActivity = helper.getDashboardActivity()

        (reqMergedBase, reqUnmergedBase) = helper.getLFNBases()
        return self.templatepage("Assign", requests=[request], teams=teams, 
                                 assignments=assignments, sites=self.sites,
                                 mergedLFNBases=self.mergedLFNBases[requestType],
                                 reqMergedBase=reqMergedBase,
                                 unmergedLFNBases=self.allUnmergedLFNBases,
                                 reqUnmergedBase=reqUnmergedBase,
                                 acqEra = acqEra, procVer = procVer, 
                                 dashboardActivity=dashboardActivity,
                                 badRequests=[])

    @cherrypy.expose    
    @cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles)
    def index(self, all=0):
        """ Main page """
        # returns dict of  name:id
        allRequests = Utilities.requestsWithStatus('assignment-approved')
        teams = ProdManagement.listTeams()

        procVer = ""
        acqEra = ""
        dashboardActivity = None
        badRequestNames = []
        goodRequests = []
        reqMergedBase = None
        reqUnmergedBase = None
        for request in allRequests:
            # make sure there's a workload attached
            try:
                helper = Utilities.loadWorkload(request)
            except Exception, ex:
                logging.error("Assign error: %s " % str(ex))
                badRequestNames.append(request["RequestName"])
            else:
                # get defaults from the first good one
                if not goodRequests:
                    # forget it if it fails.
                    try:
                        if helper.getAcquisitionEra() != None:
                            acqEra = helper.getAcquisitionEra()
                        if helper.getProcessingVersion() != None:
                            procVer = helper.getProcessingVersion()
                        (reqMergedBase, reqUnmergedBase) = helper.getLFNBases()
                        dashboardActivity = helper.getDashboardActivity()
                        goodRequests.append(request)
                    except Exception, ex:
                        logging.error("Assign error: %s " % str(ex))
                        badRequests.append(request["RequestName"])
                else:
                    goodRequests.append(request)
        return self.templatepage("Assign", all=all, requests=goodRequests, teams=teams,
                                 assignments=[], sites=self.sites,
                                 mergedLFNBases=self.allMergedLFNBases,
                                 reqMergedBase=reqMergedBase,
                                 unmergedLFNBases=self.allUnmergedLFNBases,
                                 reqUnmergedBase=reqUnmergedBase,
                                 acqEra = acqEra, procVer = procVer, 
                                 dashboardActivity=dashboardActivity,
                                 badRequests=badRequestNames)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles)
    def handleAssignmentPage(self, **kwargs):
        """ handler for the main page """
        # handle the checkboxes
        teams = []
        requestNames = []
        for key, value in kwargs.iteritems():
            if isinstance(value, types.StringTypes):
                kwargs[key] = value.strip()
            if key.startswith("Team"):
                teams.append(key[4:])
            if key.startswith("checkbox"):
                requestName = key[8:]
                self.validate(requestName)
                requestNames.append(key[8:])
        
        for requestName in requestNames:
            if kwargs['action'] == 'Reject':
                ChangeState.changeRequestStatus(requestName, 'rejected') 
            else:
                assignments = GetRequest.getAssignmentsByName(requestName)
                if teams == [] and assignments == []:
                    raise cherrypy.HTTPError(400, "Must assign to one or more teams")
                self.assignWorkload(requestName, kwargs)
                for team in teams:
                    if not team in assignments:
                        ChangeState.assignRequest(requestName, team)
                priority = kwargs.get(requestName+':priority', '')
                if priority != '':
                    Utilities.changePriority(requestName, priority)
        participle=kwargs['action']+'ed'
        if self.hold and kwargs['action'] == 'Assign':
            participle='put into hold state (see <a href="%s">clipboard</a>)' % self.clipboardUrl
            requests = [GetRequest.getRequestByName(requestName) for requestName in requestNames]
            Clipboard.inject(self.couchUrl, self.clipboardDB, *requests)
            for request in requestNames:
                ChangeState.changeRequestStatus(requestName, 'ops-hold')
        return self.templatepage("Acknowledge", participle=participle, requests=requestNames)

    def assignWorkload(self, requestName, kwargs):
        """ Make all the necessary changes in the Workload to reflect the new assignment """
        request = GetRequest.getRequestByName(requestName)
        helper = Utilities.loadWorkload(request)
        for field in ["AcquisitionEra", "ProcessingVersion"]:
            self.validate(kwargs[field], field)
        helper.setSiteWhitelist(Utilities.parseSite(kwargs,"SiteWhitelist"))
        helper.setSiteBlacklist(Utilities.parseSite(kwargs,"SiteBlacklist"))
        helper.setProcessingVersion(kwargs["ProcessingVersion"])
        helper.setAcquisitionEra(kwargs["AcquisitionEra"])
        #FIXME not validated
        helper.setLFNBase(kwargs["MergedLFNBase"], kwargs["UnmergedLFNBase"])
        helper.setMergeParameters(int(kwargs["MinMergeSize"]),
                                  int(kwargs["MaxMergeSize"]), 
                                  int(kwargs["MaxMergeEvents"]))
        helper.setupPerformanceMonitoring(int(kwargs["maxRSS"]), 
                                          int(kwargs["maxVSize"]))
        helper.setDashboardActivity(kwargs.get("dashboard", ""))
        Utilities.saveWorkload(helper, request['RequestWorkflow'])
 
         
