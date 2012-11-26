#!/usr/bin/env python
"""
Main Module for browsing and modifying requests
as done by the dataOps operators to assign requests
to processing sites.

Handles site whitelist/blacklist info as well
"""
import types
import copy
import logging
import cherrypy
import threading

import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement as ProdManagement
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrAuth import ReqMgrAuth
import WMCore.RequestManager.OpsClipboard.Inject as OpsClipboard
import WMCore.Lexicon
from WMCore.Wrappers import JsonWrapper

from WMCore.WebTools.WebAPI import WebAPI

class Assign(WebAPI):
    """ Used by data ops to assign requests to processing sites"""
    def __init__(self, config, noSiteDB = False):
        """
        _init_

        Note, noSiteDB added for TESTING PURPOSED ONLY!
        """
        WebAPI.__init__(self, config)
        ReqMgrAuth.assign_roles = config.security_roles
        # Take a guess
        self.templatedir = config.templates
        self.couchUrl = config.couchUrl
        self.clipboardDB = config.clipboardDB
        cleanUrl = Utilities.removePasswordFromUrl(self.couchUrl)
        self.clipboardUrl = "%s/%s/_design/OpsClipboard/index.html" % (cleanUrl, self.clipboardDB)
        self.opshold = config.opshold
        self.configDBName = config.configDBName
        self.wmstatWriteURL = "%s/%s" % (self.couchUrl.rstrip('/'), config.wmstatDBName)
        if not noSiteDB:
            self.sites = Utilities.sites(config.sitedb)
        else:
            self.sites = []
        self.allMergedLFNBases =  [
            "/store/backfill/1", "/store/backfill/2",
            "/store/data",  "/store/mc", "/store/generator", "/store/relval",
            "/store/hidata"]
        self.allUnmergedLFNBases = ["/store/unmerged", "/store/temp"]

        self.mergedLFNBases = {
             "ReReco" : ["/store/backfill/1", "/store/backfill/2", "/store/data", "/store/hidata"],
             "DataProcessing" : ["/store/backfill/1", "/store/backfill/2", "/store/data", "/store/hidata"],
             "ReDigi" : ["/store/backfill/1", "/store/backfill/2", "/store/data", "/store/mc"],
             "MonteCarlo" : ["/store/backfill/1", "/store/backfill/2", "/store/mc"],
             "RelValMC" : ["/store/backfill/1", "/store/backfill/2", "/store/mc"],
             "Resubmission" : ["/store/backfill/1", "/store/backfill/2", "/store/mc", "/store/data", "/store/hidata"],
             "MonteCarloFromGEN" : ["/store/backfill/1", "/store/backfill/2", "/store/mc"],
             "TaskChain": ["/store/backfill/1", "/store/backfill/2", "/store/mc", "/store/data", "/store/relval"],
             "LHEStepZero": ["/store/backfill/1", "/store/backfill/2", "/store/generator"]}

        self.yuiroot = config.yuiroot
        cherrypy.engine.subscribe('start_thread', self.initThread)

        self.wildcardKeys = getattr(config, 'wildcardKeys', {'T1*': 'T1_*',
                                                             'T2*': 'T2_*',
                                                             'T3*': 'T3_*'})
        self.wildcardSites = {}
        Utilities.addSiteWildcards(self.wildcardKeys, self.sites, self.wildcardSites)

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
        #Accept Json encoded strings
        decodedArgs = {}
        for key in kwargs.keys():
            try:
                decodedArgs[key] = JsonWrapper.loads(kwargs[key])
            except:
                #Probably wasn't JSON
                decodedArgs[key] = kwargs[key]
        kwargs = decodedArgs
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
                ChangeState.changeRequestStatus(requestName, 'rejected', wmstatUrl = self.wmstatWriteURL)
            else:
                assignments = GetRequest.getAssignmentsByName(requestName)
                if teams == [] and assignments == []:
                    raise cherrypy.HTTPError(400, "Must assign to one or more teams")
                self.assignWorkload(requestName, kwargs)
                for team in teams:
                    if not team in assignments:
                        ChangeState.assignRequest(requestName, team, wmstatUrl = self.wmstatWriteURL)
                priority = kwargs.get(requestName+':priority', '')
                if priority != '':
                    Utilities.changePriority(requestName, priority, self.wmstatWriteURL)
        participle=kwargs['action']+'ed'
        if self.opshold and kwargs['action'] == 'Assign':
            participle='put into "ops-hold" state (see <a href="%s">OpsClipboard</a>)' % self.clipboardUrl
            # this, previously used, call made all requests injected into OpsClipboard to
            # have campaign_id null since the call doesn't propagate request's
            # CampaignName at all, AcquisitionEra remains default null and probably
            # a bunch of other things is wrong too
            #requests = [GetRequest.getRequestByName(requestName) for requestName in requestNames]
            requests = [Utilities.requestDetails(requestName) for requestName in requestNames]
            OpsClipboard.inject(self.couchUrl, self.clipboardDB, *requests)
            for request in requestNames:
                ChangeState.changeRequestStatus(requestName, 'ops-hold', wmstatUrl = self.wmstatWriteURL)

        return self.templatepage("Acknowledge", participle=participle, requests=requestNames)


    def assignWorkload(self, requestName, kwargs):
        """ Make all the necessary changes in the Workload to reflect the new assignment """
        request = GetRequest.getRequestByName(requestName)
        helper = Utilities.loadWorkload(request)
        for field in ["AcquisitionEra", "ProcessingVersion"]:
            if type(kwargs[field]) == dict:
                for value in kwargs[field].values():
                    self.validate(value, field)
            else:
                self.validate(kwargs[field], field)
        # Set white list and black list
        whiteList = kwargs.get("SiteWhitelist", [])
        blackList = kwargs.get("SiteBlacklist", [])
        helper.setSiteWildcardsLists(siteWhitelist = whiteList, siteBlacklist = blackList,
                                     wildcardDict = self.wildcardSites)
        # Set ProcessingVersion and AcquisitionEra, which could be json encoded dicts
        helper.setProcessingVersion(kwargs["ProcessingVersion"])
        helper.setAcquisitionEra(kwargs["AcquisitionEra"])
        #FIXME not validated
        helper.setLFNBase(kwargs["MergedLFNBase"], kwargs["UnmergedLFNBase"])
        helper.setMergeParameters(int(kwargs.get("MinMergeSize", 2147483648)),
                                  int(kwargs.get("MaxMergeSize", 4294967296)),
                                  int(kwargs.get("MaxMergeEvents", 50000)))
        helper.setupPerformanceMonitoring(int(kwargs.get("maxRSS", 2411724)),
                                          int(kwargs.get("maxVSize", 2411724)),
                                          int(kwargs.get("SoftTimeout", 171600)),
                                          int(kwargs.get("GracePeriod", 300)))
        # Set phedex subscription information
        custodialList = kwargs.get("CustodialSites", [])
        nonCustodialList = kwargs.get("NonCustodialSites", [])
        if "AutoApprove" in kwargs:
            autoApproveList = nonCustodialList
        else:
            autoApproveList = []
        priority = kwargs.get("Priority", "Low")
        if priority not in ["Low", "Normal", "High"]:
            raise cherrypy.HTTPError(400, "Invalid subscription priority")

        helper.setSubscriptionInformationWildCards(wildcardDict = self.wildcardSites,
                                                   custodialSites = custodialList,
                                                   nonCustodialSites = nonCustodialList,
                                                   autoApproveSites = autoApproveList,
                                                   priority = priority)
        helper.setDashboardActivity(kwargs.get("dashboard", ""))
        Utilities.saveWorkload(helper, request['RequestWorkflow'], self.wmstatWriteURL)
