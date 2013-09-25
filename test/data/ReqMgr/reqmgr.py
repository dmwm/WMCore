#!/usr/bin/env python

"""
Request Manager service (ReqMgr) injection script.

This script, with appropriate input, replaces a few Ops scripts:
vocms23:/data/cmst1/CMSSW_4_1_8_patch1/src/mc_test/testbed/
    make_mc_gen_request.py
    make_rereco_skim.py
    make_redigi_request2.py
    make_mc_lhe_request.py
 
The script shall have no WMCore libraries dependency.

Command line interface: --help

There are mandatory command line arguments (e.g. URL of the Request Manager)

Production ConfigCache: https://cmsweb.cern.ch/couchdb/reqmgr_config_cache/

-----------------------------------------------------------------------------
Notes to request arguments JSON file:

Parameters with values "*-OVERRIDE-ME" are supposed to be defined (overridden)
by a user on the command line, whichever other argument can be overridden too.

// TODO
// this is passing into a webpage checkbox HTML element (hence "checked" value")
// needs to be replaced by a proper boolean argument in the REST API
// "Team"+team: "checked" (the same for "checkbox"+workflow: "checked")

"""


import os
import sys
from httplib import HTTPSConnection, HTTPConnection
import urllib
import logging
from optparse import OptionParser, TitledHelpFormatter
import json
import copy


class RESTClient(object):
    """
    HTTP client
    HTTPS client based on the provided URL (http:// or https://)
    
    """
    def __init__(self, url, cert=None, key=None):
        logging.info("RESTClient URL: %s" % url)        
        if url.startswith("https://"):
            logging.info("Using HTTPS protocol, getting user identity files ...")
            proxyFile = "/tmp/x509up_u%s" % os.getuid()
            if not os.path.exists(proxyFile):
                proxyFile = "UNDEFINED" 
            certFile = cert or os.getenv("X509_USER_CERT",
                                         os.getenv("X509_USER_PROXY", proxyFile)) 
            keyFile = key or os.getenv("X509_USER_KEY",
                                       os.getenv("X509_USER_PROXY", proxyFile)) 
            logging.info("Identity files:\n\tcert file: '%s'\n\tkey file:  '%s' " %
                         (certFile, keyFile))
            url = url.replace("https://", '')
            logging.info("Creating connection HTTPS ...")
            self.conn = HTTPSConnection(url, key_file=keyFile, cert_file=certFile)
        if url.startswith("http://"):
            logging.info("Using HTTP protocol, creating HTTP connection ...")
            url = url.replace("http://", '')
            self.conn = HTTPConnection(url)
            
            
    def httpRequest(self, verb, uri, data=None, headers=None):
        logging.info("Request: %s %s ..." % (verb, uri))
        if headers:
            self.conn.request(verb, uri, data, headers)
        else:
            self.conn.request(verb, uri, data)
        resp = self.conn.getresponse()
        data = resp.read()
        logging.debug("Status: %s" % resp.status)
        logging.debug("Reason: %s" % resp.reason)
        return resp.status, data
            
        

class ReqMgrClient(RESTClient):
    """
    Client REST interface to Request Manager service (ReqMgr).
    
    Actions: queryRequests, deleteRequests, createRequest, assignRequests,
             cloneRequest, allTest, userGroup', team,
               
    """
    def __init__(self, reqMgrUrl, config):
        self.textHeaders  =  {"Content-type": "application/x-www-form-urlencoded",
                              "Accept": "text/plain"}        
        logging.info("ReqMgr url: %s" % reqMgrUrl)
        RESTClient.__init__(self, reqMgrUrl, cert=config.cert, key=config.key)
                

    def _createRequestViaRest(self, requestArgs):
        """
        Talks to the REST ReqMgr API.
        
        """
        logging.info("Injecting a request for arguments (REST API):\n%s ..." % requestArgs["createRequest"])
        jsonArgs = json.dumps(requestArgs["createRequest"])
        status, data = self.httpRequest("PUT", "/reqmgr/reqMgr/request", data=jsonArgs)        
        if status > 216:
            logging.error("Error occurred, exit.")
            print data
            sys.exit(1)
        data = json.loads(data)
        # ReqMgr returns dictionary with key: 'WMCore.RequestManager.DataStructs.Request.Request'
        # print data
        requestName = data.values()[0]["RequestName"] 
        logging.info("Create request '%s' succeeded." % requestName)
        return requestName
                

    def _createRequestViaWebPage(self, requestArgs):
        """
        Talks to the ReqMgr webpage, as if the request came from the web browser.
        
        """
        encodedParams = urllib.urlencode(requestArgs["createRequest"])
        logging.info("Injecting a request for arguments (webpage):\n%s ..." % requestArgs["createRequest"])
        # the response is now be an HTML webpage
        status, data = self.httpRequest("POST", "/reqmgr/create/makeSchema",
                                         data=encodedParams, headers=self.textHeaders)        
        if status > 216 and status != 303:
            logging.error("Error occurred, exit.")
            print data
            sys.exit(1)
        # this is a call to a webpage/webform and the response here is HTML page
        # retrieve the request name from the returned HTML page       
        requestName = data.split("'")[1].split('/')[-1]
        logging.info("Create request '%s' succeeded." % requestName)
        return requestName
    
    
    def createRequest(self, config, restApi = True):
        """
        requestArgs - arguments for both creation and assignment
        restApi - call REST API at ReqMgr or request creating webpage
        
        """
        if restApi:
            requestName = self._createRequestViaRest(config.requestArgs)
        else:
            requestName = self._createRequestViaWebPage(config.requestArgs)
        self.approveRequest(requestName)
        if config.assignRequests or config.changeSplitting:
            # if --assignRequests or --changeSplitting at the same time, it will be checking requestNames
            config.requestNames = [requestName]
        return requestName
        

    def approveRequest(self, requestName):
        """
        Set request status assignment-approved of the requestName request.
        Once ReqMgr provides proper API for status settings, esp. for assignment,
        a single method setStates shall handle all request status changes.
        
        """
        params = {"requestName": requestName,
                  "status": "assignment-approved"}
        encodedParams = urllib.urlencode(params)
        logging.info("Approving request '%s' ..." % requestName)
        status, data = self.httpRequest("PUT", "/reqmgr/reqMgr/request",
                                        data=encodedParams, headers=self.textHeaders)
        if status != 200:
            logging.error("Approve did not succeed.")
            print data
            sys.exit(1)
        logging.info("Approve succeeded.")
            
            
    def assignRequests(self, config):
        """
        It seems that the assignment doens't have proper REST API.
        Do via web page (as in the original script).
        This is why the items
            "action": "Assign"
            "Team"+team: "checked"
            "checkbox"+workflow: "checked"
            have to be hacked here, as if they were ticked on a web form.
            This hack is the reason why the requestArgs have to get
            to this method deep-copied if subsequent request assignment happens.
        This must pass the arguments JSON encoded since some of them can be complex types
        such as JSON objects/python dictionaries.
        """
        def doAssignRequest(assignArgs, requestName):
            assignArgs["action"] = "Assign"        
            team = assignArgs["Team"]
            assignArgs["Team" + team] = "checked"
            assignArgs["checkbox" + requestName] = "checked"
            # have to remove this one, otherwise it will get confused with "Team+team"
            # TODO this needs to be put right with proper REST interface
            del assignArgs["Team"]
            jsonEncodedParams = {}
            for paramKey in assignArgs.keys():
                jsonEncodedParams[paramKey] = json.dumps(assignArgs[paramKey])
            encodedParams = urllib.urlencode(jsonEncodedParams, True)
            logging.info("Assigning request '%s' ..." % requestName)
            status, data = self.httpRequest("POST", "/reqmgr/assign/handleAssignmentPage",
                                            data=encodedParams, headers=self.textHeaders)
            if status != 200:
                logging.error("Assign did not succeed.")
                print data
                sys.exit(1)
            logging.info("Assign succeeded.")

        for requestName in config.requestNames:
            assignArgs = copy.deepcopy(config.requestArgs["assignRequest"])
            doAssignRequest(assignArgs, requestName)
        

    def changeSplitting(self, config):
        """
        Change the splitting for a task and request using the splitting
        page, there is no REST API for this so stick to the page for now.
        """

        def changeSplittingForTask(requestName, taskName,
                                   splittingAlgo, splittingArgs):
            splittingParams = {}
            splittingParams.update(splittingArgs)
            splittingParams['requestName'] = requestName
            splittingParams['splittingTask'] = '/%s/%s' % (requestName, taskName)
            splittingParams['splittingAlgo'] = splittingAlgo
            encodedParams = urllib.urlencode(splittingParams, True)
            logging.info("Changing splitting parameters for request '%s' and task '%s' ..." % (requestName, taskName))
            status, data = self.httpRequest("POST", "/reqmgr/view/handleSplittingPage",
                                            data=encodedParams, headers=self.textHeaders)

            if status != 200:
                logging.error("Splitting change did not succeed.")
                print data
                sys.exit(1)
            logging.info("Splitting change succeeded.")

        for requestName in config.requestNames:
            splittingArgs = config.requestArgs["changeSplitting"]
            for taskName in splittingArgs:
                taskInfo = splittingArgs[taskName]
                changeSplittingForTask(requestName, taskName,
                                       taskInfo["SplittingAlgo"], taskInfo)

    def userGroup(self, _):
        """
        List all groups and users registered with Request Manager.
        
        """
        logging.info("Querying registered groups ...")
        status, data = self.httpRequest("GET", "/reqmgr/reqMgr/group")
        groups = json.loads(data)
        logging.info(data)
        logging.info("Querying registered users ...")
        status, data = self.httpRequest("GET", "/reqmgr/reqMgr/user")
        logging.info(data)
        logging.info("Querying groups membership ...")
        for group in groups:
            status, data = self.httpRequest("GET", "/reqmgr/reqMgr/group/%s" % group)
            logging.info("Group: '%s': %s" % (group, data))
            
    
    def team(self, _):
        logging.info("Querying registered teams ...")
        status, data = self.httpRequest("GET", "/reqmgr/reqMgr/team")
        groups = json.loads(data)
        logging.info(data)
            
            
    def queryRequests(self, config, toQuery=None):
        """
        If toQuery and config.requestNames are not specified, then
        all requests in the system are queried.
        toQuery - particular request name to query.
        config.requestNames - list of requests to query.
        
        Returns a list of requests in either case..
        
        """  
        if toQuery:
            requestsToQuery = [toQuery]
        else:
            requestsToQuery = config.requestNames
            
        requestsData = []
        if requestsToQuery:
            for requestName in requestsToQuery:
                logging.info("Querying '%s' request ..." % requestName)
                status, data = self.httpRequest("GET", "/reqmgr/reqMgr/request/%s" % requestName)
                if status != 200:
                    print data
                    sys.exit(1)           
                request = json.loads(data)
                for k, v in sorted(request.items()):
                    print "\t%s: %s" % (k, v)
                requestsData.append(request)
            # returns data on requests in the same order as in the config.requestNames
            return requestsData
        else:
            logging.info("Querying all requests ...")
            status, data = self.httpRequest("GET", "/reqmgr/reqMgr/request")
            if status != 200:
                print data
                sys.exit(1)
            requests = json.loads(data)
            keys = ("RequestName", "AcquisitionEra", "RequestType", "Requestor",
                    "RequestType", "RequestStatus")
            for request in requests:
                type = request["type"]
                r = request[type]
                print " ".join(["%s: '%s'" % (k, r[k]) for k in keys])
            logging.info("%s requests in the system." % len(requests))
            return requests
            

    def deleteRequests(self, config):
        for requestName in config.requestNames:
            logging.info("Deleting '%s' request ..." % requestName)
            status, data = self.httpRequest("DELETE", "/reqmgr/reqMgr/request/%s" % requestName)
            if status != 200:
                print data
                sys.exit(1)
            logging.info("Done.")           

    
    def cloneRequest(self, config):
        requestName = config.cloneRequest
        logging.info("Cloning request '%s' ..." % requestName)
        headers = {"Content-Length": 0}
        status, data = self.httpRequest("PUT", "/reqmgr/reqMgr/clone/%s" % requestName, 
                                         headers=headers)
        if status > 216:
            logging.error("Error occurred, exit.")
            print data  
            sys.exit(1)
        data = json.loads(data)
        # ReqMgr returns dictionary with key: 'WMCore.RequestManager.DataStructs.Request.Request'
        # print data
        newRequestName = data.values()[0]["RequestName"] 
        logging.info("Clone request succeeded: original request name: '%s' "
                     "new request name: '%s'" % (requestName, newRequestName))
        return newRequestName
    
    
    def changePriority(self, requestName, priority):
        """
        Test changing request priority.
        It's not exposed to the command line usage, it's used only in allTests()
        
        """
        logging.info("Changing request priority: %s for %s ..." % (priority, requestName))
        # this approach should also be possible:
        # jsonSender.put("request/%s?priority=%s" % (requestName, priority))
        # "requestName": requestName can probably be specified here as well
        params = {"priority": "%s" % priority}
        encodedParams = urllib.urlencode(params)
        status, data = self.httpRequest("PUT", "/reqmgr/reqMgr/request/%s" % requestName,
                                        data=encodedParams, headers=self.textHeaders)
        if status > 200:
            logging.error("Error occurred, exit.")
            print data
            sys.exit(1)

    def testResubmission(self, config):
        """
        The resubmission requests are taking as input name of an already
        existing request, that is OriginalRequestName.
        And another necessary argument is InitialTaskPath.
        Example for a original MonteCarlo request Resubmission values:
            OriginalRequestName:
                maxa_RequestString-OVERRIDE-ME_130213_115608_2550
            InitialTaskPath:
                /maxa_RequestString-OVERRIDE-ME_130213_115608_2550/Production/ProductionMergeRAWSIMoutput
                
        1) create MonteCarlo request and save name
        2) create Resubmission request using the name from 1)
        3) compare Campaign fields, shall be the same
        
        Modifies config.requestNames
        """
        originalRequest = self.createRequest(config, restApi = True)
        config.requestNames.append(originalRequest)
        config.requestArgs["createRequest"]["RequestType"]
        origMCRequestArgs = config.requestArgs["createRequest"]
        resubmissionArgs = {"createRequest":
                                {"OriginalRequestName": originalRequest,
                                 "InitialTaskPath": "/" + originalRequest + "/Production/ProductionMergeRAWSIMoutput",
                                 "RequestType": "Resubmission",
                                 "TimePerEvent": origMCRequestArgs["TimePerEvent"],
                                 "Memory": origMCRequestArgs["Memory"],
                                 "SizePerEvent": origMCRequestArgs["SizePerEvent"],
                                 "ACDCServer": "https://cmsweb-testbed.cern.ch/couchdb",
                                 "ACDCDatabase": "acdcserver",
                                 "Requestor": origMCRequestArgs["Requestor"],
                                 "Group": origMCRequestArgs["Group"]
                                }
                           }  
        # have to call the underlying method directly to sneak the
        # Resubmission request arguments directly
        resubmissionRequest = self._createRequestViaRest(resubmissionArgs)
        # returns a list
        origReqData = self.queryRequests(config, toQuery=resubmissionRequest)
        origReqData = origReqData[0]
        msg = "Campaign in the Resubmission request not matching the original request."
        assert config.requestArgs["createRequest"]["Campaign"] == origReqData["Campaign"]
        config.requestNames.append(resubmissionRequest)
        logging.info("Resubmission tests finished.")
        
        
    def getCouchDbConnectionAndUri(self, config):
        if config.reqMgrUrl.startswith("https://"):
            couchDbConn = RESTClient(config.reqMgrUrl,
                                     cert=config.cert, key=config.key)
            uri = "/couchdb/reqmgr_workload_cache"
        if config.reqMgrUrl.startswith("http://"):
            # take COUCHURL env. variable
            # if it contains username:password@host, then remove)
            couchUrl = os.getenv("COUCHURL", None)
            url = couchUrl
            if couchUrl.find('@') > -1:
                indexAt = couchUrl.find('@')
                indexSlash = couchUrl.find("//")
                url = couchUrl[:(indexSlash+2)]
                url += couchUrl[(indexAt+1):] 
            couchDbConn = RESTClient(url)
            uri = "/reqmgr_workload_cache"
        return couchDbConn, uri
        
    
    def checkCouchDb(self, config):
        """
        Returns number of request documents in the ReqMgr CouchDB database.
        Design documents are excluded from the result.
        Number of request in MySQL/Oracle and CouchDB database should match.
        Assumptions:
            if config.reqMgrUrl starts with "https://", then running against
                either CMS web instance of other VM deployment in which case
                the ReqMgr CouchDB database is:
                config.reqMgrUrl/couchdb/reqmgr_workload_cache/
            if config.reqMgrUrl starts with "http://", then it's assumed
                running against localhost in which case:
                get $COUCHURL/reqmgr_workload_cache/
                
        This check is purely for the fact that ReqMgr on DELETE request call
        deletes requests from MySQL/Oracle but not from CouchDB (#4289).
        
        This check will also be removed for ReqMgr2 having only CouchDB backend.
                
        """
        couchDbConn, uri = self.getCouchDbConnectionAndUri(config)
                                             
        # get number of all documents
        status, data = couchDbConn.httpRequest("GET", uri)
        numAllDocs = json.loads(data)["doc_count"]
        # get number of design documents (to exlude from the result)
        uri += "/_all_docs?startkey=\"_design/\"&endkey=\"_design0\""
        status, data = couchDbConn.httpRequest("GET", uri)
        numDesignDocs = len(json.loads(data)["rows"])
        return numAllDocs-numDesignDocs
    
    
    def checkOracleCouchDbConsistency(self, config, testRequestName):
        """
        Compare consistency of selected request data fields between
        Oracle and CouchDB.
        Compare data returned by "GET", "/reqmgr/reqMgr/request/REQUEST_NAME
        which leads to Utilities.requestDetails(requestName) which pulls
        information from Oracle and from spec stored in Couch attachment
        and data fields from Couch request.
        
        """
        # TODO 1:
        # this list is not exhaustive and will be modified / amended
        # should be checked if the request parameter actually exists in
        # in both databases ... later this explicit list should be removed
        # and check will be done on automatic inspection and later it will
        # be removed altogether and Oracle dropped ...
        # implement this check automatically without listing request arguments
        # TODO 2:
        # double list: OutputDatasets (ticket already filed ...)
        # Oracle:InputDatasetTypes: '{u'/QCD_HT-1000ToInf_TuneZ2star_8TeV-madgraph-pythia6/Summer12-START50_V13-v1/GEN': u'source'}' != CouchDB:InputDatasetTypes: '{}'
        # Oracle:RequestNumEvents: '0' != CouchDB:RequestNumEvents: 'None'
        fields = """RequestStatus
            RequestSizeFiles
            AcquisitionEra
            SoftwareVersions
            TimePerEvent
            CMSSWVersion
            Campaign
            ConfigCacheUrl
            CouchDBName
            CouchURL
            CouchWorkloadDBName
            GlobalTag
            Group
            InputDatasets
            Memory
            ProcessingVersion
            RequestDate
            RequestName
            RequestString
            RequestType
            Requestor
            RequestorDN
            ScramArch
            SiteWhitelist
            RequestWorkflow
            RequestPriority
            PrepID
            SizePerEvent
            RequestNumEvents
            """.split()
                        
        # data mainly from Oracle and spec
        reqOracle = self.queryRequests(None, testRequestName)[0]
        couchDbConn, uri = self.getCouchDbConnectionAndUri(config)
        status, data = couchDbConn.httpRequest("GET", uri + "/" + testRequestName)
        reqCouch = json.loads(data)
        # TODO
        # data fields should be automatically inspected rather than listed
        # above
        for field in fields:
            def check(request, fieldName, databaseType):
                try:
                    request[fieldName]
                    return True
                except KeyError:
                    print ("ERROR: Field '%s' doesn't exist in %s database." %
                       (fieldName, databaseType))
                    return False
            
            if not check(reqOracle, field, "Oracle"): continue
            if not check(reqCouch, field, "CouchDB"): continue
            
            msg = ("ERROR: Oracle:%s: '%s' != CouchDB:%s: '%s'" %
                    (field, reqOracle[field], field, reqCouch[field]))
            assert str(reqOracle[field]) == str(reqCouch[field]), msg
            
            
    def checkCouchRequestFields(self, config, requestName):
        """
        Method checks data fields in Couch stored requests.
        Method is called whenever there is a new request created.
        
        """
        print "Checking CouchDB parameters on stored request %s" % requestName
        # request parameters (fields) not allowed in Couch request document
        deprecatedArgs = ["ReqMgrGroupID",
                          "ReqMgrRequestID",
                          "ReqMgrRequestorID",
                          "ReqMgrRequestBasePriority",
                          "WorkflowSpec",
                          "RequestSizeEvents",
                          "RequestEventSize"]
        # request parameters (fields) which are mandatory on Couch request doc
        # and must be non-empty
        requiredArgs = ["RequestName",
                        "RequestWorkflow",
                        "RequestType",
                        "RequestStatus",
                        "RequestPriority",
                        "Requestor",
                        "Group",
                        "SizePerEvent",
                        "RequestSizeFiles",
                        "RequestNumEvents"
                        ]
        # request parameters (fields) which are optional
        optionalArgs = ["PrepID", "DbsUrl"]
               
        couchDbConn, uri = self.getCouchDbConnectionAndUri(config)
        status, data = couchDbConn.httpRequest("GET", uri + "/" + requestName)
        request = json.loads(data)
        
        for arg in deprecatedArgs:
            try:
                request[arg]
                print ("Request %s has forbidden parameter: %s" %
                       (requestName, arg))
                sys.exit(1)
            except KeyError:
                pass
        for arg in requiredArgs:
            try:
                val = request[arg]
                if val == None or val == '' or val == "null" or val == "None":
                    print ("Request %s has parameter %s but is unset: %s" %
                           (requestName, arg, val))
                    sys.exit(1)
            except KeyError:
                print ("Request %s doesn't have required parameter: %s" %
                       (requestName, arg))
        for arg in optionalArgs:
            try:
                val = request[arg]
            except KeyError:
                print ("Request %s doesn't have optional parameter defined: %s" %
                       (requestName, arg))
                
        print "CouchDB parameters OK."
        
            
    def allTests(self, config):
        """
        Call all methods above. Tests everything.
        Checks that the ReqMgr instance has the same state before 
        and after this script.
                
        """
        self.userGroup(None) # argument has no meaning
        self.team(None) # argument has no meaning
        
        # save number of current requests in the system 
        currentRequests = self.queryRequests(config)
        currentCouchRequests = self.checkCouchDb(config)
        msg = ("Prior to allTests(): Number of requests in MySQL/Oracle "
               "database (%s) and CouchDB (%s) do not agree." %
               (len(currentRequests), currentCouchRequests))
        assert len(currentRequests) == currentCouchRequests, msg
        
        # save the first created request name (testRequestName) for some later tests
        testRequestName = self.createRequest(config, restApi = True)
        self.checkCouchRequestFields(config, testRequestName)
        config.requestNames = []
        config.requestNames.append(testRequestName)
        # change the splitting before assigning
        self.changeSplitting(config)
        # normally assignRequests() is called when config.assignRequests = True
        # flag is set, here it's called explicitly
        self.assignRequests(config)
        # TODO - hack
        # TaskChain request type doesn't have web GUI, can't then test
        # web GUI call for that.
        if config.requestArgs["createRequest"]["RequestType"] != "TaskChain":
            config.requestNames.append(self.createRequest(config, restApi = False))
            self.assignRequests(config)
            self.checkCouchRequestFields(config, config.requestNames[-1])
    
        # test priority changing. setting priority is absolute now,
        # the sent value becomes the final priority, there is no composition
        # config.requestNames must be set
        newPriority = 11212
        self.changePriority(testRequestName, newPriority)
        testRequestData = self.queryRequests(config, toQuery=testRequestName)[0]
        # test state (should be "assigned"
        msg = "Status should be 'assigned', is '%s'" % testRequestData["RequestStatus"]
        assert testRequestData["RequestStatus"] == "assigned", msg
        assert testRequestData["RequestPriority"] == newPriority, "New RequestPriority does not match!"
        
        # take testRequestName for Oracle, CouchDB consistency check
        # this request had status, priority modified, so it also tests whether
        # it has been properly propagate into Couch corresponding document
        self.checkOracleCouchDbConsistency(config, testRequestName) 
        
        # test clone
        config.cloneRequest = testRequestName
        clonedRequestName = self.cloneRequest(config)
        config.requestNames.append(clonedRequestName)
        # now test that the cloned request has correct priority
        clonedRequest = self.queryRequests(config, toQuery=clonedRequestName)[0]
        msg = ("Priorities don't match: original request: %s cloned request: %s" %
               (newPriority, clonedRequest["RequestPriority"]))
        assert newPriority == clonedRequest["RequestPriority"], msg
        msg = ("DbsUrl don't match: original request %s cloned request: %s" %
               (testRequestData.get("DbsUrl"), clonedRequest.get("DbsUrl")))
        assert testRequestData.get("DbsUrl") == clonedRequest.get("DbsUrl"), msg
        
        # test Resubmission request, only if we have MonteCarlo request template in input
        if config.requestArgs["createRequest"]["RequestType"] == "MonteCarlo":
            logging.info("MonteCarlo request in input detected, running Resubmission test ...")
            # shall update config.requestNames for final cleanup 
            self.testResubmission(config)        
        
        # final touches, checks and clean-up
        self.deleteRequests(config) # takes config.requestNames 
        logging.info("%s requests in the system before this test." % len(currentRequests))        
        config.requestNames = None # this means queryRequests will check all requests
        afterRequests = self.queryRequests(config)
        logging.info("%s requests in the system before this test." % len(afterRequests))
        assert currentRequests == afterRequests, "Requests in ReqMgr before and after this test not matching!"        
        afterCouchRequests = self.checkCouchDb(config)
        msg = ("After allTests(): Number of requests in MySQL/Oracle "
               "database (%s) and CouchDB (%s) do not agree." %
               (len(afterRequests), afterCouchRequests))
        assert len(afterRequests) == afterCouchRequests, msg
        
        logging.info("Running --allTests succeeded.")
                
    
    def __del__(self):
        self.conn.close()
        del self.conn
    

# ---------------------------------------------------------------------------    


def processCmdLine(args):
    def errExit(msg, parser):
        print('\n')
        parser.print_help()
        print("\n\n%s" % msg)
        sys.exit(1)
        
    form = TitledHelpFormatter(width=78)
    parser = OptionParser(usage="usage: %prog options", formatter=form, add_help_option=None)
    actions = defineCmdLineOptions(parser)
    # opts - new processed options
    # args - remainder of the input array
    opts, args = parser.parse_args(args=args)
    # check command line arguments validity
    if not opts.reqMgrUrl:
        errExit("Missing mandatory --reqMgrUrl.", parser)
    if opts.createRequest and not opts.configFile:
        errExit("When --createRequest, --configFile is necessary.", parser)
    if opts.changeSplitting and not opts.createRequest and not opts.configFile:
        errExit("Without --createRequest, --configFile must be specified for --changeSplitting.", parser)
    if opts.changeSplitting and not opts.createRequest and not opts.requestNames:
        errExit("Without --createRequest, --requestNames must be supplied to --changeSplitting.", parser)
    if opts.assignRequests and not opts.createRequest and not opts.configFile:
        errExit("Without --createRequest, --configFile must be specified for --assignRequests.", parser)
    if opts.assignRequests and not opts.createRequest and not opts.requestNames:
        errExit("Without --createRequest, --requestNames must be supplied to --assignRequests.", parser)
    if not opts.requestNames and (opts.queryRequests or opts.deleteRequests or \
                                  (opts.assignRequests and not opts.createRequest) or \
                                  (opts.changeSplitting and not opts.createRequest)):
        errExit("--requestNames must be supplied.", parser)
    if opts.createRequest and opts.requestNames:
        errExit("--requestNames can't be provided with --createRequest", parser)
    if opts.allTests and not opts.configFile:
        errExit("When --allTests, --configFile is necessary", parser)
    if (opts.json and not opts.createRequest) and (opts.json and not opts.allTests) \
        and (opts.json and not opts.assignRequests) and (opts.json and not opts.changeSplitting):
        errExit("--json only with --createRequest, --allTests, --assignRequest, --changeSplitting", parser)
    for action in filter(lambda name: getattr(opts, name), actions):
        if opts.allTests and action and action != "allTests":
            errExit("Arguments --allTests and --%s mutually exclusive." % action, parser)
    if opts.requestNames:
        # make it a list here
        opts.requestNames = opts.requestNames.split(',')
    return opts, actions


def defineCmdLineOptions(parser):
    actions = []
    # "-h" ------------------------------------------------------------------
    help = "Display this help"
    parser.add_option("-h", "--help", help=help, action='help')
    # "-c" ------------------------------------------------------------------
    help = ("User cert file (or cert proxy file). "
            "If not defined, tries X509_USER_CERT then X509_USER_PROXY env. "
            "variables. And lastly /tmp/x509up_uUID.")
    parser.add_option("-c", "--cert", help=help)    
    # "-k" ------------------------------------------------------------------
    help = ("User key file (or cert proxy file). "
            "If not defined, tries X509_USER_KEY then X509_USER_PROXY env. "
            "variables. And lastly /tmp/x509up_uUID.")
    parser.add_option("-k", "--key", help=help)
    # -u --------------------------------------------------------------------
    help = ("Request Manager service address (if not options is supplied, "
            "returns a list of the requests in ReqMgr) "
            "e.g.: https://maxareqmgr01.cern.ch")
    parser.add_option("-u", "--reqMgrUrl", help=help)
    # -f --------------------------------------------------------------------
    help = "Request create and/or assign arguments config file."
    parser.add_option("-f", "--configFile", help=help)
    # -j --------------------------------------------------------------------
    help = ("JSON string to override values from --configFile. "
            "e.g. --json=\'{\"createRequest\": {\"Requestor\": \"efajardo\"}, "
            "\"assignRequest\": {\"FirstLumi\": 1}}\' "
            "e.g. --json=`\"cat alan.json\"`")
    parser.add_option("-j", "--json", help=help)
    # -r --------------------------------------------------------------------
    help = ("Request name or list of comma-separated names to perform "
            "actions upon.")
    parser.add_option("-r", "--requestNames", help=help)
    # -q --------------------------------------------------------------------
    help = "Action: Query request(s) specified by --requestNames."
    action = "queryRequests"
    actions.append(action)
    parser.add_option("-q", "--" + action, action="store_true", help=help)
    # -d --------------------------------------------------------------------
    help = "Action: Delete request(s) specified by --requestNames."
    action = "deleteRequests"
    actions.append(action)
    parser.add_option("-d", "--" + action, action="store_true", help=help)
    # -i --------------------------------------------------------------------
    help = ("Action: Create and inject a request. Whichever from the config "
            "file defined arguments can be overridden from "
            "command line and a few have to be so (*-OVERRIDE-ME ending). "
            "Depends on --configFile. "
            "This request can be 'approved' and 'assigned' if --assignRequests.")
    action = "createRequest"
    actions.append(action)  
    parser.add_option("-i", "--" + action, action="store_true", help=help)
    # TODO
    # once ReqMgr has proper REST API for assign, then implement --setStates
    # taking a list of states to route requests through
    # -p ---------------------------------------------------------------------\
    help = "Action: Change splitting parameters for tasks in a request."
    action = "changeSplitting"
    actions.append(action)
    parser.add_option("-p", "--" + action, action="store_true", help=help)
    # -s --------------------------------------------------------------------
    help = ("Action: Approve and assign request(s) specified by --requestNames "
            "or a new request when used with --createRequest. "
            "Depends on --requestNames and --configFile when used without "
            "--createRequest")
    action = "assignRequests"
    actions.append(action)
    parser.add_option("-g", "--" + action, action="store_true", help=help)
    # -l --------------------------------------------------------------------
    help = "Action: Clone request, the request name is mandatory argument."
    action = "cloneRequest"
    actions.append(action)
    parser.add_option("-l", "--" + action, help=help)
    # -a --------------------------------------------------------------------
    help = ("Action: Perform all operations this script allows. "
            "--configFile and possibly --json must be present for initial "
            "request injection and assignment.")
    action = "allTests"
    actions.append(action)
    parser.add_option("-a", "--" + action, action="store_true", help=help)
    # -s --------------------------------------------------------------------
    # TODO
    # this will be removed once ReqMgr takes this internal user management
    # information from SiteDB, only teams will remain
    help = "Action: List groups and users registered with ReqMgr instance."
    action = "userGroup"
    actions.append(action)
    parser.add_option("-s", "--" + action,  action="store_true", help=help)
    # -t --------------------------------------------------------------------
    help = "Action: List teams registered with a Request Manager instance."
    action = "team"
    actions.append(action)
    parser.add_option("-t", "--" + action,  action="store_true", help=help)
    # -v ---------------------------------------------------------------------\
    help = "Verbose console output."
    parser.add_option("-v", "--verbose",  action="store_true", help=help)    
    return actions

def processRequestArgs(intputConfigFile, commandLineJson):
    """
    Load request arguments from a file, blend with JSON from command line.
    
    """
    logging.info("Loading file '%s' ..." % intputConfigFile)
    try:
        requestArgs = json.load(open(intputConfigFile, 'r'))
    except IOError as ex:
        logging.fatal("Reading arguments file '%s' failed, "
                      "reason: %s." % (intputConfigFile, ex))
        sys.exit(1)
    if commandLineJson:
        logging.info("Parsing request arguments on the command line ...")
        cliJson = json.loads(commandLineJson)
        # if a key exists in cliJson, update values in the main requestArgs dict
        for k in requestArgs.keys():
            if cliJson.has_key(k):
                requestArgs[k].update(cliJson[k])            
    else:
        logging.warn("No request arguments to override (--json)? Some values will be wrong.")
        
    # iterate over all items recursively and warn about those ending with 
    # OVERRIDE-ME, hence not overridden
    def check(items):
        for k, v in items:
            if isinstance(v, dict):
                check(v.items())
            if isinstance(v, unicode) and v.endswith("OVERRIDE-ME"):
                logging.warn("Not properly set: %s: %s" % (k, v))
    check(requestArgs.items())
    return requestArgs
        
    
def initialization(commandLineArgs):
    print("Processing command line arguments: '%s' ..." % commandLineArgs)
    config, actions = processCmdLine(commandLineArgs)
    logging.basicConfig(level=logging.DEBUG if config.verbose else logging.INFO)
    logging.debug("Set verbose console output.")
    reqMgrClient = ReqMgrClient(config.reqMgrUrl, config)
    if config.createRequest or config.assignRequests or config.changeSplitting or config.allTests:
        # process request arguments and store them
        config.requestArgs = processRequestArgs(config.configFile, config.json)
    return reqMgrClient, config, actions
    

def main():
    reqMgrClient, config, definedActions = initialization(sys.argv)
    # definedAction are all actions as defined for CLI
    # there is now gonna be usually 1 action to perform, but could be more
    # filter out those where config.ACTION is None
    # config is all options for this script but also request creation parameters
    actions = filter(lambda name: getattr(config, name), definedActions)
    logging.info("Actions to perform: %s" % actions) 
    for action in actions:
        logging.info("Performing '%s' ..." % action)
        # some methods need to modify config (e.g. add a request name),
        # pass them entire configuration
        reqMgrClient.__getattribute__(action)(config)
    if not actions:
        reqMgrClient.queryRequests(config)
        
    
if __name__ == "__main__":
    main()
