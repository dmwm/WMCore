#!/usr/bin/env python

"""
Request Manager service (ReqMgr) injection script.

Currently developed for MonteCarlo workflow
original version:
[vocms23] /data/cmst1/CMSSW_4_1_8_patch1/src/mc_test/testbed/make_mc_gen_request.py
but it's generic enough to involve injection of other workflows. 

The script shall have no WMCore libraries dependency.

Command line interface: --help

The final injection request is a blend of:
    1) MonteCarlo.json
    2) user can override an arbitrary request argument on a command line (JSON)
        
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
import httplib
import urllib
import logging
from optparse import OptionParser, TitledHelpFormatter
import json
import copy


class ReqMgrSubmitter(object):
    """
    Client REST interface to Request Manager service (ReqMgr).
    
    Actions: 'queryRequest', 'deleteRequests', 'createRequest', 'userGroup', 'team'

    """
    def __init__(self, reqMgrUrl, certFile, keyFile):
        logging.info("Identity files:\n\tcert file: '%s'\n\tkey file:  '%s' " %
                     (certFile, keyFile))
        self.textHeaders  =  {"Content-type": "application/x-www-form-urlencoded",
                              "Accept": "text/plain"}
        if reqMgrUrl.startswith("https://"):
            reqMgrUrl = reqMgrUrl.replace("https://", '')
        self.conn = httplib.HTTPSConnection(reqMgrUrl, key_file = keyFile,
                                            cert_file = certFile)
        
        
    def _httpRequest(self, verb, uri, data = None, headers = None):
        logging.debug("Request: %s %s ..." % (verb, uri))
        if headers:
            self.conn.request(verb, uri, data, headers)
        else:
            self.conn.request(verb, uri, data)
        resp = self.conn.getresponse()
        data = resp.read()
        logging.info("Status: %s" % resp.status)
        logging.info("Reason: %s" % resp.reason)
        return resp.status, data
        

    def _createRequestViaRest(self, requestArgs):
        """
        Talks to the REST ReqMgr API.
        
        """
        logging.debug("Injecting a request for arguments (REST API):\n%s ..." % requestArgs["request"])
        jsonArgs = json.dumps(requestArgs["request"])
        status, data = self._httpRequest("PUT", "/reqmgr/reqMgr/request", data = jsonArgs)        
        if status > 216:
            logging.error("Error occurred, exit.")
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
        encodedParams = urllib.urlencode(requestArgs["request"])
        logging.debug("Injecting a request for arguments (webpage):\n%s ..." % requestArgs["request"])
        # the response is now be an HTML webpage
        status, data = self._httpRequest("POST", "/reqmgr/create/makeSchema",
                                         data = encodedParams, headers = self.textHeaders)        
        if status > 216 and not 303:
            logging.error("Error occurred, exit.")
            sys.exit(1)
        print data
        # this is a call to a webpage/webform and the response here is HTML page
        # retrieve the request name from the returned HTML page       
        requestName = data.split("'")[1].split('/')[-1]
        logging.info("Create request '%s' succeeded." % requestName)
        return requestName        


    def approveRequest(self, requestName):
        params = {"requestName": requestName,
                  "status": "assignment-approved"}
        encodedParams = urllib.urlencode(params)
        logging.info("Approving request '%s' ..." % requestName)
        status, data = self._httpRequest("PUT", "/reqmgr/reqMgr/request",
                                         data = encodedParams, headers = self.textHeaders)
        if status != 200:
            logging.error("Approve did not succeed.")
            print data
            sys.exit(1)
        logging.info("Approve succeeded.")
            
            
    def assignRequest(self, requestArgs, requestName):
        """
        It seems that the assignment doens't have proper REST API.
        Investigate further.
        Do via web page (as in the original script).
        This is why the items
            "Team"+team: "checked"
            "checkbox"+workflow: "checked"
            have to be hacked here, as if they were ticked on a web form.
            This hack is the reason why the requestArgs have to get
            to this method deep-copied if subsequent request injection happens
            
        TODO:
        if arguments for assigning will be similar/same for other workflows,
        they should be factored out into a dedicated JSON file.
        
        """
        assignArgs = requestArgs["requestAssign"]
        print "assignArgs in the method"
        print assignArgs
        team = assignArgs["Team"]
        assignArgs["Team" + team] = "checked"
        assignArgs["checkbox" + requestName] = "checked"
        # have to remove this one, otherwise it will get confused with "Team+team"
        # TODO this needs to be put right with proper REST interface
        del assignArgs["Team"]
        encodedParams = urllib.urlencode(assignArgs, True)
        logging.info("Assigning request '%s' ..." % requestName)
        status, data = self._httpRequest("POST", "/reqmgr/assign/handleAssignmentPage",
                                         data = encodedParams, headers = self.textHeaders)
        if status != 200:
            logging.error("Assign did not succeed.")
            print data
            sys.exit(1)
        logging.info("Assign succeeded.")
        
                
    def createRequest(self, requestArgs, restApi = True):
        """
        requestArgs - arguments for both creation and assignment
        restApi - call REST API at ReqMgr or request creating webpage
        
        """
        if restApi:
            requestName = self._createRequestViaRest(requestArgs)
        else:
            requestName = self._createRequestViaWebPage(requestArgs)
        
        self.approveRequest(requestName)
        self.assignRequest(requestArgs, requestName)
        return requestName
        
                
    def userGroup(self, _):
        """
        List all groups and users registered with Request Manager.
        
        """
        logging.info("Querying registered groups ...")
        status, data = self._httpRequest("GET", "/reqmgr/reqMgr/group")
        groups = json.loads(data)
        logging.info(data)
        logging.info("Querying registered users ...")
        status, data = self._httpRequest("GET", "/reqmgr/reqMgr/user")
        logging.info(data)
        logging.info("Querying groups membership ...")
        for group in groups:
            status, data = self._httpRequest("GET", "/reqmgr/reqMgr/group/%s" % group)
            logging.info("Group: '%s': %s" % (group, data))
            
    
    def team(self, _):
        logging.info("Querying registered teams ...")
        status, data = self._httpRequest("GET", "/reqmgr/reqMgr/team")
        groups = json.loads(data)
        logging.info(data)
            
            
    def queryRequest(self, requestName = None):
        if not requestName:
            logging.info("Querying all requests ...")
            status, data = self._httpRequest("GET", "/reqmgr/reqMgr/requestnames")
            requests = json.loads(data)
            for request in requests:
                print request
            logging.info("%s requests in the system." % len(requests))
            return requests
        else:
            logging.info("Querying '%s' request ...")
            status, data = self._httpRequest("GET", "/reqmgr/reqMgr/request/%s" % requestName)            
            request = json.loads(data)
            for k, v in sorted(request.items()):
                print "\t%s: %s" % (k, v)
            

    def deleteRequests(self, requestNames):
        """
        requestName is already a list
        
        """
        assert isinstance(requestNames, list), "Wrong input, list expected (%s)." % requestNames 
        for request in requestNames:
            logging.info("Deleting '%s' request ..." % request)
            status, data = self._httpRequest("DELETE", "/reqmgr/reqMgr/request/%s" % request)
            
    
    def cloneRequest(self, requestName):
        logging.debug("Cloning request '%s' ..." % requestName)
        headers = {"Content-Length": 0}
        status, data = self._httpRequest("PUT", "/reqmgr/reqMgr/clone/%s" % requestName, 
                                         headers=headers)
        if status > 216:
            logging.error("Error occurred, exit.")  
            sys.exit(1)
        data = json.loads(data)
        # ReqMgr returns dictionary with key: 'WMCore.RequestManager.DataStructs.Request.Request'
        # print data
        newRequestName = data.values()[0]["RequestName"] 
        logging.info("Clone request succeeded: original request name: '%s' "
                     "new request name: '%s'" % (requestName, newRequestName))
        return newRequestName
        
    
    def allTests(self, requestArgs):
        """
        Call all methods above. Tests everything.
        Checks that the ReqMgr instance has the same state before 
        and after this script.
                
        """
        self.userGroup(None) # argument has no meaning
        self.team(None) # argument has no meaning
        currentRequests = self.queryRequest()
        requests = []
        # see in the assignRequest comment the reason for deepcopy here
        requestArgs1 = copy.deepcopy(requestArgs)
        requestArgs2 = copy.deepcopy(requestArgs)
        requests.append(self.createRequest(requestArgs1, restApi = True))
        requests.append(self.createRequest(requestArgs2, restApi = False))
        for requestName in requests:
            self.queryRequest(requestName)
        requests.append(self.cloneRequest(requests[0])) # clone the first request in the list
        self.deleteRequests(requests)
        logging.info("%s requests in the system before this test." % len(currentRequests))
        afterRequests = self.queryRequest()
        logging.info("%s requests in the system before this test." % len(afterRequests))
        assert currentRequests == afterRequests, "Request list not matching!"
        logging.info("Running --allTests succeeded.")
                
    
    def __del__(self):
        self.conn.close()
        del self.conn
    

# ---------------------------------------------------------------------------    


def processCmdLine(args):
    def errExit(msg, parser):
        logging.error(msg)
        parser.print_help()
        sys.exit(1)
        
    usage = \
"""usage: %prog options"""
    form = TitledHelpFormatter(width = 78)
    parser = OptionParser(usage = usage, formatter = form, add_help_option = None)
    actions = defineCmdLineOptions(parser)
    # opts - new processed options
    # args - remainder of the input array
    opts, args = parser.parse_args(args = args)
    # check mandatory arguments and some arguments dependency
    if not opts.reqMgrUrl:
        errExit("Missing mandatory --reqMgrUrl, see --help.", parser)
    if opts.createRequest and not opts.configFile:
        errExit("When --createRequest, --configFile is necessary, see --help.", parser)
    if opts.allTests and not opts.configFile:
        errExit("When --allTests, --configFile is necessary, see --help.", parser)
    if (opts.json and not opts.createRequest) and (opts.json and not opts.allTests):
        errExit("Command line --json only with --createRequest or --allTests, see --help.", parser)
    if opts.allTests and opts.createRequest:
        errExit("Arguments --allTests and --createRequest are mutually exclusive.") 
    # deleteRequests will be a list
    if opts.deleteRequests:
        opts.deleteRequests = opts.deleteRequests.split(",") 
    return opts, actions


def defineCmdLineOptions(parser):
    help = "Display this help"
    actions = []
    parser.add_option("-h", "--help", help=help, action='help')
    help = ("User cert file (or cert proxy file). "
            "If not defined, tries X509_USER_CERT then X509_USER_PROXY env. "
            "variables. And lastly /tmp/x509up_uUID.")
    parser.add_option("-c", "--cert", help=help)    
    help = ("User key file (or cert proxy file). "
            "If not defined, tries X509_USER_KEY then X509_USER_PROXY env. "
            "variables. And lastly /tmp/x509up_uUID.")
    parser.add_option("-k", "--key", help=help)
    help = ("Request Manager service address (no other options - query all requests) "
            "e.g.: https://maxareqmgr01.cern.ch")
    parser.add_option("-u", "--reqMgrUrl", help=help)
    help = "Action: Query a request specified by the request name."
    action = "queryRequest"
    actions.append(action)
    parser.add_option("-q", "--" + action, help=help)
    help = "Action: Delete request(s) specified by the request name(s) (comma separated)."
    action = "deleteRequests"
    actions.append(action)
    parser.add_option("-d", "--" + action, help=help)
    help = ("Action: Create, inject, approve and assign a request. Whichever "
            "from the config file defined arguments can be overridden from "
            "command line and a few have to be so (*-OVERRIDE-ME ending).")
    action = "createRequest"
    actions.append(action)  
    parser.add_option("-i", "--" + action, action="store_true",
                      dest="createRequest", help=help)
    help = "Config file, necessary when --createRequest."
    parser.add_option("-f", "--configFile", help=help)
    help = ("JSON (following the config file) to override values. "
            "These values are only parsed on --createRequest. "
            "e.g. {\"request\": {\"Requestor\": \"efajardo\"}, \"requestAssign\": {\"FirstLumi: 1}}")
    parser.add_option("-j", "--json", help=help)
    action = "cloneRequest"
    actions.append(action)
    help = "Action: Clone request, the request name is mandatory argument."
    parser.add_option("-l", "--" + action, help=help)
    action = "allTests"
    actions.append(action)
    help = ("Action: Perform all operations this script allows. "
            "--configFile and possibly --json must be present for initial request injection.")
    parser.add_option("-a", "--" + action, action="store_true", help=help)
    # TODO
    # this will be removed once ReqMgr takes this internal user management
    # information from SiteDB, only teams will remain
    action = "userGroup"
    actions.append(action)
    help = "Action: List groups and users registered with Request Manager instance."
    parser.add_option("-s", "--" + action,  action="store_true", help=help)
    help = "Action: List teams registered with a Request Manager instance."
    action = "team"
    actions.append(action)
    parser.add_option("-t", "--" + action,  action="store_true", help=help)
    return actions
    
    
def processRequestArgs(intputConfigFile, commandLineJson):
    """
    Load request arguments from a file, blend with JSON from command line.
    
    """
    logging.info("Loading file '%s' ..." % intputConfigFile)
    try:
        config = json.load(open(intputConfigFile, 'r'))
    except IOError as ex:
        logging.fatal("Reading arguments file '%s' failed, "
                      "reason: %s." % (intputConfigFile, ex))
        sys.exit(1)
    if commandLineJson:
        logging.info("Parsing request arguments on the command line ...")
        cliJson = json.loads(commandLineJson)
        # if a key exists in cliJson, update values in the main config dict
        for k in config.keys():
            if cliJson.has_key(k):
                config[k].update(cliJson[k])            
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
    check(config.items())
    return config
        
    
def initialization(commandLineArgs):
    logging.basicConfig(level = logging.DEBUG)
    logging.debug("Processing command line arguments: '%s' ..." % commandLineArgs)
    # opts, args = processCmdLine(commandLineArgs)
    opts, actions = processCmdLine(commandLineArgs)
    logging.debug("Getting user identity files ...")
    proxyFile = "/tmp/x509up_u%s" % os.getuid()
    if not os.path.exists(proxyFile):
        proxyFile = "UNDEFINED" 
    certFile = opts.cert or os.getenv("X509_USER_CERT", os.getenv("X509_USER_PROXY", proxyFile)) 
    keyFile = opts.key or os.getenv("X509_USER_KEY", os.getenv("X509_USER_PROXY", proxyFile)) 
    reqMgrSubmitter = ReqMgrSubmitter(opts.reqMgrUrl, certFile, keyFile)
    if opts.createRequest:
        # process request arguments and store them into opts values
        opts.createRequest = processRequestArgs(opts.configFile, opts.json)
    if opts.allTests:
        # the same as above
        opts.allTests = processRequestArgs(opts.configFile, opts.json)
    return reqMgrSubmitter, opts, actions
    

def main():
    reqMgrSubmitter, opts, definedActions = initialization(sys.argv)
    # definedAction are all actions as defined for CLI
    # there is now gonna be usually 1 action to perform, but could be more
    # filter out those where opts.ACTION is None
    actions = filter(lambda name: getattr(opts, name), definedActions)
    for action in actions:
        logging.info("Performing '%s' ..." % action)
        reqMgrSubmitter.__getattribute__(action)(getattr(opts, action))
    if not actions:
        reqMgrSubmitter.queryRequest()
        
    
if __name__ == "__main__":
    main()