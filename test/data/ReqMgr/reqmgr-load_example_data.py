"""
Development helper script capable of (according to supplied arguments):
    o fresh ReqMgr instance set up (creating user, group, team ...)
    o querying requests stored at ReqMgr instance
    o deleting requests from ReqMgr instance
    o creating new requests in the ReqMgr instance
    o routing requests through states (optionally will also inject
        a request reaching "ops-hold" state into the OpsClipboard couchapp database)
            
ReqMgr setup function:
2012-01-16
creating an already existing user-group association raise an exception
(oracle duplicate association), it's non critical
https://svnweb.cern.ch/trac/CMSDMWM/ticket/3105

ReqMgr REST API URL could e.g. be:
[the port and REST URL is configured in files
WMCore/HTTPFrontEnd/RequestManager/ReqMgrConfiguration.py
etc/ReqMgrConfig.py (NB: last slash has to be there!)]

http://localhost:8687/reqmgr/reqMgr/    [local instance]
https://maxadmwm.cern.ch/reqmgr/reqMgr/ [VM devel instance]
https://localhost:2000/reqmgr/reqMgr/   [ssh tunnel to VM devel instance]  

"""

TEAM_NAME = "White Sox"

import urllib
import sys
import httplib
import logging
from optparse import OptionParser, TitledHelpFormatter


import WMCore.WMSpec.StdSpecs.ReReco as ReReco
# RequestManager client code
from WMCore.Services.RequestManager.RequestManager import RequestManager
# for direct REST API queries
from WMCore.Services.Requests import JSONRequests
import WMCore.RequestManager.OpsClipboard.Inject as OpsClipboard



class ReqMgrTester(object):
    def __init__(self, reqMgrUrl):
        self.reqMgrUrl = reqMgrUrl
        self.restSender = JSONRequests(reqMgrUrl)
        d = dict(endpoint = self.reqMgrUrl)
        self.reqMgrService = RequestManager(d)
        

    def queryAllRequests(self):
        """
        Returns all requests stored at ReqMgr instance.
        
        """
        logging.info("Querying all requests at ReqMgr instance ...")
        r = self.reqMgrService.getRequestNames()
        print "Found %s requests:" % len(r)
        for req in r:
            print req
        

    def queryRequest(self, requestName):
        """
        Query a specific request according to the input argument.
        
        """
        urlQuery = "request/%s" % requestName
        logging.info("Querying request '%s'" % requestName)
        logging.info("Query: '%s':" % urlQuery)
        r = self.restSender.get(urlQuery)
        print str(r)
        
        
    def createRequests(self, numRequests):
        """
        Inject new numRequests into ReqMgr instance.
        (see ReqMgr_t testE how to create a request)
        
        """
        logging.info("Creating %s new requests ..." % numRequests)
        schema = ReReco.getTestArguments()
        schema['RequestName'] = 'TestReReco'
        schema['RequestType'] = 'ReReco'
        schema['CmsPath'] = "/uscmst1/prod/sw/cms"
        schema['Requestor'] = '%s' % "testinguser"
        schema['Group'] = '%s' % "PeopleLikeMe"
        schema['BlockWhitelist'] = ['/dataset/dataset/dataset#alpha']
        schema['BlockBlacklist'] = ['/dataset/dataset/dataset#beta']
        schema['Campaign'] = 'MyTestCampaign'        
        for i in range(numRequests):
            urlQuery = "request/testRequest"
            logging.info("Query: '%s':" % urlQuery)
            r = self.restSender.put(urlQuery, schema)
            # print "request creating response: ", r
            print "created: ", r[0]["RequestName"]  
                        
        
    def deleteRequest(self, requestNames):
        """
        Delete requests specified in the input, more request names
        are comma-separated.
        
        """
        logging.info("Deleting requests ...")
        for reqName in requestNames.split(','):
            reqName = reqName.strip()
            urlQuery = "request/%s" % reqName
            logging.info("Deleting request (request_name): '%s'" % reqName)
            logging.info("Query: '%s':" % urlQuery)
            r = self.restSender.delete(urlQuery)

            
    def injectOpsClipboard(self, reqName, couchUrl, couchDbName):
        """
        Once a request reaches "ops-hold" state, it can be injected into
        CouchDB, application OpsClipboard, for further manipulation.
        Do this here with the reqName request.
        
        OpsClipboard.inject() method which does in the CouchDB injection
        is called from WMCore/HTTPFrontEnd/RequestManager/Assign.py
            handleAssignmentPage method (which currently, 2012-01, doesn't
            have any unittest nor REST API) (used only from the Assignment webpage)
            
        Works when running locally accessing CouchDB behind frontend:
        py test/data/ReqMgr/reqmgr-load_example_data.py -u https://localhost:2000/reqmgr/reqMgr/ \
            -t testinguser_120131_213320_2161 -i -o https://localhost:2000/couchdb/ \
            -a ops_clipboard        
        
        """
        # find out campaign name associated with this request
        r = self.restSender.get("request/%s" % reqName)
        campaign = r[0]["Campaign"]
        logging.info("Campaign: %s" % campaign)
        requests = [{u"RequestName": reqName, u"CampaignName": campaign}]
        OpsClipboard.inject(couchUrl, couchDbName, *requests)
            
            
    def requestChangeStates(self, reqName, injectOpsClipboard, couchUrl, couchDbName):
        """
        Route the request (spec. by the request name) in the input
        through a series of possible request states.
        
        """
        logging.info("Changing state of request %s ..." % reqName)
        def changeState(requestName, urlQuery):
            logging.info("Query: '%s' ..." % urlQuery)
            r = self.restSender.put(urlQuery)
            r = self.restSender.get("request/%s" % requestName)
            #assert r[0]["RequestStatus"] == statusName
            logging.info("Querying modified request, new state: %s" % r[0]["RequestStatus"])

        # once a request is created, it's in 'new' state
        # states transition has to be an allowed one as defined here:
        # WMCore/RequestManager/RequestDB/Settings/RequestStatus.py
        statesQueries = ["request/%s?status=%s" % (reqName, "testing-approved"),
                         "request/%s?status=%s" % (reqName, "testing"),
                         "request/%s?status=%s" % (reqName, "tested"),
                         "request/%s?status=%s" % (reqName, "assignment-approved"),
                         # create an assignment now
                         # need quote because of space in the team name
                         urllib.quote("assignment/%s/%s" % (TEAM_NAME, reqName)),
                         "request/%s?status=%s" % (reqName, "ops-hold")]                 
        for query in statesQueries:
            changeState(reqName, query)
            
        if injectOpsClipboard:
            self.injectOpsClipboard(reqName, couchUrl, couchDbName)
                
        
    def setup(self):
        """
        Setup ReqMgr instance for dealing with requests - needs to create
        a user, group, SW releases entries, etc.
        as done in test/python/WMCore_t/RequestManager_t/ReqMgr_t.py
        
        """
        logging.info("ReqMgr setup ...")
        def doQuery(urlQuery):
            logging.info("Query: '%s' ..." % urlQuery)
            r = None
            try:
                r = self.restSender.put(urlQuery)
            except Exception as ex:
                print "exception"
                print str(ex) 
            print "response:", r
        queries = ["user/testinguser?email=testinguser@my.com",
                   "group/PeopleLikeMe",
                   "group/PeopleLikeMe/testinguser",
                   urllib.quote("team/" + TEAM_NAME),
                   "version/%s" % "CMSSW_3_5_8"]
        for q in queries:
            doQuery(q)
        logging.info("ReqMgr setup finished, listing known users ...")
        q = "user/"
        r = self.restSender.get(q)
        print r
        
        
        
def _processCmdLineArgs(args):
    def errExit(parser):
        logging.error("Missing mandatory option, see help.")
        parser.print_help()
        sys.exit(1)
        
    usage = \
"""usage: %prog options"""
    form = TitledHelpFormatter(width = 78)
    parser = OptionParser(usage = usage, formatter = form, add_help_option = None)
    _defineCmdLineOptions(parser)
    # opts - new processed options
    # args - remainder of the input array
    opts, args = parser.parse_args(args = args)
    for mandatory in ("reqMgrUrl",):
        if getattr(opts, mandatory, None):
            break
    else:
        errExit(parser)
    if getattr(opts, "injectOpsClipboard", None) and not getattr(opts, "couchUrl", None):
        errExit(parser)
    if getattr(opts, "injectOpsClipboard", None) and not getattr(opts, "database", None):
        errExit(parser)
    return opts



def _defineCmdLineOptions(parser):
    help = "Display this help"
    parser.add_option("-h", "--help", help=help, action='help')
    help = "Request Manager REST API URL (no other options to query all requests)"
    parser.add_option("-u", "--reqMgrUrl", help=help)
    help = "Query a request specified by the request name."
    parser.add_option("-q", "--queryRequest", help=help)
    help = "Delete request(s) specified by the request name(s) (comma separated)."
    parser.add_option("-d", "--deleteRequests", help=help)
    help = "Set up Request Manager instance (creates user, group, team, etc)."
    parser.add_option("-s", "--reqMgrSetup", action="store_true", dest="reqMgrSetup", help=help)
    help = "Create specified number of new requests."
    parser.add_option("-c", "--createRequests", help=help)
    help = "Route a specified request through a series of request states."
    parser.add_option("-t", "--requestChangeStates", help=help)
    help = "CouchURL (injecting a request into CouchDB."
    parser.add_option("-o", "--couchUrl", help=help)
    help = "CouchDB database name (injecting a request into CouchDB."
    parser.add_option("-a", "--database", help=help)
    help = ("Inject a request reaching \"ops-hold\" state into "
            "CouchDB - OpsClipboard (depends on --requestChangeStates, "
            "--couchUrl, --database defined).")
    parser.add_option("-i", "--injectOpsClipboard", action="store_true",
                      dest="injectOpsClipboard", help=help)
        


def main():
    logging.basicConfig(level = logging.DEBUG)
    opts = _processCmdLineArgs(sys.argv)
    tester = ReqMgrTester(opts.reqMgrUrl)
    if opts.reqMgrSetup:
        tester.setup()
    if opts.requestChangeStates:
        tester.requestChangeStates(opts.requestChangeStates, opts.injectOpsClipboard,
                                   opts.couchUrl, opts.database)
    elif opts.createRequests:
        tester.createRequests(int(opts.createRequests))
    elif opts.deleteRequests:
        tester.deleteRequest(opts.deleteRequests)
    elif opts.queryRequest:
        tester.queryRequest(opts.queryRequest)
    else:
        tester.queryAllRequests()
        
    logging.info("Finished.")



if __name__ == "__main__":
    main()