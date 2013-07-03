#!/usr/bin/env python

"""
Request Manager service (ReqMgr) test and client script.
 
The script shall have no WMCore libraries dependency.

Command line interface: --help

There are mandatory command line arguments (e.g. URL of the Request Manager)

Production ConfigCache: https://cmsweb.cern.ch/couchdb/reqmgr_config_cache/

Note: tests for checking data directly in CouchDB in ReqMgr1 test script:
    WMCore/test/data/ReqMgr/reqmgr.py
    

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
            proxy_file = "/tmp/x509up_u%s" % os.getuid()
            if not os.path.exists(proxy_file):
                proxy_file = "UNDEFINED" 
            cert_file = cert or os.getenv("X509_USER_CERT",
                                          os.getenv("X509_USER_PROXY", proxy_file)) 
            key_file = key or os.getenv("X509_USER_KEY",
                                        os.getenv("X509_USER_PROXY", proxy_file)) 
            logging.info("Identity files:\n\tcert file: '%s'\n\tkey file:  '%s' " %
                         (cert_file, key_file))
            url = url.replace("https://", '')
            logging.info("Creating connection HTTPS ...")
            self.conn = HTTPSConnection(url, key_file=key_file, cert_file=cert_file)
        if url.startswith("http://"):
            logging.info("Using HTTP protocol, creating HTTP connection ...")
            url = url.replace("http://", '')
            self.conn = HTTPConnection(url)
            
            
    def http_request(self, verb, uri, data=None, headers=None):
        logging.debug("Request: %s %s %s ..." % (verb, uri, data))
        self.conn.request(verb, uri, body=data, headers=headers or self.headers)
        resp = self.conn.getresponse()
        data = resp.read()
        logging.debug("Status: %s" % resp.status)
        logging.debug("Reason: %s" % resp.reason)
        if resp.status != 200:
            if hasattr(resp.msg, "x-error-detail"):
                logging.warn("Message: %s" % resp.msg["x-error-detail"])
        return resp.status, data
            
        

class ReqMgrClient(RESTClient):
    """
    Client REST interface to Request Manager service (ReqMgr).
    
    Actions: all_tests
    
    For reference:
        jsonArgs = json.dumps(requestArgs["createRequest"])
        status, data = self.http_request("PUT", "/reqmgr/reqMgr/request", data=jsonArgs)        
        data = json.loads(data)
        requestName = data.values()[0]["RequestName"]
        
        params = {"requestName": requestName,
                  "status": "assignment-approved"}
        encodedParams = urllib.urlencode(params)
        logging.info("Approving request '%s' ..." % requestName)
        status, data = self.http_request("PUT", "/reqmgr/reqMgr/request",
                                        data=encodedParams, headers=self.textHeaders)
                                                       
    """
    
    def __init__(self, url, config):
        logging.info("ReqMgr url: %s" % url)
        # ReqMgr based on WMCore.REST API requires accept types defined        
        self.headersUrl = {"Content-type": "application/x-www-form-urlencoded",
                          "Accept": "application/json"}
        self.headersBody = {"Content-type": "application/json",
                            "Accept": "application/json"}
        self.headers = self.headersUrl 
        self.urn_prefix = "/reqmgr2/data"
        RESTClient.__init__(self, url, cert=config.cert, key=config.key)
        
        
    def _caller_checker(self, urn, verb, input_data=None, exp_data=None):
        urn = self.urn_prefix + urn
        logging.info("Call %s %s %s" % (urn, verb, input_data))
        status, data = self.http_request(verb, urn, data=input_data)
        if status != 200:
            print status
            print data
            return
        data = json.loads(data)["result"]
        if exp_data:
            assert status == 200, "Call status is: %s" % status
            assert data[0] == exp_data, "'%s' != '%s' mismatch." % (data[0], exp_data)
        else:
            assert status == 200, "Call status is: %s" % status
            print "status: %s\n%s" % (status, data)
        return data
    
    
    def delete_requests(self, config):
        urn = self.urn_prefix +  "/request"
        for request_name in config.request_names:
            logging.info("Deleting '%s' request ..." % request_name)            
            args = urllib.urlencode({"request_name": request_name})
            status, data = self.http_request("DELETE", urn, data=args)
            if status != 200:
                print data
                sys.exit(1)
            logging.info("Done.")           
    

    def create_request(self, config):
        """
        config.request_args - arguments for both creation and assignment
        
        """
        logging.info("Injecting request args:\n%s ..." %
                     config.request_args["createRequest"])
        json_args = json.dumps(config.request_args["createRequest"])
        urn = self.urn_prefix + "/request"
        status, data = self.http_request("POST", urn, data=json_args,
                                         headers=self.headersBody)
        if status > 216:
            logging.error("Error occurred, exit.")
            print data
            sys.exit(1)
        data = json.loads(data)
        print data
        request_name = data["result"][0]["RequestName"] 
        logging.info("Create request '%s' succeeded." % request_name)
        return request_name
        
    
    def query_requests(self, config, to_query=None):
        """
        If to_query and config.request_names are not specified, then
        all requests in the system are queried.
        toQuery - particular request name to query.
        config.request_names - list of requests to query.
        
        Returns a list of requests in either case.
        
        """  
        if to_query:
            requests_to_query = [to_query]
        else:
            requests_to_query = config.request_names
            
        requests_data = []
        if requests_to_query:
            for request_name in requests_to_query:
                logging.info("Querying '%s' request ..." % request_name)
                urn = self.urn_prefix + "/request?name=%s" % request_name
                status, data = self.http_request("GET", urn)
                if status != 200:
                    print data
                    sys.exit(1)           
                request = json.loads(data)["result"][0]
                for k, v in sorted(request.items()):
                    print "\t%s: %s" % (k, v)
                requests_data.append(request)
            # returns data on requests in the same order as in the config.request_names
            return requests_data
        else:
            raise RuntimeError("Implementation not completed, work on GET method underway.")
            logging.info("Querying all requests ...")
            urn = self.urn_prefix + "/request?all=true"
            status, data = self.http_request("GET", urn)
            if status != 200:
                print data
                sys.exit(1)
            requests = json.loads(data)
            requests = requests["result"][0]["rows"]
            keys = ("RequestName", "RequestType", "RequestType", "RequestStatus")
            for r in requests:
                print " ".join(["%s: '%s'" % (k, r["value"][k]) for k in keys])
            logging.info("%s requests in the system." % len(requests))
            return requests


    def all_tests(self, config):
        self._caller_checker("/hello", "GET", exp_data="Hello world")
        self._caller_checker("/hello?name=John", "GET", exp_data="Hello John")
        self._caller_checker("/about", "GET")
        self._caller_checker("/info", "GET")
        group = "mygroup"
        args = urllib.urlencode({"group_name": group})
        self._caller_checker("/group", "PUT", input_data=args)
        data = self._caller_checker("/group", "GET")
        assert group in data, "%s should be in %s" % (group, data)
        self._caller_checker("/group", "DELETE", input_data=args)
        data = self._caller_checker("/group", "GET")
        assert group not in data, "%s should be deleted from %s" % (group, data)
        team = "myteam"
        args = urllib.urlencode({"team_name": team})
        self._caller_checker("/team", "PUT", input_data=args)
        data = self._caller_checker("/team", "GET")
        assert team in data, "%s should be in %s" % (team, data)
        self._caller_checker("/team", "DELETE", input_data=args)
        data = self._caller_checker("/team", "GET")
        assert team not in data, "%s should be deleted from %s" % (team, data)
        
        data = self._caller_checker("/software", "GET")
                
        data = self._caller_checker("/status", "GET")
        assert len(data) > 0, "%s should be non-empty list." % data
        # test some request status
        status = ["assigned", "assignment-approved", "failed", "new"]
        for s in status: assert s in data, "%s is not in %s" % (s, data)
        data2 = self._caller_checker("/status?transition=false", "GET")
        assert data == data2, "%s != %s" % (data, data2)
        # returns also all allowed transitions
        data = self._caller_checker("/status?transition=true", "GET")
        for status_def in data:
            status = status_def.keys()[0]
            trans = status_def[status]
            assert status in data2, "%s is not in %s" % (status, data2)
            assert isinstance(trans, list), "transition %s should be list" % trans
            
        data = self._caller_checker("/type", "GET")
        assert len(data) > 0, "%s should be non-empty list." % data
        
        # request tests
        new_request_name = self.create_request(config) 
        
        data = self._caller_checker("/request?name=%s" % new_request_name, "GET")
        request = data[0]
        assert request["RequestName"] == new_request_name  
        assert request["RequestStatus"] == "new"
                        
        print "\nall_tests succeeded."
        
    
    def __del__(self):
        self.conn.close()
        del self.conn
    

# ---------------------------------------------------------------------------    


def process_cli_args(args):
    def err_exit(msg, parser):
        print('\n')
        parser.print_help()
        print("\n\n%s" % msg)
        sys.exit(1)
        
    form = TitledHelpFormatter(width=78)
    parser = OptionParser(usage="usage: %prog options", formatter=form,
                          add_help_option=None)
    actions = define_cli_options(parser)
    # opts - new processed options
    # args - remainder of the input array
    opts, args = parser.parse_args(args=args)
    # check command line arguments validity
    if not opts.reqmgrurl:
        err_exit("Missing mandatory --reqmgrurl.", parser)

    if opts.create_request and not opts.config_file:
        err_exit("When --create_request, --config_file is necessary.", parser)
    if opts.create_request and opts.request_names:
        err_exit("--request_names can't be provided with --create_request", parser)
    if opts.all_tests and not opts.config_file:
        err_exit("When --all_tests, --config_file is necessary", parser)
    if (opts.json and not opts.create_request) and (opts.json and not opts.all_tests):
        err_exit("--json only with --create_request, --all_tests", parser)
        
    for action in filter(lambda name: getattr(opts, name), actions):
        if opts.all_tests and action and action != "all_tests":
            err_exit("Arguments --all_tests and --%s mutually exclusive." % action, parser)

    if opts.request_names:
        # make it a list here
        opts.request_names = opts.request_names.split(',')
            
    return opts, actions


def define_cli_options(parser):
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
    parser.add_option("-u", "--reqmgrurl", help=help)
    # -f --------------------------------------------------------------------
    help = "Request template with arguments definition, request config file."
    parser.add_option("-f", "--config_file", help=help)    
    # -j --------------------------------------------------------------------
    help = ("JSON string to override values from --config_file. "
            "e.g. --json=\'{\"createRequest\": {\"Requestor\": \"efajardo\"}, "
            "\"assignRequest\": {\"FirstLumi\": 1}}\' "
            "e.g. --json=`\"cat alan.json\"`")
    parser.add_option("-j", "--json", help=help)    
    # -r --------------------------------------------------------------------
    help = ("Request name or list of comma-separated names to perform "
            "actions upon.")
    parser.add_option("-r", "--request_names", help=help)
    # -v ---------------------------------------------------------------------
    help = "Verbose console output."
    parser.add_option("-v", "--verbose",  action="store_true", help=help)
    # actions definition below ----------------------------------------------    
    # -i --------------------------------------------------------------------   
    help = ("Action: Create and inject a request. Whichever from the config "
            "file defined arguments can be overridden from "
            "command line and a few have to be so (*-OVERRIDE-ME ending). "
            "Depends on --config_file.")
    action = "create_request"
    actions.append(action)  
    parser.add_option("-i", "--" + action, action="store_true", help=help)
    # -d --------------------------------------------------------------------
    help = "Action: Delete request(s) specified by --request_names."
    action = "delete_requests"
    actions.append(action)
    parser.add_option("-d", "--" + action, action="store_true", help=help)    
    # -q --------------------------------------------------------------------
    help = "Action: Query request(s) specified by --request_names."
    action = "query_requests"
    actions.append(action)
    parser.add_option("-q", "--" + action, action="store_true", help=help)
    # -a --------------------------------------------------------------------
    help = ("Action: Perform all operations this script allows. "
            "Check all REST API of the ReqMgr service.")
    action = "all_tests"
    actions.append(action)
    parser.add_option("-a", "--" + action, action="store_true", help=help)
    
    return actions


def process_request_args(intput_config_file, command_line_json):    
    """
    Load request arguments from a file, blend with JSON from command line.
    
    """
    logging.info("Loading file '%s' ..." % intput_config_file)
    try:
        request_args = json.load(open(intput_config_file, 'r'))
    except IOError as ex:
        logging.fatal("Reading request arguments file '%s' failed, "
                      "reason: %s." % (intput_config_file, ex))
        sys.exit(1)
    if command_line_json:
        logging.info("Parsing request arguments on the command line ...")
        cli_json = json.loads(command_line_json)
        # if a key exists in cli_json, update values in the main request_args dict
        for k in request_args.keys():
            if cli_json.has_key(k):
                request_args[k].update(cli_json[k])            
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
    check(request_args.items())
    return request_args

    
def initialization(cli_args):
    print("Processing command line arguments: '%s' ..." % cli_args)
    config, actions = process_cli_args(cli_args)
    logging.basicConfig(level=logging.DEBUG if config.verbose else logging.INFO)
    logging.debug("Set verbose console output.")
    reqmgr_client = ReqMgrClient(config.reqmgrurl, config)
    if config.create_request or config.all_tests:
        # process request arguments and store them
        config.request_args = process_request_args(config.config_file, config.json)    
    return reqmgr_client, config, actions
    

def main():
    reqmgr_client, config, defined_actions = initialization(sys.argv)
    # definedAction are all actions as defined for CLI
    # there is now gonna be usually 1 action to perform, but could be more
    # filter out those where config.ACTION is None
    # config is all options for this script but also request creation parameters
    actions = filter(lambda name: getattr(config, name), defined_actions)
    logging.info("Actions to perform: %s" % actions) 
    for action in actions:
        logging.info("Performing '%s' ..." % action)
        # some methods need to modify config (e.g. add a request name),
        # pass them entire configuration
        reqmgr_client.__getattribute__(action)(config)
    if not actions:
        reqmgr_client.query_requests(config)
        
        
    
if __name__ == "__main__":
    main()