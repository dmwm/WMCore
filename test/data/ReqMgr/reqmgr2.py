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
            
            
    def http_request(self, verb, uri, data=None, header=None):
        logging.debug("Request: %s %s %s ..." % (verb, uri, data))
        self.conn.request(verb, uri, body=data, headers=self.headers)
        resp = self.conn.getresponse()
        data = resp.read()
        logging.debug("Status: %s" % resp.status)
        logging.debug("Reason: %s" % resp.reason)
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
                                        
    For WMCore.REST, PUT method, sending args = json.dumps({"group_name": group})
        didn't work, experimented with headers, no luck - had to do urlencoding
               
    """
    def __init__(self, url, config):
        logging.info("ReqMgr url: %s" % url)
        # ReqMgr based on WMCore.REST API requires accept types defined
        self.headers = {"Content-type": "application/x-www-form-urlencoded",
                        "Accept": "application/json"}
        self.urn_prefix = "/reqmgr2"
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
        
        data = self._caller_checker("/sw", "GET")
        
        data = self._caller_checker("/request", "GET")
        data = self._caller_checker("/request?all=false", "GET")
        data = self._caller_checker("/request?all=true", "GET")
        req_name = data[0]["rows"][0]["id"] # is RequestName
        self._caller_checker("/request?request_name=%s" % req_name, "GET")
        
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
    for action in filter(lambda name: getattr(opts, name), actions):
        if opts.all_tests and action and action != "all_tests":
            err_exit("Arguments --all_tests and --%s mutually exclusive." % action, parser)
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
    # -a --------------------------------------------------------------------
    help = ("Action: Perform all operations this script allows. "
            "Check all REST API of the ReqMgr service.")
    action = "all_tests"
    actions.append(action)
    parser.add_option("-a", "--" + action, action="store_true", help=help)
    # -v ---------------------------------------------------------------------\
    help = "Verbose console output."
    parser.add_option("-v", "--verbose",  action="store_true", help=help)    
    return actions

    
def initialization(cli_args):
    print("Processing command line arguments: '%s' ..." % cli_args)
    config, actions = process_cli_args(cli_args)
    logging.basicConfig(level=logging.DEBUG if config.verbose else logging.INFO)
    logging.debug("Set verbose console output.")
    reqmgr_client = ReqMgrClient(config.reqmgrurl, config)
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
        reqmgr_client.all_tests(config)
        
    
if __name__ == "__main__":
    main()