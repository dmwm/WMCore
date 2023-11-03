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


from builtins import object, str as newstr, bytes as newbytes, next

import json
import logging
import os
import sys
import urllib.parse
from argparse import ArgumentParser
from http.client import HTTPSConnection, HTTPConnection


class RESTClient(object):
    """
    HTTP client
    HTTPS client based on the provided URL (http:// or https://)
    """

    def __init__(self, url, cert=None, key=None, logger=None):
        self.logger = logger
        self.logger.info("RESTClient URL: %s", url)
        if url.startswith("https://"):
            self.logger.debug("Using HTTPS protocol, getting user identity files ...")
            proxy_file = "/tmp/x509up_u%s" % os.getuid()
            if not os.path.exists(proxy_file):
                proxy_file = "UNDEFINED"
            cert_file = cert or os.getenv("X509_USER_CERT",
                                          os.getenv("X509_USER_PROXY", proxy_file))
            key_file = key or os.getenv("X509_USER_KEY",
                                        os.getenv("X509_USER_PROXY", proxy_file))
            self.logger.info("Identity files:\n\tcert file: '%s'\n\tkey file:  '%s' ",
                             cert_file, key_file)
            url = url.replace("https://", '')
            self.logger.debug("Creating connection HTTPS ...")
            self.conn = HTTPSConnection(url, key_file=key_file, cert_file=cert_file)
        if url.startswith("http://"):
            self.logger.info("Using HTTP protocol, creating HTTP connection ...")
            url = url.replace("http://", '')
            self.conn = HTTPConnection(url)

    def http_request(self, verb, uri, data=None, headers=None):
        self.logger.debug("Request: %s %s %s ..." % (verb, uri, data))
        self.conn.request(verb, uri, body=data, headers=headers or self.headers)
        resp = self.conn.getresponse()
        data = resp.read()
        self.logger.debug("Status: %s", resp.status)
        self.logger.debug("Reason: %s", resp.reason)
        if resp.status != 200:
            if hasattr(resp.msg, "x-error-detail"):
                self.logger.warning("Message: %s", resp.msg["x-error-detail"])
        return resp.status, data


class ReqMgrClient(RESTClient):
    """
    Client REST interface to Request Manager service (ReqMgr).

    Actions: all_tests

    For reference:
        jsonArgs = json.dumps(requestArgs["createRequest"])
        status, data = self.http_request("PUT", "/reqmgr/reqMgr/request", data=jsonArgs)
        data = json.loads(data)
        requestName = data.values()[0]["request"]

        params = {"requestName": requestName,
                  "status": "assignment-approved"}
        encodedParams = urllib.urlencode(params)
        logging.info("Approving request '%s' ..." % requestName)
        status, data = self.http_request("PUT", "/reqmgr/reqMgr/request",
                                        data=encodedParams, headers=self.textHeaders)

    """

    def __init__(self, url, config, logger=None):
        self.logger = logging.getLogger() if not logger else logger
        self.logger.info("ReqMgr url: %s", url)
        # ReqMgr based on WMCore.REST API requires accept types defined
        self.headersUrl = {"Content-type": "application/x-www-form-urlencoded",
                           "Accept": "application/json"}
        self.headersBody = {"Content-type": "application/json",
                            "Accept": "application/json"}
        self.headers = self.headersUrl
        self.urn_prefix = "/reqmgr2/data"
        RESTClient.__init__(self, url, cert=config.cert, key=config.key, logger=logger)

    def _caller_checker(self, urn, verb, input_data=None, exp_data=None):
        urn = self.urn_prefix + urn
        self.logger.info("Call %s %s %s", urn, verb, input_data)
        status, data = self.http_request(verb, urn, data=input_data)
        if status != 200:
            self.logger.error("HTTP request failed with status: %s, data: %s", status, data)
            return
        data = json.loads(data)["result"]
        if exp_data:
            assert status == 200, "Call status is: %s" % status
            assert data[0] == exp_data, "'%s' != '%s' mismatch." % (data[0], exp_data)
        else:
            assert status == 200, "Call status is: %s" % status
            self.logger.info("status: %s\n%s", status, data)
        return data

    def delete_requests(self, config):
        urn = self.urn_prefix + "/request"
        for request_name in config.request_names:
            self.logger.info("Deleting '%s' request ...", request_name)
            args = urllib.parse.urlencode({"request_name": request_name})
            status, data = self.http_request("DELETE", urn, data=args)
            if status != 200:
                self.logger.error("Failed to delete request with status: %s, data: %s", status, data)
                sys.exit(1)
            self.logger.info("Done.")

    def create_request(self, config):
        """
        config.request_args - arguments for both creation and assignment

        """
        self.logger.info("Injecting request args:\n%s ...", config.request_args["createRequest"])
        json_args = json.dumps(config.request_args["createRequest"])
        urn = self.urn_prefix + "/request"
        status, data = self.http_request("POST", urn, data=json_args,
                                         headers=self.headersBody)
        if status > 216:
            self.logger.error("Failed to create request with status: %s, data: %s", status, data)
            sys.exit(1)
        data = json.loads(data)
        self.logger.info(data)
        request_name = data["result"][0]["request"]
        self.approve_request(request_name)
        self.logger.info("Create request '%s' succeeded.", request_name)

        config.request_names = request_name

        return request_name

    def approve_request(self, request_name):
        """
        Set request status assignment-approved of the requestName request.
        Once ReqMgr provides proper API for status settings, esp. for assignment,
        a single method setStates shall handle all request status changes.

        """
        self.logger.info("Approving request '%s' ...", request_name)

        json_args = json.dumps({"RequestStatus": "assignment-approved"})
        urn = self.urn_prefix + "/request/%s" % request_name
        status, data = self.http_request("PUT", urn, data=json_args,
                                         headers=self.headersBody)

        if status != 200:
            self.logger.error("Failed to approve request with status: %s, data: %s", status, data)
            sys.exit(1)
        self.logger.info("Approve succeeded.")

    def assign_request(self, config):
        """
        config.request_args - arguments for both creation and assignment

        """
        assign_args = config.request_args["assignRequest"]
        assign_args["RequestStatus"] = "assigned"
        json_args = json.dumps(assign_args)
        if isinstance(config.request_names, (newstr, newbytes)):
            config.request_names = [config.request_names]
        for request_name in config.request_names:
            self.logger.info("Assigning %s with request args: %s ...",
                             request_name, config.request_args["assignRequest"])
            urn = self.urn_prefix + "/request/%s" % request_name
            status, data = self.http_request("PUT", urn, data=json_args,
                                             headers=self.headersBody)
            if status > 216:
                self.logger.error("Failed to assign request with status: %s, data: %s", status, data)
                sys.exit(1)
            data = json.loads(data)
            self.logger.info(data)
            self.logger.info("Assign succeeded.")

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
                self.logger.info("Querying '%s' request ...", request_name)
                urn = self.urn_prefix + "/request?name=%s" % request_name
                status, data = self.http_request("GET", urn)
                if status != 200:
                    self.logger.error("Failed to get request with status: %s, data: %s", status, data)
                    sys.exit(1)
                request = json.loads(data)["result"][0]
                for k, v in sorted(request.items()):
                    self.logger.info("\t%s: %s", k, v)
                requests_data.append(request)
            # returns data on requests in the same order as in the config.request_names
            return requests_data

    def all_tests(self, config):
        self._caller_checker("/hello", "GET", exp_data="Hello world")
        self._caller_checker("/hello?name=John", "GET", exp_data="Hello John")
        self._caller_checker("/about", "GET")
        self._caller_checker("/info", "GET")
        group = "mygroup"
        args = urllib.parse.urlencode({"group_name": group})
        self._caller_checker("/group", "PUT", input_data=args)
        data = self._caller_checker("/group", "GET")
        assert group in data, "%s should be in %s" % (group, data)
        self._caller_checker("/group", "DELETE", input_data=args)
        data = self._caller_checker("/group", "GET")
        assert group not in data, "%s should be deleted from %s" % (group, data)
        team = "myteam"
        args = urllib.parse.urlencode({"team_name": team})
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
            status = next(iter(status_def))
            trans = status_def[status]
            assert status in data2, "%s is not in %s" % (status, data2)
            assert isinstance(trans, list), "transition %s should be list" % trans

        data = self._caller_checker("/type", "GET")
        assert len(data) > 0, "%s should be non-empty list." % data

        # request tests
        new_request_name = self.create_request(config)

        data = self._caller_checker("/request?name=%s" % new_request_name, "GET")
        request = data[0][new_request_name]
        assert request["RequestName"] == new_request_name
        assert request["RequestStatus"] == "new"

        self.logger.info("\nall_tests succeeded.")

    def __del__(self):
        self.conn.close()
        del self.conn


# ---------------------------------------------------------------------------


def process_cli_args():
    def err_exit(msg, parser):
        print('\n')
        parser.print_help()
        print("\n\n%s" % msg)
        sys.exit(1)

    parser = ArgumentParser(usage='%(prog)s [options]', add_help=False)
    actions = define_cli_options(parser)
    # opts - new processed options
    # args - remainder of the input array
    opts = parser.parse_args()
    # check command line arguments validity
    if not opts.reqmgrurl:
        err_exit("Missing mandatory --reqmgrurl.", parser)

    if opts.create_request and not opts.config_file:
        err_exit("When --create_request, --config_file is necessary.", parser)
    if opts.create_request and opts.request_names:
        err_exit("--request_names can't be provided with --create_request", parser)
    if opts.all_tests and not opts.config_file:
        err_exit("When --all_tests, --config_file is necessary", parser)
    if opts.json and not (opts.create_request or opts.assign_request or opts.all_tests):
        err_exit("--json only with --create_request, --assign_request or --all_tests", parser)

    for action in [name for name in actions if getattr(opts, name)]:
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
    parser.add_argument("-h", "--help", help=help, action='help')
    # "-c" ------------------------------------------------------------------
    help = ("User cert file (or cert proxy file). "
            "If not defined, tries X509_USER_CERT then X509_USER_PROXY env. "
            "variables. And lastly /tmp/x509up_uUID.")
    parser.add_argument("-c", "--cert", help=help)
    # "-k" ------------------------------------------------------------------
    help = ("User key file (or cert proxy file). "
            "If not defined, tries X509_USER_KEY then X509_USER_PROXY env. "
            "variables. And lastly /tmp/x509up_uUID.")
    parser.add_argument("-k", "--key", help=help)
    # -u --------------------------------------------------------------------
    help = ("Request Manager service address (if not options is supplied, "
            "returns a list of the requests in ReqMgr) "
            "e.g.: https://maxareqmgr01.cern.ch")
    parser.add_argument("-u", "--reqmgrurl", help=help)
    # -f --------------------------------------------------------------------
    help = "Request template with arguments definition, request config file."
    parser.add_argument("-f", "--config_file", help=help)
    # -j --------------------------------------------------------------------
    help = ("JSON string to override values from --config_file. "
            "e.g. --json=\'{\"createRequest\": {\"Requestor\": \"efajardo\"}, "
            "\"assignRequest\": {\"FirstLumi\": 1}}\' "
            "e.g. --json=`\"cat alan.json\"`")
    parser.add_argument("-j", "--json", help=help)
    # -r --------------------------------------------------------------------
    help = ("Request name or list of comma-separated names to perform "
            "actions upon.")
    parser.add_argument("-r", "--request_names", help=help)
    # -v ---------------------------------------------------------------------
    help = "Verbose console output."
    parser.add_argument("-v", "--verbose", action="store_true", help=help)
    # actions definition below ----------------------------------------------
    # -i --------------------------------------------------------------------
    help = ("Action: Create and approve a request. Whichever from the config "
            "file defined arguments can be overridden from "
            "command line and a few have to be so (*-OVERRIDE-ME ending). "
            "Depends on --config_file.")
    action = "create_request"
    actions.append(action)
    parser.add_argument("-i", "--" + action, action="store_true", help=help)
    # -g --------------------------------------------------------------------
    help = ("Action: Assign request(s) specified by --request_names "
            "or a new request when used with --create_request. "
            "Depends on --request_names and --config_file when used without "
            "--create_request")
    action = "assign_request"
    actions.append(action)
    parser.add_argument("-g", "--" + action, action="store_true", help=help)

    # -d --------------------------------------------------------------------
    help = "Action: Delete request(s) specified by --request_names."
    action = "delete_requests"
    actions.append(action)
    parser.add_argument("-d", "--" + action, action="store_true", help=help)
    # -q --------------------------------------------------------------------
    help = "Action: Query request(s) specified by --request_names."
    action = "query_requests"
    actions.append(action)
    parser.add_argument("-q", "--" + action, action="store_true", help=help)
    # -a --------------------------------------------------------------------
    help = ("Action: Perform all operations this script allows. "
            "Check all REST API of the ReqMgr service.")
    action = "all_tests"
    actions.append(action)
    parser.add_argument("-a", "--" + action, action="store_true", help=help)

    return actions


def process_request_args(input_config_file, command_line_json, logger):
    """
    Load request arguments from a file, blend with JSON from command line.

    """
    logger.info("Loading file '%s' ...", input_config_file)
    try:
        request_args = json.load(open(input_config_file, 'r'))
    except IOError as ex:
        logger.fatal("Reading request arguments file '%s' failed, reason: %s.", input_config_file, ex)
        sys.exit(1)
    if command_line_json:
        logger.info("Parsing request arguments on the command line ...")
        cli_json = json.loads(command_line_json)
        # if a key exists in cli_json, update values in the main request_args dict
        for k in request_args:
            if k in cli_json:
                request_args[k].update(cli_json[k])
    else:
        logger.warning("No request arguments to override (--json)? Some values will be wrong.")

    # iterate over all items recursively and warn about those ending with
    # OVERRIDE-ME, hence not overridden
    def check(items):
        for k, v in items:
            if isinstance(v, dict):
                check(v.items())
            if isinstance(v, newstr) and v.endswith("OVERRIDE-ME"):
                logger.warning("Not properly set: %s: %s", k, v)

    check(request_args.items())
    return request_args


def initialization():
    config, actions = process_cli_args()
    if config.verbose:
        logger = loggerSetup(logging.INFO)
    else:
        logger = loggerSetup()
    logger.info("Command line arguments: %s ...", sys.argv)

    reqmgr_client = ReqMgrClient(config.reqmgrurl, config, logger)
    if config.create_request or config.assign_request or config.all_tests:
        # process request arguments and store them
        config.request_args = process_request_args(config.config_file, config.json, logger)
    return reqmgr_client, config, actions, logger


def loggerSetup(logLevel=logging.INFO):
    """
    Return a logger which writes everything to stdout.
    """
    logger = logging.getLogger(__name__)
    outHandler = logging.StreamHandler(sys.stdout)
    outHandler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(module)s: %(message)s"))
    outHandler.setLevel(logLevel)
    logger.addHandler(outHandler)
    logger.setLevel(logLevel)
    return logger


def main():
    reqmgr_client, config, defined_actions, logger = initialization()
    # definedAction are all actions as defined for CLI
    # there is now gonna be usually 1 action to perform, but could be more
    # filter out those where config.ACTION is None
    # config is all options for this script but also request creation parameters
    actions = [name for name in defined_actions if getattr(config, name)]
    logger.debug("Actions to perform: %s", actions)
    for action in actions:
        logger.info("Performing '%s' ...", action)
        # some methods need to modify config (e.g. add a request name),
        # pass them entire configuration
        reqmgr_client.__getattribute__(action)(config)
    if not actions:
        reqmgr_client.query_requests(config)


if __name__ == "__main__":
    main()
