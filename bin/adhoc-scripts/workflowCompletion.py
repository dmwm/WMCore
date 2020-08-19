#!/usr/bin/env python
"""
Script to retrieve the request status and evaluate the workflow
completion, so the ration of input lumis vs output lumis.
"""
from __future__ import print_function, division

from future import standard_library
standard_library.install_aliases()
import argparse
import http.client
import json
import os
import pwd
import sys
import urllib.request, urllib.parse
from urllib.error import HTTPError, URLError

# ID for the User-Agent
CLIENT_ID = 'workflowCompletion::python/%s.%s' % sys.version_info[:2]


class HTTPSClientAuthHandler(urllib.request.HTTPSHandler):
    """
    Basic HTTPS class
    """

    def __init__(self, key, cert):
        urllib.request.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=290):
        return http.client.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)


def getX509():
    "Helper function to get x509 from env or tmp file"
    proxy = os.environ.get('X509_USER_PROXY', '')
    if not proxy:
        proxy = '/tmp/x509up_u%s' % pwd.getpwuid(os.getuid()).pw_uid
        if not os.path.isfile(proxy):
            return ''
    return proxy


def getContent(url, params=None):
    cert = getX509()
    client = '%s (%s)' % (CLIENT_ID, os.environ.get('USER', ''))
    handler = HTTPSClientAuthHandler(cert, cert)
    opener = urllib.request.build_opener(handler)
    opener.addheaders = [("User-Agent", client),
                         ("Accept", "application/json")]
    try:
        response = opener.open(url, params)
        output = response.read()
    except HTTPError as e:
        print("The server couldn't fulfill the request at %s" % url)
        print("Error code: ", e.code)
        output = '{}'
        # sys.exit(1)
    except URLError as e:
        print('Failed to reach server at %s' % url)
        print('Reason: ', e.reason)
        sys.exit(2)
    return output


def handleReqMgr(reqName, reqmgrUrl):
    """
    Return the list of output datasets
    """
    urn = reqmgrUrl + "/reqmgr2/data/request/" + reqName
    reqmgrOut = json.loads(getContent(urn))['result'][0][reqName]

    if reqmgrOut['RequestStatus'] in ['assignment-approved', 'assigned', 'staging', 'staged']:
        print("Workflow %s in status: %s , skipping!\n" % (reqName, reqmgrOut['RequestStatus']))
        return None, None

    if 'InputDataset' in reqmgrOut:
        inputData = reqmgrOut['InputDataset']
    elif 'Task1' in reqmgrOut and 'InputDataset' in reqmgrOut['Task1']:
        inputData = reqmgrOut['Task1']['InputDataset']
    elif 'Step1' in reqmgrOut and 'InputDataset' in reqmgrOut['Step1']:
        inputData = reqmgrOut['Step1']['InputDataset']
    else:
        inputData = None

    print("==> %s\t(status: %s)" % (reqName, reqmgrOut['RequestStatus']))
    print("InputDataset:\n    %s (total input lumis: %s)" % (inputData, reqmgrOut['TotalInputLumis']))
    return reqmgrOut['TotalInputLumis'], reqmgrOut['OutputDatasets']


def handleDBS(reqmgrOutDsets, cmswebUrl):
    """
    Get total number of lumi sections in each dataset
    """
    if 'testbed' in cmswebUrl:
        dbsUrl = cmswebUrl + "/dbs/int/global/DBSReader/"
    else:
        dbsUrl = cmswebUrl + "/dbs/prod/global/DBSReader/"

    dbsOutput = {}
    for dataset in reqmgrOutDsets:
        fullUrl = dbsUrl + "filesummaries?" + urllib.parse.urlencode({'dataset': dataset})
        data = json.loads(getContent(fullUrl))
        if data:
            dbsOutput[dataset] = data[0]['num_lumi']
        else:
            dbsOutput[dataset] = 0

    return dbsOutput


def main():
    """
    Requirements: you need to have your proxy and proper x509 environment
     variables set.

    Receive a workflow name in order to fetch the following information:
     - from couchdb: gets the output datasets and spec file
     - from phedex: gets dataset, block, files information
     - from dbs: gets dataset, block and files information
    """
    parser = argparse.ArgumentParser(description="Validate workflow input, output and config")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-w', '--workflow', help='A single workflow name')
    group.add_argument('-i', '--inputFile', help='Plain text file containing request names (one per line)')
    parser.add_argument('-c', '--cms', help='CMSWEB url to talk to DBS/PhEDEx. E.g: cmsweb-testbed.cern.ch')
    parser.add_argument('-r', '--reqmgr', help='Request Manager URL. Example: cmsweb-testbed.cern.ch')
    args = parser.parse_args()

    if args.workflow:
        listRequests = [args.workflow]
    elif args.inputFile:
        with open(args.inputFile, 'r') as f:
            listRequests = [req.rstrip('\n') for req in f.readlines()]
    else:
        parser.error("You must provide either a workflow name or an input file name.")
        sys.exit(3)

    cmswebUrl = "https://" + args.cms if args.cms else "https://cmsweb.cern.ch"
    reqmgrUrl = "https://" + args.reqmgr if args.reqmgr else "https://cmsweb.cern.ch"

    for reqName in listRequests:
        ### Retrieve and process ReqMgr information
        inputLumis, reqmgrOutDsets = handleReqMgr(reqName, reqmgrUrl)
        if reqmgrOutDsets is None:
            continue

        ### Retrieve and process DBS information
        dbsInfo = handleDBS(reqmgrOutDsets, cmswebUrl)
        print("OutputDatasets:")
        for dset in reqmgrOutDsets:
            print("    %s (lumis: %s, lumi completion: %s)" % (dset, dbsInfo[dset], dbsInfo[dset] / inputLumis))
        print("")

    sys.exit(0)


if __name__ == "__main__":
    sys.exit(main())
